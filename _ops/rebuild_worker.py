import subprocess
import time
import sys
import os
import hashlib
import json
import tempfile
import ctypes
import re

# --- [CONFIGURAÇÃO DE PERSISTÊNCIA] ---
# Movemos o hash para fora da pasta do Git para ele não sumir no checkout
METADATA_DIR = r"C:\deploy_metadata"
HASH_STORAGE = os.path.join(METADATA_DIR, "worker_state.json")

if not os.path.exists(METADATA_DIR):
    os.makedirs(METADATA_DIR, exist_ok=True)

# --- [BYPASS DE CREDENCIAIS DOCKER] ---
os.environ["DOCKER_BUILDKIT"] = "1"
os.environ["COMPOSE_DOCKER_CLI_BUILD"] = "1"

def get_long_path(path):
    try:
        buf = ctypes.create_unicode_buffer(500)
        ctypes.windll.kernel32.GetLongPathNameW(path, buf, 500)
        return buf.value
    except: return path

fake_config_dir = get_long_path(tempfile.mkdtemp())
with open(os.path.join(fake_config_dir, "config.json"), "w") as f:
    json.dump({ "credsStore": "", "credsHelpers": {}, "auths": {} }, f)

os.environ["DOCKER_CONFIG"] = fake_config_dir
DOCKER_CMD = f'docker --config "{fake_config_dir}"'
# -----------------------------------------------------------

TIMEOUT_DRAIN = 300
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)

FILES_TO_MONITOR = [
    os.path.join(project_root, "prefect-worker", "requirements.txt"),
    os.path.join(project_root, "prefect-worker", "Dockerfile")
]

def get_file_hash(path):
    if not os.path.exists(path): return None
    hasher = hashlib.md5()
    with open(path, 'rb') as f: hasher.update(f.read())
    return hasher.hexdigest()

def check_if_build_needed():
    current_hashes = {os.path.basename(p): get_file_hash(p) for p in FILES_TO_MONITOR}
    stored_hashes = {}
    if os.path.exists(HASH_STORAGE):
        try:
            with open(HASH_STORAGE, 'r') as f: stored_hashes = json.load(f)
        except: pass
    # Só buildamos se os ficheiros monitorizados mudarem
    return current_hashes != stored_hashes, current_hashes

def save_new_hashes(hashes):
    with open(HASH_STORAGE, 'w') as f: json.dump(hashes, f, indent=4)

def run_command(command, description):
    print(f"[EXEC] {description}...")
    result = subprocess.run(command, shell=True, env=os.environ)
    return result.returncode == 0

def extract_image_name():
    try:
        with open(os.path.join(project_root, "prefect-worker", "docker-compose.yml"), "r") as f:
            match = re.search(r'image:\s+([^\s]+)', f.read())
            if match: return match.group(1)
    except: pass
    return "custom-prefect-worker:latest"

def graceful_drain(color, timeout=None):
    # Lógica de drenagem para permitir que jobs do Prefect terminem
    try:
        cmd = f'{DOCKER_CMD} ps --filter "name=worker-{color}" -q'
        ids = subprocess.run(cmd, shell=True, capture_output=True, text=True).stdout.split()
    except: ids = []
    if not ids: return

    print(f"[STOP] Drenando {color} (SIGTERM)...")
    subprocess.run(f"{DOCKER_CMD} kill --signal=SIGTERM {' '.join(ids)}", shell=True)
    
    start = time.time()
    while True:
        rem = subprocess.run(cmd, shell=True, capture_output=True, text=True).stdout.strip()
        if not rem or (timeout and (time.time() - start) > timeout): break
        time.sleep(5)

def rebuild_blue_green():
    docker_dir = os.path.join(project_root, "prefect-worker")
    os.chdir(docker_dir)

    needs_build, new_hashes = check_if_build_needed()
    
    # Verifica se há algum container ativo para evitar downtime
    check_any = subprocess.run(f'{DOCKER_CMD} ps --filter "name=worker-" -q', shell=True, capture_output=True, text=True).stdout.strip()

    if not needs_build and "--force" not in sys.argv and check_any:
        print("[SKIP] Cache detectado no disco. Pulando build do Docker.")
        return

    # Lógica Blue-Green
    is_blue = subprocess.run(f'{DOCKER_CMD} ps --filter "name=worker-blue" -q', shell=True, capture_output=True, text=True).stdout.strip()
    new_color = "green" if is_blue else "blue"
    curr_color = "blue" if is_blue else "green"
    
    print(f"[INFO] Ciclo Blue-Green: {curr_color or 'Nenhum'} -> {new_color}")

    if needs_build or "--force" in sys.argv:
        image_name = extract_image_name()
        print(f"[BUILD] Gerando nova imagem: {image_name}")
        
        # Adicionei --load para garantir que a imagem saia do cache para o registro local
        cmd_build = f"{DOCKER_CMD} build --load -t {image_name} -f Dockerfile .."
        if not run_command(cmd_build, "Docker Build"): sys.exit(1)
        save_new_hashes(new_hashes)

    # Sobe a nova cor sem derrubar a antiga ainda
    cmd_up = f"{DOCKER_CMD} compose -p worker-{new_color} up -d --remove-orphans"
    if not run_command(cmd_up, f"Subindo {new_color}"): sys.exit(1)

    print("[WAIT] Aguardando estabilização (15s)...")
    time.sleep(15)

    # Drena e remove a cor antiga apenas após a nova estar OK
    if check_any and curr_color:
        graceful_drain(curr_color, timeout=TIMEOUT_DRAIN)
        run_command(f"{DOCKER_CMD} compose -p worker-{curr_color} down", f"Removendo {curr_color}")

    # Limpa imagens antigas para não encher o disco
    subprocess.run(f"{DOCKER_CMD} image prune -f", shell=True, capture_output=True)
    print(f"[OK] {new_color.upper()} esta ativo e processando!")

if __name__ == "__main__":
    rebuild_blue_green()