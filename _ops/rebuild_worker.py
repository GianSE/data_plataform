import subprocess
import time
import sys
import os
import hashlib
import json
import tempfile
import shutil
import ctypes
import re

# --- [FIX CRÍTICO V5] BYPASS TOTAL (BUILD MANUAL) ---

# 1. Desativa BuildKit e recursos modernos que exigem credenciais
os.environ["DOCKER_BUILDKIT"] = "0"
os.environ["COMPOSE_DOCKER_CLI_BUILD"] = "0"

def get_long_path(path):
    """Converte caminhos curtos do Windows para longos."""
    try:
        buf = ctypes.create_unicode_buffer(500)
        GetLongPathName = ctypes.windll.kernel32.GetLongPathNameW
        GetLongPathName(path, buf, 500)
        return buf.value
    except Exception:
        return path

# 2. Configuração Limpa de Credenciais
fake_config_dir_raw = tempfile.mkdtemp()
fake_config_dir = get_long_path(fake_config_dir_raw)
config_path = os.path.join(fake_config_dir, "config.json")

try:
    with open(config_path, "w") as f:
        json.dump({ "credsStore": "", "credsHelpers": {}, "auths": {} }, f)
    
    os.environ["DOCKER_CONFIG"] = fake_config_dir
    # Flag global para forçar o uso da config
    DOCKER_CMD = f'docker --config "{fake_config_dir}"'
    print(f"[FIX] Bypass ativado. Path: {fake_config_dir}")
except Exception as e:
    print(f"[WARN] Erro no bypass: {e}")
    DOCKER_CMD = "docker"

# -----------------------------------------------------------

TIMEOUT_DRAIN = 300
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)

FILES_TO_MONITOR = [
    os.path.join(project_root, "prefect-worker", "requirements.txt"),
    os.path.join(project_root, "prefect-worker", "Dockerfile")
]
HASH_STORAGE = os.path.join(current_dir, ".requirements_hashes.json")

# --- FUNÇÕES DE APOIO ---

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
    return current_hashes != stored_hashes, current_hashes

def save_new_hashes(hashes):
    with open(HASH_STORAGE, 'w') as f: json.dump(hashes, f, indent=4)

def run_command(command, description):
    print(f"[EXEC] {description}...")
    # Importante: shell=True e env=os.environ
    result = subprocess.run(command, shell=True, env=os.environ)
    if result.returncode != 0:
        print(f"[ERRO] Falha durante: {description}.")
        return False
    return True

def extract_image_name():
    """Lê o docker-compose.yml para descobrir o nome da imagem."""
    try:
        with open("docker-compose.yml", "r") as f:
            content = f.read()
            # Procura por 'image: nome-da-imagem:tag'
            match = re.search(r'image:\s+([^\s]+)', content)
            if match:
                return match.group(1)
    except Exception:
        pass
    # Fallback se não achar
    return "custom-prefect-worker:latest"

def graceful_drain(color, timeout=None):
    # Lógica de drenagem (Resumida para caber, mas mantendo a lógica do DOCKER_CMD)
    try:
        cmd = f'{DOCKER_CMD} ps --filter "name=worker-{color}" -q'
        ids = subprocess.run(cmd, shell=True, capture_output=True, text=True, env=os.environ).stdout.split()
    except: ids = []

    if not ids:
        print(f"[INFO] Nenhum container {color} para drenar.")
        return

    print(f"[STOP] Enviando SIGTERM para {color}...")
    subprocess.run(f"{DOCKER_CMD} kill --signal=SIGTERM {' '.join(ids)}", shell=True, env=os.environ)
    
    print(f"[WAIT] Drenando jobs ({timeout or 'Infinito'}s)...")
    start = time.time()
    while True:
        rem = subprocess.run(cmd, shell=True, capture_output=True, text=True, env=os.environ).stdout.strip()
        if not rem: break
        if timeout and (time.time() - start) > timeout:
            print("[TIMEOUT] Forçando parada.")
            break
        time.sleep(5)

# --- FUNÇÃO PRINCIPAL ---

def rebuild_blue_green():
    docker_dir = os.path.join(project_root, "prefect-worker")
    os.chdir(docker_dir)

    needs_build, new_hashes = check_if_build_needed()
    
    # Verifica se tem ALGO rodando
    check_any = subprocess.run(f'{DOCKER_CMD} ps --filter "name=worker-" -q', shell=True, capture_output=True, text=True, env=os.environ).stdout.strip()

    if not needs_build and "--force" not in sys.argv and check_any:
        print("[SKIP] Sem mudanças e worker ativo.")
        sys.exit(0)

    if "--force" in sys.argv: needs_build = True

    # Define Cores
    is_blue = subprocess.run(f'{DOCKER_CMD} ps --filter "name=worker-blue" -q', shell=True, capture_output=True, text=True, env=os.environ).stdout.strip()
    new_color = "green" if is_blue else "blue"
    curr_color = "blue" if is_blue else "green"
    
    print(f"[INFO] Ciclo: {curr_color} -> {new_color}")

    # --- BUILD MANUAL (A GRANDE MUDANÇA) ---
    if needs_build:
        image_name = extract_image_name()
        print(f"[BUILD] Modo Manual via CLI (Bypass Compose). Imagem: {image_name}")
        
        # 1. Pre-Pull Base (Garante que baixa sem pedir senha)
        try:
             with open("Dockerfile", "r") as df:
                 for line in df:
                     if line.strip().startswith("FROM"):
                         subprocess.run(f"{DOCKER_CMD} pull {line.split()[1]}", shell=True, env=os.environ)
                         break
        except: pass

        # 2. Build Nativo (Respeita o bypass de credenciais)
        # Note: -f Dockerfile e context .. (pois estamos na pasta prefect-worker)
        cmd_build = f"{DOCKER_CMD} build -t {image_name} -f Dockerfile .."
        
        if not run_command(cmd_build, f"Construindo {image_name}"):
            sys.exit(1)
    else:
        print("[SKIP] Usando cache.")

    # Sobe o container (Compose usa a imagem que acabamos de criar)
    cmd_up = f"{DOCKER_CMD} compose -p worker-{new_color} up -d --remove-orphans"
    if not run_command(cmd_up, f"Subindo worker-{new_color}"):
        sys.exit(1)

    if needs_build: save_new_hashes(new_hashes)

    print("[WAIT] Estabilizando (15s)...")
    time.sleep(15)

    if curr_color:
        graceful_drain(curr_color, timeout=TIMEOUT_DRAIN)
        run_command(f"{DOCKER_CMD} compose -p worker-{curr_color} down", "Limpando antigo")

    if needs_build:
        subprocess.run(f"{DOCKER_CMD} image prune -f", shell=True, capture_output=True, env=os.environ)

    print(f"[OK] Worker ({new_color}) Ativo.")

if __name__ == "__main__":
    rebuild_blue_green()