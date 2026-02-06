import subprocess
import time
import sys
import os
import hashlib
import json
import tempfile
import shutil
import ctypes # <--- Importante para corrigir o caminho do Windows

# --- [FIX CRÍTICO V3] CORREÇÃO DE CAMINHO CURTO E BYPASS ---

def get_long_path(path):
    """Converte caminhos 'curtos' do Windows (User~1) para caminhos reais."""
    try:
        buf = ctypes.create_unicode_buffer(500)
        GetLongPathName = ctypes.windll.kernel32.GetLongPathNameW
        GetLongPathName(path, buf, 500)
        return buf.value
    except Exception:
        return path

# 1. Cria pasta temporária
fake_config_dir_raw = tempfile.mkdtemp()
# 2. Converte para caminho longo (Remove o APFTI0~1 que quebra o Docker)
fake_config_dir = get_long_path(fake_config_dir_raw)

# 3. Cria o config.json
config_path = os.path.join(fake_config_dir, "config.json")
try:
    with open(config_path, "w") as f:
        json.dump({
            "credsStore": "",       # Mata o wincred
            "credsHelpers": {},     # Mata helpers extras
            "auths": {}             # Limpa auths
        }, f)
    
    # 4. Define no ambiente (Cinto)
    os.environ["DOCKER_CONFIG"] = fake_config_dir
    
    # 5. Cria o prefixo do comando (Suspensório)
    # Vamos injetar isso em todo comando docker para OBRIGAR o uso
    DOCKER_CMD = f'docker --config "{fake_config_dir}"'
    
    print(f"[FIX] Bypass ativado. Path corrigido: {fake_config_dir}")

except Exception as e:
    print(f"[WARN] Falha ao configurar bypass: {e}")
    DOCKER_CMD = "docker" # Fallback

# -----------------------------------------------------------

# Tempo de drenagem
TIMEOUT_DRAIN = 300

# --- CONFIGURAÇÃO DE CAMINHOS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)

FILES_TO_MONITOR = [
    os.path.join(project_root, "prefect-worker", "requirements.txt"),
    os.path.join(project_root, "prefect-worker", "Dockerfile")
]

HASH_STORAGE = os.path.join(current_dir, ".requirements_hashes.json")

# --- FUNÇÕES UTILITÁRIAS ---

def get_file_hash(path):
    if not os.path.exists(path):
        return None
    hasher = hashlib.md5()
    with open(path, 'rb') as f:
        buf = f.read()
        hasher.update(buf)
    return hasher.hexdigest()

def check_if_build_needed():
    current_hashes = {}
    for file_path in FILES_TO_MONITOR:
        name = os.path.basename(file_path)
        current_hashes[name] = get_file_hash(file_path)

    stored_hashes = {}
    if os.path.exists(HASH_STORAGE):
        try:
            with open(HASH_STORAGE, 'r') as f:
                stored_hashes = json.load(f)
        except Exception:
            pass 

    if current_hashes != stored_hashes:
        return True, current_hashes
    
    return False, current_hashes

def save_new_hashes(hashes):
    with open(HASH_STORAGE, 'w') as f:
        json.dump(hashes, f, indent=4)

def run_command(command, description):
    print(f"[EXEC] {description}...")
    # Passamos env=os.environ explicitamente para garantir que o Child Process pegue o DOCKER_CONFIG
    result = subprocess.run(command, shell=True, capture_output=False, env=os.environ)
    
    if result.returncode != 0:
        print(f"[ERRO] Falha durante: {description}.")
        return False
    return True

def graceful_drain(color, timeout=None):
    # Usa o prefixo DOCKER_CMD para garantir que o 'docker ps' funcione também
    try:
        cmd_find = f'{DOCKER_CMD} ps --filter "name=worker-{color}" -q'
        ids = subprocess.run(cmd_find, shell=True, capture_output=True, text=True, env=os.environ).stdout.strip().split('\n')
        ids = [x for x in ids if x]
    except Exception:
        ids = []

    if not ids:
        print(f"[INFO] Nenhum container {color} encontrado para drenar.")
        return

    print(f"[STOP] Enviando SIGTERM para infraestrutura antiga ({color})...")
    # Usa o prefixo DOCKER_CMD
    subprocess.run(f"{DOCKER_CMD} kill --signal=SIGTERM {' '.join(ids)}", shell=True, env=os.environ)

    print(f"[WAIT] Aguardando jobs pendentes no '{color}' finalizarem...")
    if timeout:
        print(f"(Timeout: {timeout}s)")
    else:
        tempo_minutos = TIMEOUT_DRAIN / 60
        print(f"(Modo SIGTERM: O script aguardará {tempo_minutos} minutos)")

    start_time = time.time()
    while True:
        # Usa o prefixo DOCKER_CMD
        check_cmd = f'{DOCKER_CMD} ps --filter "name=worker-{color}" -q'
        remaining = subprocess.run(check_cmd, shell=True, capture_output=True, text=True, env=os.environ).stdout.strip()

        if not remaining:
            print(f" [OK] Worker ({color}) desligou graciosamente.")
            break

        if timeout and (time.time() - start_time) > timeout:
            print("[TIMEOUT] Tempo limite esgotado! Forçando desligamento...")
            break
        
        time.sleep(5)

# --- FUNÇÃO PRINCIPAL ---

def rebuild_blue_green():
    docker_dir = os.path.join(project_root, "prefect-worker")
    os.chdir(docker_dir)

    needs_build, new_hashes = check_if_build_needed()

    # Usa o prefixo DOCKER_CMD
    check_any_worker = subprocess.run(
        f'{DOCKER_CMD} ps --filter "name=worker-" -q', 
        shell=True, capture_output=True, text=True, env=os.environ
    ).stdout.strip()

    if not needs_build and "--force" not in sys.argv and check_any_worker:
        print("[SKIP] Nenhuma mudança detectada no requirements.txt ou Dockerfile.")
        print("[SKIP] O worker atual já está rodando.")
        sys.exit(0)

    if not check_any_worker:
        print("[INFO] Nenhum worker ativo encontrado. Iniciando subida de nova infraestrutura...")

    if "--force" in sys.argv:
        needs_build = True
        print("[INFO] Modo --force detectado. Ignorando cache.")

    is_blue_active = False
    try:
        # Usa o prefixo DOCKER_CMD
        check_blue = subprocess.run(
            f'{DOCKER_CMD} ps --filter "name=worker-blue" -q', 
            shell=True, capture_output=True, text=True, env=os.environ
        ).stdout.strip()
        if check_blue:
            is_blue_active = True
    except Exception:
        pass

    new_color = "green" if is_blue_active else "blue"
    current_color = "blue" if is_blue_active else "green"
    
    print(f"[INFO] Ciclo Blue-Green: {current_color} (Atual) -> {new_color} (Novo)")

    # 3. Build e Up do Novo
    if needs_build:
        print(f"[BUILD] Mudanças detectadas. Iniciando Build do {new_color}...")
        
        # --- AQUI ESTÁ O TRUQUE: Injetamos DOCKER_CMD antes do 'compose' ---
        # Isso vira: "docker --config 'C:\Users\...' compose -p ..."
        cmd_build = f"{DOCKER_CMD} compose -p worker-{new_color} build --no-cache"
        
        if not run_command(cmd_build, f"Construindo imagem worker-{new_color}"):
            sys.exit(1)
    else:
        print("[SKIP] Nenhuma mudança nas dependências. Usando imagem em cache.")

    # Sobe a nova stack usando o prefixo DOCKER_CMD
    cmd_up = f"{DOCKER_CMD} compose -p worker-{new_color} up -d --remove-orphans"
    success = run_command(cmd_up, f"Subindo worker-{new_color}")
    if not success:
        sys.exit(1)

    if needs_build:
        save_new_hashes(new_hashes)

    print("[WAIT] Aguardando 15 segundos para o novo worker estabilizar...")
    time.sleep(15)

    # 6. Drenagem e Remoção do Antigo
    if current_color:
        graceful_drain(current_color, timeout=TIMEOUT_DRAIN)

        print(f"[CLEAN] Removendo stack antiga ({current_color})...")
        # Usa o prefixo DOCKER_CMD
        run_command(f"{DOCKER_CMD} compose -p worker-{current_color} down", "Limpando recursos antigos")

    if needs_build:
        print("[CLEAN] Limpando imagens antigas (dangling)...")
        # Usa o prefixo DOCKER_CMD
        subprocess.run(f"{DOCKER_CMD} image prune -f", shell=True, capture_output=True, env=os.environ)

    print(f"[OK] Sucesso! Apenas Worker ({new_color}) está ativo.")

if __name__ == "__main__":
    rebuild_blue_green()