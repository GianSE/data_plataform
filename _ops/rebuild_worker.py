import subprocess
import time
import sys
import os
import hashlib
import json

# --- CONFIGURAÇÃO DE CAMINHOS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)

# Arquivos que disparam rebuild se mudarem
FILES_TO_MONITOR = [
    os.path.join(project_root, "prefect-worker", "requirements.txt"),
    os.path.join(project_root, "prefect-worker", "Dockerfile")
]

HASH_STORAGE = os.path.join(current_dir, ".requirements_hashes.json")

def get_file_hash(path):
    """Calcula MD5 do arquivo para saber se mudou"""
    if not os.path.exists(path):
        return None
    hasher = hashlib.md5()
    with open(path, 'rb') as f:
        buf = f.read()
        hasher.update(buf)
    return hasher.hexdigest()

def check_if_build_needed():
    """Retorna True se houve mudança nos arquivos monitorados"""
    current_hashes = {}
    
    # 1. Calcula hashes atuais
    for file_path in FILES_TO_MONITOR:
        name = os.path.basename(file_path)
        current_hashes[name] = get_file_hash(file_path)

    # 2. Carrega histórico anterior
    stored_hashes = {}
    if os.path.exists(HASH_STORAGE):
        try:
            with open(HASH_STORAGE, 'r') as f:
                stored_hashes = json.load(f)
        except:
            pass 

    # 3. Compara
    if current_hashes != stored_hashes:
        return True, current_hashes
    
    return False, current_hashes

def save_new_hashes(hashes):
    with open(HASH_STORAGE, 'w') as f:
        json.dump(hashes, f, indent=4)

def run_command(command, description):
    print(f"[EXEC] {description}...")
    result = subprocess.run(command, shell=True, capture_output=False)
    
    if result.returncode != 0:
        print(f"[ERRO] Falha durante: {description}.")
        return False
    return True

def rebuild_blue_green():
    # Caminho do Docker Compose
    docker_dir = os.path.join(project_root, "prefect-worker")
    if not os.path.exists(docker_dir):
        docker_dir = os.path.join(project_root, "prefect") 
    
    os.chdir(docker_dir)

    # 1. Verifica Inteligência (Hashes)
    needs_build, new_hashes = check_if_build_needed()
    
    if "--force" in sys.argv:
        needs_build = True
        print("[INFO] Modo --force detectado. Ignorando cache.")

    # 2. Identificar cor atual
    is_blue_active = False
    try:
        check_blue = subprocess.run(
            'docker ps --filter "name=worker-blue" -q', 
            shell=True, capture_output=True, text=True
        ).stdout.strip()
        if check_blue:
            is_blue_active = True
    except:
        pass

    new_color = "green" if is_blue_active else "blue"
    current_color = "blue" if is_blue_active else "green"
    
    print(f"[INFO] Ciclo Blue-Green: {current_color} (Atual) -> {new_color} (Novo)")

    # --- CORREÇÃO AQUI: BUILD SEPARADO ---
    if needs_build:
        print("[BUILD] Mudanças detectadas. Iniciando Build ZERO CACHE...")
        # Passo 1: Build explícito com --no-cache
        cmd_build = f"docker compose -p worker-{new_color} build --no-cache"
        if not run_command(cmd_build, f"Construindo imagem worker-{new_color}"):
            sys.exit(1)
    else:
        print("[SKIP] Nenhuma mudança nas dependências. Usando imagem em cache.")

    # Passo 2: Up (apenas sobe, pois o build já foi feito ou não é necessário)
    cmd_up = f"docker compose -p worker-{new_color} up -d --remove-orphans"
    
    success = run_command(cmd_up, f"Subindo worker-{new_color}")
    if not success:
        sys.exit(1)

    # 4. Salva hashes se houve sucesso no build
    if needs_build:
        save_new_hashes(new_hashes)

    # 5. Estabilização
    print("[WAIT] Aguardando 10 segundos para estabilização...")
    time.sleep(10)

    # 6. Parar o antigo
    try:
        old_ids = subprocess.run(
            f'docker ps --filter "name=worker-{current_color}" -q',
            shell=True, capture_output=True, text=True
        ).stdout.strip().split('\n')
        
        old_ids = [oid for oid in old_ids if oid]

        if old_ids:
            print(f"[STOP] Parando infraestrutura antiga ({current_color})...")
            subprocess.run(f"docker kill {' '.join(old_ids)}", shell=True, capture_output=True)
            run_command(f"docker compose -p worker-{current_color} down", "Removendo stack antiga")
        else:
            print(f"[INFO] Nenhum container {current_color} encontrado para remover.")

    except Exception as e:
        print(f"[AVISO] Erro ao tentar remover antigo: {e}")

    # 7. Limpeza de Imagens
    if needs_build:
        print("[CLEAN] Limpando imagens antigas (dangling)...")
        subprocess.run("docker image prune -f", shell=True, capture_output=True)

    print(f"[OK] Sucesso! Worker ({new_color}) está ativo.")

if __name__ == "__main__":
    rebuild_blue_green()