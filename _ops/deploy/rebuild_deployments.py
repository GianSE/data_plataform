import os
import sys
import subprocess
import io
import hashlib
import json

# --- ESCUDO PARA TERMINAL (FIX UNICODE) ---
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

# --- DETECÇÃO DE AMBIENTE ---
IN_DOCKER = os.path.exists("/.dockerenv")

if not IN_DOCKER:
    print("\n[PC] Redirecionando para o container do worker...")
    try:
        # Busca o ID do container custom-prefect-worker que está rodando
        container_id = subprocess.run(
            'docker ps --filter "ancestor=custom-prefect-worker:3.6.13-python3.12" --format "{{.ID}}"',
            shell=True, capture_output=True, text=True
        ).stdout.strip().split('\n')[0]

        if not container_id:
            raise Exception("Nenhum container do worker encontrado rodando.")

        # Executa o próprio script dentro do container
        subprocess.run(["docker", "exec", "-t", container_id, "python", "/app/_ops/deploy/rebuild_deployments.py"], check=True)
    except Exception as e:
        print(f"\n[ERRO] Falha ao redirecionar: {e}")
    sys.exit(0)

# ==============================================================================
# MODO CONTAINER: DEPLOY INTELIGENTE (MD5)
# ==============================================================================

BASE_FLOWS_DIR = "/app/flows_prefect"
# DICA: Para persistir, este ficheiro deve estar num VOLUME mapeado no docker-compose
HASH_STORAGE = "/app/_metadata/.deployments_hashes.json"

def get_file_hash(path):
    """Calcula o MD5 do conteúdo do ficheiro."""
    hasher = hashlib.md5()
    with open(path, 'rb') as f:
        hasher.update(f.read())
    return hasher.hexdigest()

def load_hashes():
    if os.path.exists(HASH_STORAGE):
        try:
            with open(HASH_STORAGE, 'r') as f:
                return json.load(f)
        except: return {}
    return {}

def save_hashes(hashes):
    with open(HASH_STORAGE, 'w') as f:
        json.dump(hashes, f, indent=4)

def find_and_execute_deploys():
    force_all = "--all" in sys.argv
    print(f"[INFO] [Auto-Deploy] Iniciando varredura em: {BASE_FLOWS_DIR}")
    
    current_hashes = load_hashes()
    new_hashes = current_hashes.copy() # Mantém o que já existe
    
    success_count = 0
    error_count = 0
    skipped_count = 0

    for root, dirs, files in os.walk(BASE_FLOWS_DIR):
        if "_shared" in dirs: dirs.remove("_shared") 
        
        for filename in files:
            if filename.startswith("flow_") and filename.endswith(".py"):
                file_path = os.path.join(root, filename)
                file_hash = get_file_hash(file_path)
                
                # Verifica se o ficheiro mudou comparando com o JSON
                if not force_all and current_hashes.get(filename) == file_hash:
                    skipped_count += 1
                    continue

                print(f"[DEPLOY] Processando: {filename}...", end=" ", flush=True)
                
                try:
                    # Executa o deploy do flow no Prefect
                    subprocess.run(
                        ["python", "-u", file_path, "deploy"],
                        capture_output=True, text=True, check=True
                    )
                    print("OK")
                    new_hashes[filename] = file_hash # Só atualiza o hash se der OK
                    success_count += 1
                except subprocess.CalledProcessError as e:
                    print("FALHOU")
                    print(f"--- ERRO EM {filename} ---\n{e.stderr}\n{'-'*30}")
                    error_count += 1

    save_hashes(new_hashes)
    print(f"\n[FIM] Sucessos: {success_count} | Falhas: {error_count} | Ignorados: {skipped_count}")

if __name__ == "__main__":
    find_and_execute_deploys()