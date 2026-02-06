import subprocess
import time
import sys
import os
import hashlib
import json

# Tempo que espera até matar o container em segudos 300 = 5min
# None = Infinito
TIMEOUT_DRAIN = 300

# --- CONFIGURAÇÃO DE CAMINHOS ---
# Usa caminho absoluto para evitar erros de "file not found"
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)

# Arquivos que disparam rebuild se mudarem
FILES_TO_MONITOR = [
    os.path.join(project_root, "prefect-worker", "requirements.txt"),
    os.path.join(project_root, "prefect-worker", "Dockerfile")
]

HASH_STORAGE = os.path.join(current_dir, ".requirements_hashes.json")
# --- FUNÇÕES UTILITÁRIAS ---

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
        except Exception:
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

# --- NOVA FUNÇÃO: DRENAGEM (SIGTERM) ---
def graceful_drain(color, timeout=None):
    """
    Envia SIGTERM para os containers da cor antiga e aguarda finalização.
    timeout=None espera infinitamente (recomendado para ETLs longos).
    """
    # 1. Identifica os IDs dos containers antigos
    try:
        cmd_find = f'docker ps --filter "name=worker-{color}" -q'
        ids = subprocess.run(cmd_find, shell=True, capture_output=True, text=True).stdout.strip().split('\n')
        ids = [x for x in ids if x] # Remove vazios
    except Exception:
        ids = []

    if not ids:
        print(f"[INFO] Nenhum container {color} encontrado para drenar.")
        return

    # 2. Envia SIGTERM (Para de pegar jobs, termina os atuais)
    print(f"[STOP] Enviando SIGTERM para infraestrutura antiga ({color})...")
    subprocess.run(f"docker kill --signal=SIGTERM {' '.join(ids)}", shell=True)

    # 3. Loop de Espera
    print(f"[WAIT] Aguardando jobs pendentes no '{color}' finalizarem...")
    if timeout:
        print(f"(Timeout: {timeout}s)")
    else:
        tempo_minutos = TIMEOUT_DRAIN / 60
        print(f"(Modo SIGTERM: O script aguardará {tempo_minutos} minutos até o fim dos jobs)")

    start_time = time.time()
    while True:
        # Verifica se ainda existe algum container rodando
        check_cmd = f'docker ps --filter "name=worker-{color}" -q'
        remaining = subprocess.run(check_cmd, shell=True, capture_output=True, text=True).stdout.strip()

        if not remaining:
            print(" [OK] Worker ({color}) desligou graciosamente.")
            break

        # Checagem de Timeout (se definido)
        if timeout and (time.time() - start_time) > timeout:
            print("[TIMEOUT] Tempo limite esgotado! Forçando desligamento...")
            break
        
        time.sleep(5)

# --- FUNÇÃO PRINCIPAL ---

def rebuild_blue_green():
    docker_dir = os.path.join(project_root, "prefect-worker")
    os.chdir(docker_dir)

    # 1. Verifica Inteligência (Hashes)
    needs_build, new_hashes = check_if_build_needed()
    
    # --- NOVA LÓGICA DE INTELIGÊNCIA BLINDADA ---
    # 1. Verifica se houve mudança nos arquivos
    needs_build, new_hashes = check_if_build_needed()

    # 2. Verifica se existe ALGUM worker rodando (Independente da cor)
    check_any_worker = subprocess.run(
        'docker ps --filter "name=worker-" -q', 
        shell=True, capture_output=True, text=True
    ).stdout.strip()

    # 3. Decisão: Só dá SKIP se não mudou nada E se já tem alguém trabalhando
    if not needs_build and "--force" not in sys.argv and check_any_worker:
        print("[SKIP] Nenhuma mudança detectada no requirements.txt ou Dockerfile.")
        print("[SKIP] O worker atual já está rodando. Mantendo execução para evitar crash.")
        sys.exit(0)

    # Se chegou aqui, ou teve mudança, ou o force foi usado, ou não tem nenhum container vivo
    if not check_any_worker:
        print("[INFO] Nenhum worker ativo encontrado. Iniciando subida de nova infraestrutura...")
    # -----------------------------------

    if "--force" in sys.argv:
        needs_build = True
        print("[INFO] Modo --force detectado. Ignorando cache.")

    # 2. Identificar cor atual
    is_blue_active = False
    try:
        # Verifica se existe algum container rodando com 'worker-blue' no nome
        check_blue = subprocess.run(
            'docker ps --filter "name=worker-blue" -q', 
            shell=True, capture_output=True, text=True
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
        # Build explícito para o projeto específico
        cmd_build = f"docker compose -p worker-{new_color} build --no-cache"
        if not run_command(cmd_build, f"Construindo imagem worker-{new_color}"):
            sys.exit(1)
    else:
        print("[SKIP] Nenhuma mudança nas dependências. Usando imagem em cache.")

    # Sobe a nova stack (Define o nome do projeto com -p)
    cmd_up = f"docker compose -p worker-{new_color} up -d --remove-orphans"
    success = run_command(cmd_up, f"Subindo worker-{new_color}")
    if not success:
        sys.exit(1)

    # 4. Salva hashes se houve sucesso no build (e se foi necessário)
    if needs_build:
        save_new_hashes(new_hashes)

    # 5. Estabilização (Para garantir que o novo comece a pegar jobs)
    print("[WAIT] Aguardando 15 segundos para o novo worker estabilizar...")
    time.sleep(15)

    
    # 6. Drenagem e Remoção do Antigo
    if current_color:
        # AQUI ESTÁ A MUDANÇA: Chama a drenagem em vez de matar direto
        graceful_drain(current_color, timeout=TIMEOUT_DRAIN) # timeout=None espera pra sempre

        # Depois que desligou, remove a stack (limpa redes e referências)
        print(f"[CLEAN] Removendo stack antiga ({current_color})...")
        run_command(f"docker compose -p worker-{current_color} down", "Limpando recursos antigos")

    # 7. Limpeza de Imagens soltas
    if needs_build:
        print("[CLEAN] Limpando imagens antigas (dangling)...")
        subprocess.run("docker image prune -f", shell=True, capture_output=True)

    print(f"[OK] Sucesso! Apenas Worker ({new_color}) está ativo.")

if __name__ == "__main__":
    rebuild_blue_green()