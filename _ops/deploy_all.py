import os
import sys
import subprocess
import time

# --- DETECÇÃO DE AMBIENTE (O PULO DO GATO 🐱) ---
# Verifica se estamos rodando dentro de um container Docker
# A presença do arquivo /.dockerenv é o padrão universal do Docker
IN_DOCKER = os.path.exists("/.dockerenv")

# Caminho fixo do script DENTRO do container
INTERNAL_SCRIPT_PATH = "/app/_ops/deploy_all.py"

if not IN_DOCKER:
    # --- MODO HOST (WINDOWS) ---
    print("\n🖥️  Você está no HOST (Windows).")
    print("📡 Conectando ao container 'prefect_worker' para rodar o deploy internamente...\n")
    
    try:
        # Chama o Docker para rodar ESTE mesmo script, mas lá dentro
        # O -t mantém as cores do terminal
        cmd = ["docker", "exec", "-t", "prefect_worker", "python", INTERNAL_SCRIPT_PATH]
        
        # Executa e espera terminar
        result = subprocess.run(cmd)
        
        if result.returncode == 0:
            print("\n✅ Sucesso! Comando remoto finalizado.")
        else:
            print("\n❌ Erro ao executar no container.")
            
    except FileNotFoundError:
        print("❌ Erro: O comando 'docker' não foi encontrado. Você tem o Docker instalado?")
    except KeyboardInterrupt:
        print("\n⚠️ Interrompido pelo usuário.")
        
    # Encerra o script aqui para o Windows não tentar rodar o resto
    sys.exit(0)

# ==============================================================================
# DAQUI PRA BAIXO É O CÓDIGO QUE RODA DENTRO DO CONTAINER (MODO WORKER)
# ==============================================================================

import importlib
import inspect
from prefect import Flow

# --- CONFIGURAÇÃO DE CAMINHOS ---
current_dir = os.path.dirname(os.path.abspath(__file__)) 
app_root = os.path.dirname(current_dir) # /app

# Definir raízes importantes do projeto
pipelines_root = os.path.join(app_root, "pipelines")
flows_prefect_root = os.path.join(pipelines_root, "flows_prefect")
flows_master_root = os.path.join(pipelines_root, "flows_master")
tasks_root = os.path.join(app_root, "tasks_python")

# Lista de caminhos que PRECISAM estar no sys.path para os imports funcionarem
paths_to_add = [
    app_root,            # Raiz
    pipelines_root,      # Permite: from flows_prefect import ...
    flows_prefect_root,  # Permite: from _shared import ...
    flows_master_root,   # Permite: import catalog 
    tasks_root           # Permite: from settings import ...
]

# Injeta os caminhos no Python Local
for p in paths_to_add:
    if p not in sys.path:
        sys.path.insert(0, p)

# String do PYTHONPATH para injetar no Worker
pythonpath_str = ":".join(paths_to_add)

DIRS_TO_SCAN = ["flows_prefect", "flows_master"]

def find_and_deploy_flows():
    print(f"🕵️  [Container] Varrendo fluxos em: {pipelines_root}")
    deployment_count = 0

    for directory in DIRS_TO_SCAN:
        base_path = os.path.join(pipelines_root, directory)
        
        if not os.path.exists(base_path):
            continue

        for root, _, files in os.walk(base_path):
            for filename in files:
                if filename.endswith(".py") and filename != "__init__.py":
                    try:
                        # 1. Importação Dinâmica
                        rel_dir = os.path.relpath(root, pipelines_root)
                        module_path = os.path.join(rel_dir, filename).replace(".py", "").replace(os.sep, ".")
                        
                        module = importlib.import_module(module_path)
                        
                        # 2. Busca @flow
                        for name, obj in inspect.getmembers(module):
                            if isinstance(obj, Flow):
                                if obj.fn.__module__ == module.__name__:
                                    print(f"🚀 Deployando: {obj.name} ({filename})")
                                    
                                    # Entrypoint correto
                                    file_rel_path = os.path.join(rel_dir, filename)
                                    entrypoint_str = f"{file_rel_path}:{name}"
                                    
                                    obj.from_source(
                                        source="/app/pipelines",
                                        entrypoint=entrypoint_str
                                    ).deploy(
                                        name=f"{obj.name}", 
                                        work_pool_name="process-pool",
                                        build=False,
                                        push=False,
                                        job_variables={"env": {"PYTHONPATH": pythonpath_str}}
                                    )
                                    deployment_count += 1
                                    
                    except Exception as e:
                        print(f"⚠️  Erro em {filename}: {e}")

    print(f"\n✅ Total de Flows deployados: {deployment_count}")

if __name__ == "__main__":
    find_and_deploy_flows()