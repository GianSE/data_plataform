import os
import sys
import subprocess
import io

# --- ESCUDO PARA TERMINAL (FIX UNICODE/CHARMAP) ---
# Força o terminal a aceitar UTF-8 mesmo em sessões remotas (WinRM/PowerShell)
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

# --- DETECÇÃO DE AMBIENTE ---
IN_DOCKER = os.path.exists("/.dockerenv")
INTERNAL_SCRIPT_PATH = "/app/_ops/deploy_all.py"

if not IN_DOCKER:
    # --- MODO HOST (WINDOWS) ---
    print("\n[PC] Você está no HOST (Windows).")
    print("[RUN] Conectando ao container 'prefect_worker' para rodar o deploy...\n")
    
    try:
        # Chama o Docker para rodar este mesmo script lá dentro
        cmd = ["docker", "exec", "-t", "prefect_worker", "python", INTERNAL_SCRIPT_PATH]
        result = subprocess.run(cmd)
        
        if result.returncode == 0:
            print("\n[OK] Deploy finalizado com sucesso!")
        else:
            print("\n[ERRO] Falha ao executar deploy no container.")
            
    except FileNotFoundError:
        print("[ERRO] Comando 'docker' não encontrado.")
    except KeyboardInterrupt:
        print("\n[STOP] Interrompido pelo usuário.")
    sys.exit(0)

# ==============================================================================
# MODO CONTAINER (O QUE RODA DE FATO NO PREFECT)
# ==============================================================================
import importlib
import inspect
from prefect import Flow

current_dir = os.path.dirname(os.path.abspath(__file__)) 
app_root = os.path.dirname(current_dir)
pipelines_root = os.path.join(app_root, "pipelines")
flows_prefect_root = os.path.join(pipelines_root, "flows_prefect")
flows_master_root = os.path.join(pipelines_root, "flows_master")
tasks_root = os.path.join(app_root, "tasks_python")

paths_to_add = [app_root, pipelines_root, flows_prefect_root, flows_master_root, tasks_root]

for p in paths_to_add:
    if p not in sys.path:
        sys.path.insert(0, p)

pythonpath_str = ":".join(paths_to_add)
DIRS_TO_SCAN = ["flows_prefect", "flows_master"]

def find_and_deploy_flows():
    print(f"[Container] Varrendo fluxos em: {pipelines_root}")
    deployment_count = 0

    for directory in DIRS_TO_SCAN:
        base_path = os.path.join(pipelines_root, directory)
        if not os.path.exists(base_path): continue

        for root, _, files in os.walk(base_path):
            for filename in files:
                if filename.endswith(".py") and filename != "__init__.py":
                    try:
                        rel_dir = os.path.relpath(root, pipelines_root)
                        module_path = os.path.join(rel_dir, filename).replace(".py", "").replace(os.sep, ".")
                        module = importlib.import_module(module_path)
                        
                        for name, obj in inspect.getmembers(module):
                            if isinstance(obj, Flow):
                                if obj.fn.__module__ == module.__name__:
                                    print(f"Deployando: {obj.name} ({filename})")
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
                        print(f"[ERRO] em {filename}: {e}")

    print(f"\n[OK] Total de Flows deployados: {deployment_count}")

if __name__ == "__main__":
    find_and_deploy_flows()