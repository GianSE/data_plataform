import os
import sys
import importlib
import inspect
from prefect import Flow

# --- CONFIGURAÇÃO DE CAMINHOS ---
current_dir = os.path.dirname(os.path.abspath(__file__)) # .../_ops
project_root = os.path.dirname(current_dir)              # .../pipelines

# Adiciona a raiz ao Path para imports funcionarem
if project_root not in sys.path:
    sys.path.append(project_root)

# Pastas onde ele vai procurar por Flows
DIRS_TO_SCAN = ["flows_prefect", "flows_master"]

def find_and_deploy_flows():
    print(f"🕵️  Iniciando varredura automática em: {DIRS_TO_SCAN}")
    deployment_count = 0

    for directory in DIRS_TO_SCAN:
        base_path = os.path.join(project_root, directory)
        
        # O os.walk desce em todas as subpastas recursivamente
        for root, _, files in os.walk(base_path):
            for filename in files:
                if filename.endswith(".py") and filename != "__init__.py":
                    
                    # 1. Constrói o caminho do módulo Python (ex: flows_etl.comercial.silver)
                    # Relativo à raiz do projeto
                    rel_dir = os.path.relpath(root, project_root)
                    module_path = os.path.join(rel_dir, filename).replace(".py", "").replace(os.sep, ".")
                    
                    try:
                        # 2. Importa o módulo dinamicamente
                        module = importlib.import_module(module_path)
                        
                        # 3. Procura por objetos @flow dentro do módulo
                        # O inspect.getmembers lista tudo que tem no arquivo
                        for name, obj in inspect.getmembers(module):
                            if isinstance(obj, Flow):
                                # Evita deployar flows importados de outros arquivos
                                # Só deploya se o flow foi definido NESTE arquivo
                                if obj.fn.__module__ == module.__name__:
                                    print(f"🚀 Deployando: {obj.name} (em {filename})")
                                    
                                    # Usa o nome do arquivo + nome da função para o entrypoint
                                    entrypoint_str = f"{module_path}:{name}"
                                    
                                    obj.from_source(
                                        source="/app/pipelines",
                                        entrypoint=entrypoint_str
                                    ).deploy(
                                        name=f"Auto Deploy - {obj.name}", 
                                        work_pool_name="process-pool",
                                        build=False,
                                        push=False
                                    )
                                    deployment_count += 1
                                    
                    except Exception as e:
                        print(f"⚠️  Erro ao processar {filename}: {e}")

    print(f"\n✅ Total de Flows deployados: {deployment_count}")

if __name__ == "__main__":
    find_and_deploy_flows()