import os
import sys
import importlib

# --- CONFIGURAÇÃO DE CAMINHOS ---
current_dir = os.path.dirname(os.path.abspath(__file__)) # .../_ops
project_root = os.path.dirname(current_dir)              # Raiz do projeto (data_plataform)

# Adiciona a raiz e a pasta pipelines ao Path
if project_root not in sys.path:
    sys.path.append(project_root)
pipelines_path = os.path.join(project_root, "pipelines")
if pipelines_path not in sys.path:
    sys.path.append(pipelines_path)

# --- CORREÇÃO: Pastas que realmente existem no seu projeto ---
DIRS_TO_SCAN = [
    "pipelines/flows_prefect", 
    "pipelines/flows_master", 
    "tasks_python"
]

def check_all_imports():
    print(f"🕵️  Iniciando verificação de integridade em: {DIRS_TO_SCAN}")
    error_count = 0
    checked_count = 0

    for directory in DIRS_TO_SCAN:
        base_path = os.path.join(project_root, directory)
        
        # Garante que a pasta existe antes de tentar ler
        if not os.path.exists(base_path):
            print(f"⚠️  Aviso: Pasta não encontrada: {directory}")
            continue

        # Varre recursivamente (subpastas inclusas)
        for root, _, files in os.walk(base_path):
            for filename in files:
                if filename.endswith(".py") and filename != "__init__.py":
                    
                    # 1. Calcula o caminho do módulo (ex: flows_etl.comercial.silver)
                    rel_dir = os.path.relpath(root, project_root)
                    module_name = os.path.join(rel_dir, filename).replace(".py", "").replace(os.sep, ".")
                    
                    print(f"   Testando: {module_name:<60}", end="")
                    
                    try:
                        # 2. Tenta importar. Se tiver erro de sintaxe ou lib faltando, explode aqui.
                        importlib.import_module(module_name)
                        print("✅ OK")
                        checked_count += 1
                        
                    except Exception as e:
                        print(f"❌ FALHA")
                        print(f"      └── Erro: {e}")
                        error_count += 1

    print("-" * 80)
    if error_count > 0:
        print(f"🚫 VERIFICAÇÃO FALHOU: {error_count} erro(s) encontrado(s).")
        sys.exit(1) # Retorna código de erro para travar o Deploy
    else:
        print(f"✨ SUCESSO: {checked_count} arquivos verificados. Nenhum erro encontrado.")
        sys.exit(0)

if __name__ == "__main__":
    check_all_imports()