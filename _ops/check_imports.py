import os
import sys
import importlib

# --- CONFIGURAÇÃO DE CAMINHOS ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir) # Raiz do repositório

# Pastas que precisam estar no PATH para resolver os imports internos
# Baseado na estrutura: pipelines/flows_prefect/_shared e pipelines/flows_master/catalog
paths_to_add = [
    project_root,
    os.path.join(project_root, "flows_prefect"),
    os.path.join(project_root, "flows_master"),
    os.path.join(project_root, "tasks_python")
]

for p in paths_to_add:
    if p not in sys.path:
        sys.path.insert(0, p)

# Pastas para auditoria conforme o README
DIRS_TO_SCAN = ["flows_prefect", "tasks_python"]

def check_all_imports():
    print(f"🕵️  Iniciando verificação de integridade em: {DIRS_TO_SCAN}")
    error_count = 0
    checked_count = 0

    for directory in DIRS_TO_SCAN:
        base_path = os.path.join(project_root, directory)
        if not os.path.exists(base_path):
            print(f"⚠️  Aviso: Pasta não encontrada: {directory}")
            continue

        for root, _, files in os.walk(base_path):
            for filename in files:
                if filename.endswith(".py") and filename != "__init__.py":
                    # Calcula o nome do módulo relativo à raiz
                    rel_dir = os.path.relpath(root, project_root)
                    module_name = os.path.join(rel_dir, filename).replace(".py", "").replace(os.sep, ".")
                    
                    print(f"   Testando: {module_name:<60}", end="")
                    try:
                        importlib.import_module(module_name)
                        print("✅ OK")
                        checked_count += 1
                    except Exception as e:
                        print("❌ FALHA")
                        print(f"      └── Erro: {e}")
                        error_count += 1

    print("-" * 80)
    sys.exit(1 if error_count > 0 else 0)

if __name__ == "__main__":
    check_all_imports()