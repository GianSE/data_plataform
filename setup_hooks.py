import os
import stat
import shutil
import sys

def install_hooks():
    print("🔧 Configurando ambiente de desenvolvimento...")

    # 1. Identificar caminhos
    current_dir = os.path.dirname(os.path.abspath(__file__)) # pasta pipe/
    
    # Caminho onde o hook está salvo no projeto (Source)
    hook_source = os.path.join(current_dir, "data_plataform", "pipelines", "_ops", "hooks", "pre-push")
    
    # Caminho onde o Git espera que o hook esteja (Destination)
    # Assume que a pasta .git está um nível acima ou na mesma pasta de 'pipe'
    # Ajuste conforme onde está sua pasta .git
    git_dir = os.path.join(current_dir, "..", ".git") 
    
    # Se 'pipe' for a raiz do repo, então é:
    if not os.path.exists(git_dir):
        git_dir = os.path.join(current_dir, ".git")

    if not os.path.exists(git_dir):
        print("❌ Erro: Pasta .git não encontrada. Você clonou o repositório corretamente?")
        return

    hooks_dir = os.path.join(git_dir, "hooks")
    hook_dest = os.path.join(hooks_dir, "pre-push")

    # 2. Copiar o arquivo
    try:
        print(f"   📂 Copiando hook para: {hook_dest}")
        shutil.copyfile(hook_source, hook_dest)
    except FileNotFoundError:
        print(f"❌ Erro: Arquivo fonte não encontrado em: {hook_source}")
        return

    # 3. Dar permissão de execução (chmod +x)
    # Isso é crucial para Linux/Mac (e Git Bash no Windows)
    st = os.stat(hook_dest)
    os.chmod(hook_dest, st.st_mode | stat.S_IEXEC)

    print("✅ Hook 'pre-push' instalado com sucesso!")
    print("🚀 Agora, todo 'git push' será verificado automaticamente.")

if __name__ == "__main__":
    install_hooks()