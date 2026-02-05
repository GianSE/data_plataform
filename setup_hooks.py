import shutil
import os
import subprocess
import sys

# Caminhos
hooks_src = "_ops/hooks"
hooks_dst = ".git/hooks"
hooks_to_sync = ["pre-commit"] # pre-push removido pois agora tudo está no pre-commit

print("🚀 Iniciando Setup do Ambiente de Desenvolvimento...\n")

# --- 1. INSTALAÇÃO DE FERRAMENTAS (Ruff & Bandit) ---
print("📦 [1/3] Verificando e instalando dependências de qualidade...")
try:
    # Usa o pip do python atual para instalar
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "ruff", "bandit"],
        stdout=subprocess.DEVNULL, # Silencia o log de instalação se der certo
        stderr=subprocess.PIPE
    )
    print("   ✅ Ruff e Bandit instalados/atualizados com sucesso!")
except subprocess.CalledProcessError:
    print("   ❌ Erro ao instalar bibliotecas via pip. Verifique sua conexão.")
except Exception as e:
    print(f"   ⚠️  Aviso: {e}")

# --- 2. CONFIGURAÇÃO DOS HOOKS ---
print("\n🔄 [2/3] Sincronizando Git Hooks...")

# Garante que a pasta existe
if not os.path.exists(hooks_dst):
    try:
        os.makedirs(hooks_dst)
    except Exception:
        pass

# Instala o pre-commit e remove o antigo pre-push se existir
for hook in ["pre-commit", "pre-push"]:
    src = os.path.join(hooks_src, hook)
    dst = os.path.join(hooks_dst, hook)
    
    # Se for o pre-commit (que queremos manter)
    if hook in hooks_to_sync and os.path.exists(src):
        shutil.copy(src, dst)
        print(f"   ✅ {hook} instalado/atualizado.")
    
    # Se for o pre-push (que queremos matar) ou arquivo velho
    elif os.path.exists(dst):
        try:
            os.remove(dst)
            print(f"   🗑️  {hook} removido (limpeza).")
        except Exception:
            pass

# --- 3. CONFIGURAÇÃO DO GIT (Windows/CRLF) ---
print("\n⚙️  [3/3] Ajustando configurações do Git...")
try:
    # Ignora aviso de CRLF
    subprocess.run(["git", "config", "core.safecrlf", "false"], check=True)
    # Define preferência de quebra de linha
    subprocess.run(["git", "config", "core.autocrlf", "input"], check=True)
    print("   ✅ Git configurado para ignorar avisos de quebra de linha (CRLF)!")
except Exception:
    print("   ⚠️  Não foi possível ajustar config do git (pode ignorar).")

print("\n✨ Setup finalizado! Seu ambiente está blindado. 🛡️")