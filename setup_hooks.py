# setup_hooks.py
import shutil
import subprocess # <--- Adicione este import
import os

hooks_src = "_ops/hooks"
hooks_dst = ".git/hooks"

# 1. Instalação dos Hooks (código original)
try:
    if not os.path.exists(hooks_dst):
        os.makedirs(hooks_dst)
        
    shutil.copy(f"{hooks_src}/pre-commit", f"{hooks_dst}/pre-commit")
    print("✅ Hooks do Git instalados com sucesso!")
except Exception as e:
    print(f"❌ Erro ao copiar hooks: {e}")

# 2. Configuração Automática do Git (NOVO)
print("⚙️  Ajustando configurações do Git para Windows/Docker...")
try:
    # Desativa o aviso chato de CRLF
    subprocess.run(["git", "config", "core.safecrlf", "false"], check=True)
    
    # Opcional: Garante que o Git saiba que queremos LF (reforça o .gitattributes)
    subprocess.run(["git", "config", "core.autocrlf", "input"], check=True)
    
    print("✅ Git configurado para ignorar avisos de CRLF!")
except Exception as e:
    print(f"⚠️  Aviso: Não foi possível rodar o git config automaticamente: {e}")