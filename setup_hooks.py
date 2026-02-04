# setup_hooks.py
import shutil

hooks_src = "_ops/hooks"
hooks_dst = ".git/hooks"

# Copia os arquivos
shutil.copy(f"{hooks_src}/pre-commit", f"{hooks_dst}/pre-commit")
shutil.copy(f"{hooks_src}/pre-push", f"{hooks_dst}/pre-push")

print("✅ Hooks do Git instalados com sucesso!")