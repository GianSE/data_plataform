import shutil
import os
import subprocess
import sys

# Caminhos
hooks_src = "_ops/hooks"
hooks_dst = ".git/hooks"
hooks_to_sync = ["pre-commit"] 

print("🚀 Iniciando Setup do Ambiente de Desenvolvimento...\n")

# --- 1. INSTALAÇÃO DE FERRAMENTAS ---
print("📦 [1/4] Verificando dependências...")
try:
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "ruff", "bandit"],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
    )
    print("   ✅ Ruff e Bandit OK!")
except Exception:
    print("   ⚠️  Aviso na instalação do pip.")

# --- 2. CONFIGURAÇÃO DOS HOOKS ---
print("\n🔄 [2/4] Sincronizando Git Hooks...")
if not os.path.exists(hooks_dst):
    try: 
        os.makedirs(hooks_dst)
    except Exception: 
        pass

for hook in ["pre-commit", "pre-push"]:
    src = os.path.join(hooks_src, hook)
    dst = os.path.join(hooks_dst, hook)
    
    if hook in hooks_to_sync and os.path.exists(src):
        shutil.copy(src, dst)
        print(f"   ✅ {hook} atualizado.")
    elif os.path.exists(dst):
        try: 
            os.remove(dst) 
            print(f"   🗑️  {hook} removido.")
        except Exception: 
            pass

# --- 3. CONFIGURAÇÃO DO GIT ---
print("\n⚙️  [3/4] Ajustando Git...")
try:
    subprocess.run(["git", "config", "core.safecrlf", "false"], check=True)
    subprocess.run(["git", "config", "core.autocrlf", "input"], check=True)
    print("   ✅ Git CRLF OK!")
except Exception: 
    pass

# --- 4. CONFIGURAÇÃO DO ALIAS 'WORKER' (MÉTODO SEGURO) ---
print("\n⌨️  [4/4] Configurando atalho 'worker'...")

try:
    # 1. Pede ao PowerShell onde fica o arquivo de perfil
    result = subprocess.run(
        ["powershell", "-Command", "echo $PROFILE"], 
        capture_output=True, text=True
    )
    profile_path = result.stdout.strip()

    if profile_path:
        # Garante que a pasta existe
        os.makedirs(os.path.dirname(profile_path), exist_ok=True)
        
        # O código exato que queremos escrever (sem brigar com aspas do shell)
        worker_func = """
function worker {
    $id = docker ps --filter "name=worker" --format "{{.Names}}" | Select-Object -First 1
    if ($id) {
        Write-Host "🚀 Entrando em: $id" -ForegroundColor Cyan
        docker exec -it $id /bin/bash
    } else {
        Write-Host "❌ Nenhum worker rodando." -ForegroundColor Red
    }
}
"""
        # Lê o arquivo atual para não duplicar
        content = ""
        if os.path.exists(profile_path):
            with open(profile_path, "r", encoding="utf-8-sig") as f:
                content = f.read()

        # Escreve apenas se necessário
        if "function worker" not in content:
            with open(profile_path, "a", encoding="utf-8-sig") as f:
                f.write("\n" + worker_func)
            print(f"   ✅ Alias gravado em: {profile_path}")
        else:
            print("   ⚡ Alias já existe. Nenhuma alteração.")
            
    else:
        print("   ⚠️  Não consegui localizar o perfil do PowerShell.")

except Exception as e:
    print(f"   ⚠️  Erro ao configurar alias: {e}")

print("\n✨ Setup finalizado! 🛡️")