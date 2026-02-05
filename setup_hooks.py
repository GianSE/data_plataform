import shutil
import os
import subprocess
import sys

# Caminhos
hooks_src = "_ops/hooks"
hooks_dst = ".git/hooks"
hooks_to_sync = ["pre-commit"] 

print("🚀 Iniciando Setup do Ambiente de Desenvolvimento...\n")

# --- 1. INSTALAÇÃO DE FERRAMENTAS (Ruff & Bandit) ---
print("📦 [1/4] Verificando e instalando dependências de qualidade...")
try:
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "ruff", "bandit"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE
    )
    print("   ✅ Ruff e Bandit instalados/atualizados com sucesso!")
except subprocess.CalledProcessError:
    print("   ❌ Erro ao instalar bibliotecas via pip. Verifique sua conexão.")
except Exception as e:
    print(f"   ⚠️  Aviso: {e}")

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
        print(f"   ✅ {hook} instalado/atualizado.")
    
    elif os.path.exists(dst):
        try:
            os.remove(dst)
            print(f"   🗑️  {hook} removido (limpeza).")
        except Exception:
            pass

# --- 3. CONFIGURAÇÃO DO GIT (Windows/CRLF) ---
print("\n⚙️  [3/4] Ajustando configurações do Git...")
try:
    subprocess.run(["git", "config", "core.safecrlf", "false"], check=True)
    subprocess.run(["git", "config", "core.autocrlf", "input"], check=True)
    print("   ✅ Git configurado para ignorar avisos de quebra de linha (CRLF)!")
except Exception:
    print("   ⚠️  Não foi possível ajustar config do git (pode ignorar).")

# --- 4. CONFIGURAÇÃO DO ALIAS 'WORKER' (PowerShell) ---
print("\n⌨️  [4/4] Configurando atalho 'worker' no PowerShell...")

# Script PowerShell que será injetado
ps_script = r"""
$ProfilePath = $PROFILE
# 1. Cria o perfil se não existir
if (!(Test-Path $ProfilePath)) { 
    New-Item -Type File -Path $ProfilePath -Force | Out-Null 
}

# --- CORREÇÃO AQUI: Filtro 'name=worker' (sem hífen) ---
# Isso garante que pegue 'prefect-worker', 'worker-blue' e 'worker-green'
$FuncCode = 'function worker { $id = docker ps --filter "name=worker" --format "{{.Names}}" | Select-Object -First 1; if ($id) { Write-Host "🚀 Entrando em: $id" -ForegroundColor Cyan; docker exec -it $id /bin/bash } else { Write-Host "❌ Nenhum worker rodando." -ForegroundColor Red } }'

# 3. Lê o arquivo atual
$CurrentContent = Get-Content -Path $ProfilePath -Raw -ErrorAction SilentlyContinue

# 4. Remove versão antiga da função (se existir) para atualizar
if ($CurrentContent -match "function worker") {
    $CurrentContent = $CurrentContent -replace 'function worker \{.*?\}', ''
    Set-Content -Path $ProfilePath -Value $CurrentContent
}

# 5. Adiciona a nova versão
Add-Content -Path $ProfilePath -Value "`n$FuncCode"
Write-Host "   ✅ Alias 'worker' atualizado no perfil: $ProfilePath"
"""

try:
    # Executa o bloco acima invocando o PowerShell
    subprocess.run(["powershell", "-Command", ps_script], check=True)
except FileNotFoundError:
    print("   ⚠️  PowerShell não encontrado (você está no Linux/Mac?). Pulei essa etapa.")
except Exception as e:
    print(f"   ⚠️  Erro ao configurar alias: {e}")

print("\n✨ Setup finalizado! Seu ambiente está blindado. 🛡️")
print("👉 Dica: Reinicie seu terminal para o comando 'worker' funcionar.")