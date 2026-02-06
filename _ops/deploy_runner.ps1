# Arquivo: _ops/deploy_local.ps1
# Este script roda LOCALMENTE dentro de cada servidor (chamado pelo GitHub Runner)

Write-Host "🤖 [AGENT] Iniciando Deploy Local na maquina: $env:COMPUTERNAME" -ForegroundColor Cyan

# 1. Garante que estamos na pasta certa (Raiz do projeto)
Set-Location $PSScriptRoot\..

# 2. Rebuild da Infraestrutura (Docker Blue/Green)
Write-Host "`n🏗️ [INFRA] Verificando Containers e Imagens..." -ForegroundColor Yellow
python .\_ops\rebuild_worker.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ [ERRO] Falha na infraestrutura Docker." -ForegroundColor Red
    exit 1
}

# 3. Atualização dos Flows no Prefect
Write-Host "`n🌊 [FLOWS] Registrando deployments..." -ForegroundColor Yellow
python .\_ops\rebumasild_deployments.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ [ERRO] Falha ao registrar flows." -ForegroundColor Red
    exit 1
}

Write-Host "`n✅ [SUCESSO] Deploy finalizado em $env:COMPUTERNAME!" -ForegroundColor Green