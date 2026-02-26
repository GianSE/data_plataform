# Arquivo: _ops/deploy_dev.ps1
# Script para execucao simplificada no notebook local

Write-Host "[DEV] Iniciando deploy local..." -ForegroundColor Cyan

# 1. Garante que estamos na raiz do projeto
Set-Location $PSScriptRoot\..\..

# 2. Rebuild da Infraestrutura (Docker)
Write-Host "`n[1/2] Executando rebuild_worker.py..." -ForegroundColor Yellow
python .\_ops\deploy\rebuild_worker.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "--- ERRO no rebuild do worker." -ForegroundColor Red
    exit 1
}

# 3. Registro de Deployments (Prefect)
Write-Host "`n[2/2] Executando rebuild_deployments.py..." -ForegroundColor Yellow
python .\_ops\deploy\rebuild_deployments.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "--- ERRO no registro de deployments." -ForegroundColor Red
    exit 1
}

Write-Host "`n[SUCESSO] Scripts executados com exito!" -ForegroundColor Green