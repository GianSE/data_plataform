# Arquivo: _ops/deploy_runner.ps1
# Versão corrigida para compatibilidade total com Windows PowerShell 5.1

Write-Host "[AGENT] Iniciando Deploy Local na maquina: $env:COMPUTERNAME" -ForegroundColor Cyan

# 1. Garante que estamos na pasta certa
Set-Location $PSScriptRoot\..
$ProjectRoot = Get-Location

# 2. Configuração da Pasta Segura
$SafeEnvFolder = "C:\actions-runner\_work\data_plataform\envs_data_plataform"

Write-Host "`n[AUTH] Restaurando ficheiros de configuracao de $SafeEnvFolder..." -ForegroundColor Yellow

if (Test-Path $SafeEnvFolder) {
    # Copia o .env
    if (Test-Path "$SafeEnvFolder\.env") {
        $DestEnv = Join-Path $ProjectRoot "tasks_python\_settings\.env"
        Copy-Item -Path "$SafeEnvFolder\.env" -Destination $DestEnv -Force
        Write-Host "   OK: .env restaurado em tasks_python/_settings/." -ForegroundColor Green
    }

    # Copia o .env.worker
    if (Test-Path "$SafeEnvFolder\.env.worker") {
        $DestWorker = Join-Path $ProjectRoot "prefect-worker\.env.worker"
        Copy-Item -Path "$SafeEnvFolder\.env.worker" -Destination $DestWorker -Force
        Write-Host "   OK: .env.worker restaurado em prefect-worker/." -ForegroundColor Green
    }
} else {
    Write-Host "   ERRO CRITICO: Pasta segura $SafeEnvFolder nao encontrada!" -ForegroundColor Red
    exit 1
}

# 3. Rebuild da Infraestrutura (Docker Blue/Green)
Write-Host "`n[INFRA] Verificando Containers e Imagens..." -ForegroundColor Yellow
# Usando chamada direta para evitar problemas de PATH
python .\_ops\rebuild_worker.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "--- ERRO: Falha na infraestrutura Docker." -ForegroundColor Red
    exit 1
}

# 4. Atualização dos Flows no Prefect
Write-Host "`n[FLOWS] Registrando deployments..." -ForegroundColor Yellow
python .\_ops\rebuild_deployments.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "--- ERRO: Falha ao registrar flows." -ForegroundColor Red
    exit 1
}

Write-Host "`n[SUCESSO] Deploy finalizado em $env:COMPUTERNAME!" -ForegroundColor Green