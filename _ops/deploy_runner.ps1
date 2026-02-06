# Arquivo: _ops/deploy_runner.ps1
# Este script roda LOCALMENTE dentro de cada servidor (chamado pelo GitHub Runner)

Write-Host "🤖 [AGENT] Iniciando Deploy Local na maquina: $env:COMPUTERNAME" -ForegroundColor Cyan

# 1. Garante que estamos na pasta certa (Raiz do projeto: ...\data_plataform\data_plataform)
Set-Location $PSScriptRoot\..
$ProjectRoot = Get-Location

# 2. Configuração da Pasta Segura e Restauração de Ambientes
$SafeEnvFolder = "C:\actions-runner\_work\data_plataform\envs_data_plataform"

Write-Host "`n🔐 [AUTH] Restaurando ficheiros de configuração de $SafeEnvFolder..." -ForegroundColor Yellow

if (Test-Path $SafeEnvFolder) {
    # Copia o .env para tasks_python/_settings/
    if (Test-Path "$SafeEnvFolder\.env") {
        $DestEnv = Join-Path $ProjectRoot "tasks_python\_settings\.env"
        Copy-Item -Path "$SafeEnvFolder\.env" -Destination $DestEnv -Force
        Write-Host "   ✅ .env restaurado com sucesso." -ForegroundColor Green
    } else {
        Write-Host "   ⚠️ Aviso: .env não encontrado na pasta segura." -ForegroundColor Red
    }

    # Copia o .env.worker para prefect-worker/
    if (Test-Path "$SafeEnvFolder\.env.worker") {
        $DestWorker = Join-Path $ProjectRoot "prefect-worker\.env.worker"
        Copy-Item -Path "$SafeEnvFolder\.env.worker" -Destination $DestWorker -Force
        Write-Host "   ✅ .env.worker restaurado com sucesso." -ForegroundColor Green
    } else {
        Write-Host "   ⚠️ Aviso: .env.worker não encontrado na pasta segura." -ForegroundColor Red
    }
} else {
    Write-Host "   ❌ ERRO CRÍTICO: Pasta segura $SafeEnvFolder não encontrada!" -ForegroundColor Red
    exit 1
}

# 3. Rebuild da Infraestrutura (Docker Blue/Green)
Write-Host "`n🏗️ [INFRA] Verificando Containers e Imagens..." -ForegroundColor Yellow
# Se o comando 'python' ainda falhar, lembre-se de usar o caminho completo do executável aqui
python .\_ops\rebuild_worker.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ [ERRO] Falha na infraestrutura Docker." -ForegroundColor Red
    exit 1
}

# 4. Atualização dos Flows no Prefect
Write-Host "`n🌊 [FLOWS] Registrando deployments..." -ForegroundColor Yellow
python .\_ops\rebuild_deployments.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ [ERRO] Falha ao registrar flows." -ForegroundColor Red
    exit 1
}

Write-Host "`n✅ [SUCESSO] Deploy finalizado em $env:COMPUTERNAME!" -ForegroundColor Green