# Arquivo: _ops/deploy_runner.ps1
# Versao com caminhos especificos para os servidores .251 e .252

Write-Host "[AGENT] Iniciando Deploy Local na maquina: $env:COMPUTERNAME" -ForegroundColor Cyan

# 1. Garante que estamos na pasta certa
Set-Location $PSScriptRoot\..
$ProjectRoot = Get-Location

# 2. Define o caminho do Python baseado no servidor
$PythonExe = "python" # Padrao caso esteja no PATH

if ($env:COMPUTERNAME -like "*23001*") { 
    # Servidor .252
    $PythonExe = "C:\Users\apf.ti02.APFD23001\AppData\Local\Programs\Python\Python313\python.exe"
    Write-Host "   [INFO] Detectado Servidor .252 - Usando Python 3.13" -ForegroundColor Gray
} 
elseif ($env:COMPUTERNAME -like "*23003*") {
    # Servidor .251
    $PythonExe = "C:\Users\apf.ti02\AppData\Local\Programs\Python\Python312\python.exe"
    Write-Host "   [INFO] Detectado Servidor .251 - Usando Python 3.12" -ForegroundColor Gray
}

# Valida se o executavel realmente existe no caminho informado
if (!(Test-Path $PythonExe)) {
    Write-Host "   [ERRO] Executavel do Python nao encontrado em: $PythonExe" -ForegroundColor Red
    exit 1
}

# 3. Restaurar Ficheiros de Configuracao da Pasta Segura
$SafeEnvFolder = "C:\actions-runner\_work\data_plataform\envs_data_plataform"
Write-Host "`n[AUTH] Restaurando ficheiros de configuracao..." -ForegroundColor Yellow

if (Test-Path $SafeEnvFolder) {
    # Copia o .env para tasks_python/_settings/
    if (Test-Path "$SafeEnvFolder\.env") {
        $DestEnv = Join-Path $ProjectRoot "tasks_python\_settings\.env"
        Copy-Item -Path "$SafeEnvFolder\.env" -Destination $DestEnv -Force
        Write-Host "   OK: .env restaurado." -ForegroundColor Green
    }
    # Copia o .env.worker para prefect-worker/
    if (Test-Path "$SafeEnvFolder\.env.worker") {
        $DestWorker = Join-Path $ProjectRoot "prefect-worker\.env.worker"
        Copy-Item -Path "$SafeEnvFolder\.env.worker" -Destination $DestWorker -Force
        Write-Host "   OK: .env.worker restaurado." -ForegroundColor Green
    }
} else {
    Write-Host "   ERRO: Pasta segura nao encontrada em $SafeEnvFolder" -ForegroundColor Red
    exit 1
}

# 4. Rebuild da Infraestrutura (Docker Blue/Green)
Write-Host "`n[INFRA] Verificando Containers e Imagens..." -ForegroundColor Yellow
& $PythonExe .\_ops\rebuild_worker.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "--- ERRO: Falha na infraestrutura Docker." -ForegroundColor Red
    exit 1
}

# 5. Atualizacao dos Flows no Prefect
Write-Host "`n[FLOWS] Registrando deployments..." -ForegroundColor Yellow
& $PythonExe .\_ops\rebuild_deployments.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "--- ERRO: Falha ao registrar flows." -ForegroundColor Red
    exit 1
}

Write-Host "`n[SUCESSO] Deploy finalizado em $env:COMPUTERNAME!" -ForegroundColor Green