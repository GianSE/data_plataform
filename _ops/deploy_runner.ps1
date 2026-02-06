# Arquivo: _ops/deploy_runner.ps1
# Versão corrigida com caminhos específicos para .251 e .252

Write-Host "[AGENT] Iniciando Deploy Local na maquina: $env:COMPUTERNAME" -ForegroundColor Cyan

# 1. Garante que estamos na pasta certa
Set-Location $PSScriptRoot\..
$ProjectRoot = Get-Location

# 2. Identificação do Python por Servidor
$PythonExe = "python" # Valor padrão

if ($env:COMPUTERNAME -eq "APFD23001") {
    # Caminho do Servidor .252 (Python 3.13)
    $PythonExe = "C:\Users\apf.ti02.APFD23001\AppData\Local\Programs\Python\Python313\python.exe"
    Write-Host "   [INFO] Ambiente .252 detectado." -ForegroundColor Gray
} 
elseif ($env:COMPUTERNAME -eq "APFD23003") {
    # Caminho do Servidor .251 (Python 3.12)
    $PythonExe = "C:\Users\apf.ti02\AppData\Local\Programs\Python\Python312\python.exe"
    Write-Host "   [INFO] Ambiente .251 detectado." -ForegroundColor Gray
}

# 3. Configuração da Pasta Segura e Restauração de Ambientes
$SafeEnvFolder = "C:\actions-runner\_work\data_plataform\envs_data_plataform"

Write-Host "`n[AUTH] Restaurando ficheiros de configuracao de $SafeEnvFolder..." -ForegroundColor Yellow

if (Test-Path $SafeEnvFolder) {
    if (Test-Path "$SafeEnvFolder\.env") {
        $DestEnv = Join-Path $ProjectRoot "tasks_python\_settings\.env"
        Copy-Item -Path "$SafeEnvFolder\.env" -Destination $DestEnv -Force
        Write-Host "   OK: .env restaurado." -ForegroundColor Green
    }
    if (Test-Path "$SafeEnvFolder\.env.worker") {
        $DestWorker = Join-Path $ProjectRoot "prefect-worker\.env.worker"
        Copy-Item -Path "$SafeEnvFolder\.env.worker" -Destination $DestWorker -Force
        Write-Host "   OK: .env.worker restaurado." -ForegroundColor Green
    }
}

# 4. Rebuild da Infraestrutura (Docker Blue/Green)
Write-Host "`n[INFRA] Verificando Containers e Imagens..." -ForegroundColor Yellow
# O operador '&' força o Windows a usar o caminho absoluto definido acima
& $PythonExe .\_ops\rebuild_worker.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "--- ERRO: Falha na infraestrutura Docker." -ForegroundColor Red
    exit 1
}

# 5. Atualização dos Flows no Prefect
Write-Host "`n[FLOWS] Registrando deployments..." -ForegroundColor Yellow
& $PythonExe .\_ops\rebuild_deployments.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "--- ERRO: Falha ao registrar flows." -ForegroundColor Red
    exit 1
}

Write-Host "`n[SUCESSO] Deploy finalizado em $env:COMPUTERNAME!" -ForegroundColor Green