# Arquivo: _ops/deploy_runner.ps1
Write-Host "[AGENT] Iniciando Deploy Local na maquina: $env:COMPUTERNAME" -ForegroundColor Cyan

Set-Location $PSScriptRoot\..
# 2. Identificação do Python por Servidor
$PythonExe = "python" # Valor padrão

if ($env:COMPUTERNAME -eq "APFD23001") {
    # Caminho do Servidor .252 (Python 3.13)
    $PythonExe = "C:\Users\apf.ti02.APFD23001\AppData\Local\Programs\Python\Python313\python.exe"
    Write-Host "   [INFO] Servidor .252 detectado." -ForegroundColor Gray
} 
elseif ($env:COMPUTERNAME -eq "APFD23003") {
    # Caminho do Servidor .251 (Python 3.12)
    $PythonExe = "C:\Users\apf.ti02\AppData\Local\Programs\Python\Python312\python.exe"
    Write-Host "   [INFO] Servidor .251 detectado." -ForegroundColor Gray
}

# 1. Rebuild da Infraestrutura (Docker)
Write-Host "`n[INFRA] Verificando Containers e Imagens..." -ForegroundColor Yellow
& $PythonExe .\_ops\rebuild_worker.py

# 2. Registro de Deployments (Prefect)
Write-Host "`n[FLOWS] Registrando deployments..." -ForegroundColor Yellow
& $PythonExe .\_ops\rebuild_deployments.py

Write-Host "`n[SUCESSO] Deploy finalizado em $env:COMPUTERNAME!" -ForegroundColor Green