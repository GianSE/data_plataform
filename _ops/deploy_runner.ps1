# 1. Identificação Inicial
Write-Host "[AGENT] Iniciando Deploy Local na maquina: $env:COMPUTERNAME" -ForegroundColor Cyan
Set-Location $PSScriptRoot\..

# 2. Configuração do Caminho de Metadados (Persistente fora da pasta do Git)
$MetadataPath = "C:\deploy_metadata\state.json"
if (!(Test-Path "C:\deploy_metadata")) { New-Item -ItemType Directory -Force -Path "C:\deploy_metadata" }

# 3. Identificação do Python por Hostname Real
$PythonExe = "python"
# Note: Usamos ALT252 pois foi o que o seu terminal retornou
if ($env:COMPUTERNAME -eq "ALT252") {
    $PythonExe = "C:\Users\apf.ti02.APFD23001\AppData\Local\Programs\Python\Python313\python.exe"
    Write-Host "   [INFO] Servidor .252 (ALT252) detectado." -ForegroundColor Gray
} 
elseif ($env:COMPUTERNAME -eq "APFD23003") {
    $PythonExe = "C:\Users\apf.ti02\AppData\Local\Programs\Python\Python312\python.exe"
    Write-Host "   [INFO] Servidor .251 (APFD23003) detectado." -ForegroundColor Gray
}

# 4. Checagem de Cache (Hash do requirements.txt)
$CurrentHash = (Get-FileHash "prefect-worker/requirements.txt").Hash
$SkipPip = $false

if (Test-Path $MetadataPath) {
    $State = Get-Content $MetadataPath | ConvertFrom-Json
    if ($State.requirements_hash -eq $CurrentHash) { 
        $SkipPip = $true 
        $env:SKIP_PIP_INSTALL = "true" # Passa uma flag para o Python saber
    }
}

# 5. Execução do Deploy
if ($SkipPip) {
    Write-Host "`n[CACHE] Dependencias inalteradas. Pulando instalacao pesada." -ForegroundColor Gray
} else {
    Write-Host "`n[INSTALL] Mudanca detectada no requirements.txt!" -ForegroundColor Yellow
}

# Roda os scripts de infra e flows
Write-Host "[INFRA] Verificando Containers e Imagens..." -ForegroundColor Yellow
& $PythonExe .\_ops\rebuild_worker.py

Write-Host "`n[FLOWS] Registrando deployments..." -ForegroundColor Yellow
& $PythonExe .\_ops\rebuild_deployments.py

# 6. Atualização do Estado (Apenas se o deploy teve sucesso)
if ($LASTEXITCODE -eq 0) {
    $NewState = @{ requirements_hash = $CurrentHash } | ConvertTo-Json
    $NewState | Out-File -FilePath $MetadataPath -Encoding utf8
    Write-Host "`n[SUCESSO] Deploy finalizado e cache atualizado!" -ForegroundColor Green
} else {
    Write-Host "`n[ERRO] O deploy falhou. O cache nao foi atualizado." -ForegroundColor Red
}