# -----------------------------------------------------------------
# DEPLOY MASTER: GIT PULL + INFRA (REBUILD) + FLOWS (DEPLOY)
# -----------------------------------------------------------------

# --- 1. CONFIGURAÇÃO DOS SERVIDORES ---
$ListaServidores = @(
    @{
        Nome   = "Servidor Principal (.251)"
        IP     = "192.168.21.251"
        User   = "altaneiro.ti02"
        Path   = "C:\App\Docker\data_plataform"
    },
    @{
        Nome   = "Servidor Secundário (.252)"
        IP     = "192.168.21.252"
        User   = "altaneiro.ti02"
        Path   = "C:\App\Docker\data_plataform"
    }
)

Write-Host "[INIT] Iniciando DEPLOY COMPLETO em Massa..." -ForegroundColor Cyan

# --- 2. LOOP DE EXECUÇÃO ---
foreach ($Server in $ListaServidores) {
    Write-Host "`n------------------------------------------------------"
    Write-Host "[CONN] Conectando em: $($Server.Nome) ($($Server.IP))"
    Write-Host "------------------------------------------------------" -ForegroundColor Yellow
    
    # 2.1 Pede a senha
    try {
        $msg = "Digite a senha para $($Server.User) em $($Server.IP)"
        $cred = Get-Credential -UserName $Server.User -Message $msg
    }
    catch {
        Write-Host "[SKIP] Cancelado pelo usuario." -ForegroundColor Red
        continue
    }

    # 2.2 Executa Comandos no Servidor Remoto
    Invoke-Command -ComputerName $Server.IP -Credential $cred -ScriptBlock {
        $ErrorActionPreference = "Stop"
        $Path = $using:Server.Path
        $ServerName = $env:COMPUTERNAME
        
        Write-Host "[$ServerName] [DIR] Acessando pasta do projeto..."
        
        if (-not (Test-Path $Path)) {
            Write-Host "[$ServerName] [ERRO] Pasta '$Path' nao encontrada!" -ForegroundColor Red
            return
        }

        Set-Location $Path

        # --- ETAPA 1: GIT PULL ---
        Write-Host "`n[$ServerName] [GIT] [1/3] Executando Git Pull..." -ForegroundColor Cyan
        $gitOut = git pull origin main 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[$ServerName] [ERRO] Falha no Git Pull. Abortando." -ForegroundColor Red
            $gitOut | ForEach-Object { Write-Host "   $_" }
            return
        }
        Write-Host "[$ServerName] [OK] Codigo atualizado." -ForegroundColor Green

        # --- ETAPA 2: REBUILD WORKER ---
        Write-Host "`n[$ServerName] [INFRA] [2/3] Verificando Docker (Rebuild Worker)..." -ForegroundColor Cyan
        
        # Executa o script Python e exibe o output em tempo real
        # Usamos cmd /c para garantir que o stdout seja capturado corretamente no Invoke-Command
        cmd /c "python .\_ops\rebuild_worker.py"
        
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[$ServerName] [ERRO] Falha critica na Infraestrutura. Deploy abortado." -ForegroundColor Red
            return
        }
        Write-Host "[$ServerName] [OK] Infraestrutura validada." -ForegroundColor Green

        # --- ETAPA 3: DEPLOY ALL ---
        Write-Host "`n[$ServerName] [FLOWS] [3/3] Registrando Flows (Deploy All)..." -ForegroundColor Cyan
        
        cmd /c "python .\_ops\deploy_all.py"
        
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[$ServerName] [ERRO] Falha ao registrar flows." -ForegroundColor Red
        } else {
            Write-Host "[$ServerName] [OK] Deploy finalizado com SUCESSO!" -ForegroundColor Green
        }
    }
}

Write-Host "`n[FIM] Operacao finalizada em todos os servidores." -ForegroundColor Green
Start-Sleep -Seconds 5