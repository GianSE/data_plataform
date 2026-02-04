# -----------------------------------------------------------------
# SOFT DEPLOY: GIT PULL + PREFECT DEPLOY (Sem reiniciar Docker)
# -----------------------------------------------------------------

# --- 1. CONFIGURAÇÃO DOS SERVIDORES ---
$ListaServidores = @(
    @{
        Nome   = "Servidor Principal (.251)"
        IP     = "192.168.21.251"
        User   = "Administrator"
        Path   = "C:\Users\altaneiro.analista01\Desktop\plataforma\data_plataform"
    },
    @{
        Nome   = "Servidor Secundário (.37)"
        IP     = "192.168.21.37"
        User   = "Admin"
        Path   = "C:\Sistemas\data_plataform"
    }
)

Write-Host "🚀 Iniciando Soft Deploy (Git + Atualização de Flows)..." -ForegroundColor Cyan

# --- 2. LOOP DE EXECUÇÃO ---
foreach ($Server in $ListaServidores) {
    Write-Host "`n======================================================"
    Write-Host "📡 Conectando em: $($Server.Nome) ($($Server.IP))"
    Write-Host "======================================================" -ForegroundColor Yellow
    
    # Pede a senha
    try {
        $msg = "Senha para $($Server.User) em $($Server.IP)"
        $cred = Get-Credential -UserName $Server.User -Message $msg
    }
    catch {
        Write-Host "❌ Cancelado." -ForegroundColor Red
        continue
    }

    # COMANDO REMOTO
    Invoke-Command -ComputerName $Server.IP -Credential $cred -ScriptBlock {
        $ErrorActionPreference = "Stop"
        $ProjectDir = $using:Server.Path
        $ServerName = $env:COMPUTERNAME
        
        # --- PASSO A: GIT PULL ---
        Write-Host "[$ServerName] 📂 Acessando: $ProjectDir"
        if (-not (Test-Path $ProjectDir)) { throw "Pasta não encontrada!" }
        Set-Location $ProjectDir

        Write-Host "[$ServerName] ⬇️  Atualizando código (Git Pull)..."
        $gitOut = git pull origin main 2>&1
        
        # Mostra o resultado do Git (mesmo se der erro, queremos ver)
        if ($LASTEXITCODE -ne 0) { 
            Write-Host "[$ServerName] ❌ Erro no Git:" -ForegroundColor Red
            $gitOut | ForEach-Object { Write-Host "   $_" }
            # Opcional: throw "Abortando." (Se quiser parar se o git falhar)
        } else {
            Write-Host "[$ServerName] ✅ Código atualizado no disco." -ForegroundColor Green
        }

        # --- PASSO B: RODAR DEPLOY_ALL.PY ---
        # Procura qualquer container do worker rodando (Blue ou Green)
        $containerID = docker ps --filter "name=prefect-worker" -q | Select-Object -First 1
        
        if (-not $containerID) {
            Write-Host "[$ServerName] ⚠️  Aviso: Nenhum container rodando. Não é possível registrar flows." -ForegroundColor Yellow
        } else {
            Write-Host "[$ServerName] 🔄 Registrando flows no Prefect..."
            
            try {
                # Executa o script dentro do container encontrado
                $out = docker exec $containerID python /app/pipelines/_ops/deploy_all.py 2>&1
                
                # Exibe a saída do script Python
                $out | ForEach-Object { Write-Host "   $_" -ForegroundColor Gray }
                Write-Host "[$ServerName] ✅ Flows atualizados com sucesso!" -ForegroundColor Green
            }
            catch {
                Write-Host "[$ServerName] ❌ Falha ao rodar deploy_all.py: $_" -ForegroundColor Red
            }
        }
    }
}

Write-Host "`n✨ Soft Deploy Finalizado!" -ForegroundColor Green
Start-Sleep -Seconds 3