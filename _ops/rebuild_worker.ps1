# -----------------------------------------------------------------
# DEPLOY SIMPLIFICADO: GIT PULL + BLUE-GREEN (Sem Registro no Prefect)
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

Write-Host "🚀 Iniciando Deploy de Infraestrutura (Blue-Green)..." -ForegroundColor Cyan

# --- 2. LOOP DE EXECUÇÃO ---
foreach ($Server in $ListaServidores) {
    Write-Host "`n======================================================"
    Write-Host "📡 Conectando em: $($Server.Nome) ($($Server.IP))"
    Write-Host "======================================================" -ForegroundColor Yellow
    
    # Pede a senha específica
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
        
        # --- A. GIT PULL ---
        Write-Host "[$ServerName] 📂 Acessando: $ProjectDir"
        if (-not (Test-Path $ProjectDir)) { throw "Pasta não encontrada!" }
        Set-Location $ProjectDir

        Write-Host "[$ServerName] ⬇️  Git Pull..."
        $gitOut = git pull origin main 2>&1
        if ($LASTEXITCODE -ne 0) { throw "Erro Git: $gitOut" }
        Write-Host "[$ServerName] ✅ Código atualizado." -ForegroundColor Green

        # --- B. BLUE-GREEN ---
        $DockerDir = "prefect-worker"
        if (Test-Path $DockerDir) { Set-Location $DockerDir }
        
        # 1. Quem está rodando?
        $blueRunning = docker ps --filter "name=worker-blue" -q
        if ($blueRunning) {
            $NewColor = "green"; $CurrentColor = "blue"
            $OldContainerName = (docker ps --filter "name=worker-blue" --format "{{.Names}}")
        } else {
            $NewColor = "blue"; $CurrentColor = "green"
            $greenRunning = docker ps --filter "name=worker-green" -q
            if ($greenRunning) { $OldContainerName = (docker ps --filter "name=worker-green" --format "{{.Names}}") }
        }

        # 2. Sobe o Novo
        Write-Host "[$ServerName] 🏗️  Subindo $NewColor..."
        docker compose -p "worker-$NewColor" up -d --build --remove-orphans
        
        Start-Sleep -Seconds 5
        if (-not (docker ps --filter "name=worker-$NewColor" -q)) { throw "Novo worker falhou ao subir." }
        Write-Host "[$ServerName] ✅ $NewColor Online." -ForegroundColor Green

        # 3. Mata o Velho
        if ($OldContainerName) {
            Write-Host "[$ServerName] 🛑 Parando $CurrentColor..."
            docker kill --signal=SIGTERM $OldContainerName
            
            # Drenagem rápida
            do {
                $stillRunning = docker ps --filter "name=$OldContainerName" -q
                if ($stillRunning) { Start-Sleep -Seconds 2 }
            } while ($stillRunning)
            
            docker compose -p "worker-$CurrentColor" down
            Write-Host "[$ServerName] 💀 $CurrentColor Removido."
        }
    }
}

Write-Host "`n✨ Tudo pronto!" -ForegroundColor Green
Start-Sleep -Seconds 3