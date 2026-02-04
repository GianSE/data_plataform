# -----------------------------------------------------------------
# ATUALIZAÇÃO REMOTA MULTI-SERVIDOR (Senhas Diferentes) GIT PULL
# -----------------------------------------------------------------

# --- 1. CONFIGURAÇÃO DOS SERVIDORES ---
# Adicione quantos blocos quiser aqui dentro do @(...)
$ListaServidores = @(
    @{
        Nome   = "Servidor Principal (.251)"
        IP     = "192.168.21.251"
        User   = "Administrator"
        Path   = "C:\Users\altaneiro.analista01\Desktop\plataforma\data_plataform"
    },
    @{
        Nome   = "Servidor Secundário (.37)"
        IP     = "192.168.21.37"  # Ajuste o IP completo aqui
        User   = "Admin"          # Pode ser outro usuário
        Path   = "C:\Sistemas\data_plataform" # Pode ser outro caminho
    }
)

Write-Host "🚀 Iniciando Atualização em Massa..." -ForegroundColor Cyan

# --- 2. LOOP DE EXECUÇÃO ---
foreach ($Server in $ListaServidores) {
    Write-Host "`n------------------------------------------------------"
    Write-Host "📡 Conectando em: $($Server.Nome) ($($Server.IP))"
    Write-Host "------------------------------------------------------" -ForegroundColor Yellow
    
    # 2.1 Pede a senha ESPECÍFICA deste servidor
    try {
        $msg = "Digite a senha para $($Server.User) em $($Server.IP)"
        $cred = Get-Credential -UserName $Server.User -Message $msg
    }
    catch {
        Write-Host "❌ Pulo: Cancelado pelo usuário." -ForegroundColor Red
        continue # Pula para o próximo servidor da lista
    }

    # 2.2 Executa o Git Pull Remoto
    Invoke-Command -ComputerName $Server.IP -Credential $cred -ScriptBlock {
        $ErrorActionPreference = "Stop"
        
        # Pega as variáveis que passamos lá de fora
        $Path = $using:Server.Path
        
        Write-Host "[$env:COMPUTERNAME] 📂 Acessando: $Path"
        
        if (Test-Path $Path) {
            Set-Location $Path
            
            Write-Host "[$env:COMPUTERNAME] ⬇️  Rodando Git Pull..."
            $gitResult = git pull origin main 2>&1
            
            if ($LASTEXITCODE -eq 0) {
                Write-Host "[$env:COMPUTERNAME] ✅ SUCESSO!" -ForegroundColor Green
                # Mostra apenas as linhas relevantes (limpeza visual)
                $gitResult | Where-Object { $_ -match "Updating|Fast-forward|Already up to date" } | ForEach-Object { Write-Host "   $_" }
            } else {
                Write-Host "[$env:COMPUTERNAME] ❌ ERRO NO GIT:" -ForegroundColor Red
                $gitResult | ForEach-Object { Write-Host "   $_" }
            }
        } else {
            Write-Host "[$env:COMPUTERNAME] ❌ ERRO: Pasta não encontrada!" -ForegroundColor Red
        }
    }
}

Write-Host "`n✨ Processo Finalizado em todos os servidores." -ForegroundColor Green
Start-Sleep -Seconds 5