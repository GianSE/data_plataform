# 💊 Drogamais Data Platform

Plataforma de Engenharia de Dados da rede Drogamais construída sobre **Prefect 3**, **DuckDB**, **MinIO** e **MariaDB**.

Toda a plataforma é orientada a **zero intervenção manual**: um `git push` na branch `main` é suficiente para validar, empacotar e re-registrar automaticamente todos os flows alterados nos dois servidores de produção.

---

## 🏗️ Arquitetura Geral

```
GitHub (main)
    │
    │ push
    ▼
GitHub Actions (CD)
    │
    ├─── Self-Hosted Runner → servidor .251 (APFD23003)
    └─── Self-Hosted Runner → servidor .252 (ALT252)
              │
              │ deploy_runner.ps1
              ▼
    ┌─────────────────────────────┐
    │  rebuild_worker.py          │  → Reconstrói a imagem Docker se
    │  (hash Dockerfile + reqs)   │    requirements.txt ou Dockerfile mudaram
    └─────────────────────────────┘
              │
    ┌─────────────────────────────┐
    │  rebuild_deployments.py     │  → Varre flows_prefect/, calcula MD5 de
    │  (hash por flow file)        │    cada flow_*.py e re-registra só os
    └─────────────────────────────┘    que mudaram no Prefect
              │
              ▼
    ┌──────────────────────────────────────────┐
    │  Prefect Worker (Docker)                 │
    │  process-pool  |  max 3 jobs paralelos   │
    │  API: 192.168.21.251:4200                │
    └──────────────────────────────────────────┘
```

### Stack Tecnológica

| Camada | Tecnologia |
|---|---|
| Orquestração | Prefect 3.6.13 |
| Motor de consulta | DuckDB 1.1.0 |
| Data Lake | MinIO (S3-compatible) |
| Data Warehouse | MariaDB |
| Processamento | Pandas 3.0 / Polars 1.38 |
| Infraestrutura | Docker + docker-compose |
| CI/CD | GitHub Actions + Self-Hosted Runners |

---

## 📐 Arquitetura de Dados (Medallion)

Os dados fluem por quatro camadas numeradas, cada uma com seu diretório espelhado em `flows_prefect/` e `tasks_python/`:

```
0_raw    → Extração bruta da fonte (arquivos, APIs, sistemas legados)
1_bronze → Limpeza mínima, preservação do dado original no MinIO
2_silver → Enriquecimento, joins e consolidações no DuckDB
3_gold   → Modelos analíticos prontos para consumo no MariaDB
```

Cada camada em `flows_prefect/` contém apenas **wrappers de orquestração** (agendamento, retry, logging via Prefect). A lógica de transformação real fica em `tasks_python/` e é invocada pelo wrapper via `python_task`, que captura o stdout/stderr do script filho e exibe no Prefect UI em tempo real.

---

## 📂 Estrutura do Projeto

```text
data_plataform/
│
├── flows_prefect/           # Orquestração (Prefect flows / wrappers)
│   ├── _shared/
│   │   ├── wrappers.py      # python_task, standard_flow (retries, logging)
│   │   └── deployment.py    # gerenciar_run, rodar_deployment
│   ├── 0_raw/
│   ├── 1_bronze/
│   ├── 2_silver/
│   └── 3_gold/
│
├── tasks_python/            # Lógica ETL pura (sem dependência do Prefect)
│   ├── _settings/
│   │   ├── config.py        # Conexões MariaDB, MinIO, DuckDB secret SQL
│   │   └── .env             # Credenciais (não versionado)
│   ├── _utils/
│   │   └── monitor_mariadb.py  # Monitor de carga em tempo real (threading)
│   ├── 0_raw/
│   ├── 1_bronze/
│   ├── 2_silver/
│   └── 3_gold/
│
└── _ops/                    # Infraestrutura e CI/CD
    ├── deploy/
    │   ├── setup_dev.py             # Setup local: hooks + git config + alias 'worker'
    │   ├── deploy_dev.ps1           # Deploy simplificado na máquina local
    │   ├── deploy_runner.ps1        # Deploy via CI (Self-Hosted Runner)
    │   ├── deploy_full.ps1          # Deploy remoto em massa (WinRM → 2 servidores)
    │   ├── rebuild_worker.py        # Rebuild inteligente do Docker (hash MD5)
    │   ├── rebuild_deployments.py   # Re-registro inteligente de flows (hash MD5)
    │   └── check_imports.py         # Validador de sintaxe Python
    ├── hooks/
    │   └── pre-commit               # Git hook: valida sintaxe antes do commit
    └── prefect-worker/
        ├── Dockerfile               # Imagem baseada em prefecthq/prefect:3.6.13-python3.12
        ├── docker-compose.yml       # Serviço do worker com volumes e env
        └── requirements.txt         # Dependências do ambiente de execução
```

---

## ⚙️ Como o Deploy Funciona

### Deploy Inteligente por Hash (MD5)

A plataforma **nunca re-registra tudo do zero**. Dois níveis de cache evitam trabalho desnecessário:

1. **Worker Docker** — `rebuild_worker.py` calcula o MD5 de `Dockerfile` e `requirements.txt`. A imagem só é reconstruída (e o container reiniciado com graceful drain) quando esses arquivos mudam. O estado fica persistido em `C:\deploy_metadata\`.

2. **Flows Prefect** — `rebuild_deployments.py` calcula o MD5 de cada `flow_*.py` em `flows_prefect/`. Só os flows cujos arquivos mudaram são re-registrados no Prefect. O estado fica em `/app/_metadata/.deployments_hashes.json` (volume Docker persistente).

### Pipeline CI/CD Completo

```
git commit   →  pre-commit hook valida sintaxe de todos os .py staged
git push     →  GitHub recebe o código na branch main
                │
                └─ GitHub Actions dispara cd_deploy.yml
                        │
                        ├── Runner no .251: inject secrets → deploy_runner.ps1
                        └── Runner no .252: inject secrets → deploy_runner.ps1
                                │
                                ├── rebuild_worker.py  (infra)
                                └── rebuild_deployments.py (flows)
```

Os secrets (credenciais de banco, MinIO) são armazenados no **GitHub Secrets** e injetados como arquivo `.env` durante o deploy pelo workflow, nunca sendo versionados. O nome do worker é gerado automaticamente a partir do IP da máquina (`worker-192.168.21.xxx`).

---

## 🔧 Primitivas da Plataforma

### `python_task`
Task Prefect que executa qualquer script Python como subprocesso, com streaming de stdout/stderr direto para o Prefect UI. Possui retry automático (1 tentativa, 60s de delay).

### `standard_flow`
Decorator de flow com retries configurados (2 tentativas, 60s) e nome padronizado.

### `gerenciar_run`
Função que detecta automaticamente se o flow está sendo executado para **deploy** (`python flow.py deploy`) ou **teste local** (`python flow.py`), sem alterar o código.

### `rodar_deployment`
Task usada em flows orquestradores para disparar outros deployments registrados no Prefect, aguardando sua conclusão.

---

## 🚀 Onboarding (Configuração Local)

O ambiente de desenvolvimento local **não usa venv**. Ele roda dentro de um container Docker idêntico ao de produção, garantindo paridade total entre dev e prod.

### 1. Configurar Variáveis de Ambiente

```bash
copy tasks_python\_settings\.env.example tasks_python\_settings\.env
# Edite o .env com as credenciais do MariaDB e MinIO
```

### 2. Subir o Worker Local

```bash
python _ops/deploy/rebuild_worker.py
```

Isso constrói a imagem `custom-prefect-worker` e sobe o container com as mesmas dependências, volumes e variáveis de ambiente que os servidores de produção. O código-fonte do projeto é montado em `/app` via bind mount, então qualquer alteração local já é refletida no container.

### 3. Rodar o Setup de Desenvolvimento

```bash
python _ops/deploy/setup_dev.py
```

Esse script configura três coisas de uma vez:

| Etapa | O que faz |
|---|---|
| **Git Hooks** | Copia `_ops/hooks/pre-commit` para `.git/hooks/`. Antes de cada commit, valida a sintaxe de todos os `.py` staged. |
| **Git CRLF** | Configura `core.autocrlf=input` e `core.safecrlf=false` para evitar problemas Windows ↔ Linux. |
| **Alias `worker`** | Registra uma função no `$PROFILE` do PowerShell que permite entrar no container local com um único comando. |

Após reiniciar o terminal, basta digitar:

```powershell
worker
```

Para abrir um shell bash diretamente dentro do container do worker local — o mesmo ambiente que roda em produção.

---

## 🛠️ Como Criar um Novo Flow

### 1. Script ETL (`tasks_python/`)

Crie a lógica de transformação em `tasks_python/{camada}/meu_script.py`. Sem dependência do Prefect — Python puro.

### 2. Flow Wrapper (`flows_prefect/`)

Crie `flows_prefect/{camada}/flow_meu_script.py` usando os primitivos da plataforma:

```python
from flows_prefect._shared.wrappers import python_task, standard_flow
from flows_prefect._shared.deployment import gerenciar_run

@standard_flow(name="Meu Flow")
def pipeline():
    python_task(script_name="meu_script", python_base_path="/app/tasks_python/{camada}")

if __name__ == "__main__":
    gerenciar_run(
        pipeline_flow=pipeline,
        entrypoint_name="{camada}/flow_meu_script.py:pipeline",
        deploy_name="Meu Flow",
        tags=["minha-tag"],
        cron_schedule="0 5 * * *"  # opcional
    )
```

### 3. Deploy

```bash
git add .
git commit -m "feat: new flow meu_script"
git push origin main
```

O sistema descobre e registra o flow automaticamente nos dois servidores.

---

## 🔍 Comandos Úteis

```bash
# Entrar no container local (após setup_dev.py)
worker

# Validar sintaxe manualmente
python _ops/deploy/check_imports.py

# Testar flow localmente (sem deploy)
python flows_prefect/{camada}/flow_meu_script.py

# Deploy local (rebuild worker + re-registro de flows alterados)
.\_ops\deploy\deploy_dev.ps1

# Forçar re-registro de TODOS os flows (dentro do container)
docker exec <container_id> python /app/_ops/deploy/rebuild_deployments.py --all

# Deploy remoto em massa nos dois servidores (via WinRM)
.\_ops\deploy\deploy_full.ps1
```

---

## 📜 Scripts de Deploy

| Script | Onde roda | O que faz |
|---|---|---|
| `deploy_dev.ps1` | Máquina local | Executa `rebuild_worker.py` + `rebuild_deployments.py` em sequência |
| `deploy_runner.ps1` | Self-Hosted Runner (CI) | Mesmo que o dev, mas com cache de `requirements.txt` via hash persistido em `C:\deploy_metadata\` |
| `deploy_full.ps1` | Máquina local (remoto) | Conecta nos dois servidores via WinRM, faz `git pull` + rebuild + deploy em cada um |
| `rebuild_worker.py` | Qualquer | Reconstrói a imagem Docker apenas se `Dockerfile` ou `requirements.txt` mudaram (hash MD5) |
| `rebuild_deployments.py` | Dentro do container | Varre `flows_prefect/`, re-registra no Prefect apenas os `flow_*.py` alterados (hash MD5) |
