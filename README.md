# 💊 Drogamais Data Platform

Plataforma de Engenharia de Dados baseada em Prefect, DuckDB e MariaDB.
Este projeto utiliza uma arquitetura orientada a domínios (Acode, Plugpharma, etc) orquestrada por fluxos Master..

## 📂 Estrutura do Projeto

```text
data_plataform/
├── pipelines/              # Lógica de Orquestração
│   ├── flows_master/       # Orquestradores (Regem a ordem de execução)
│   ├── flows_prefect/      # Flows individuais (Wrappers de execução)
│   └── _ops/               # Scripts de CI/CD
│
├── tasks_python/           # Lógica de Transformação (ETL puro / Library)
│   ├── settings/           # Configurações e .env
│   ├── bronze/
│   └── comercial/
│
├── prefect-worker/         # Infraestrutura Docker e worker
└── setup_hooks.py          # Script de configuração do ambiente local pre-push
```

## 🚀 Como Iniciar (Onboarding)

- Se você acabou de clonar este repositório, siga os passos abaixo para configurar seu ambiente:

## Preparar Ambiente Python

```bash
python -m venv venv
.\venv\Scripts\activate
pip install -r prefect-worker/requirements.txt
```

### 1. Configurar Variáveis de Ambiente

Crie um arquivo .env dentro de pipelines/tasks_python/settings/ baseado no exemplo:

```bash
cp tasks_python/settings/.env.example tasks_python/settings/.env
```
Edite o arquivo .env com as credenciais do MariaDB e MinIO

### 2. Instalar Git Hooks (Segurança)

- Para impedir que códigos quebrados sejam enviados ao servidor, instalamos um Pre-Push Hook. Na raiz do projeto, execute:
```bash
python setup_hooks.py
```
✅ Isso garante que, ao dar git push, o sistema rode automaticamente o check_imports.py para validar seu código.

## 🛠️ Como Desenvolver e Fazer Deploy

- Nossa arquitetura usa Dynamic Discovery. Você não precisa registrar flows manualmente.

### Passo 1: Criar o Script ETL

- Crie seu script de transformação em tasks_python/{dominio}/.

    ```Ex: tasks_python/rh/calculo_hora_extra.py```

### Passo 2: Criar o Flow (Wrapper)

- Crie um arquivo em pipelines/flows_prefect/{dominio}/ que chama o script acima.

- Use python_task para rodar o script.

- Importante: Não coloque agendamento (Cron) aqui se ele for rodado pelo Master.

### Passo 3: Atualizar o Master (Orquestração final)

- Se for um fluxo novo, adicione uma etapa no pipelines/flows_master/master_{dominio}.py usando rodar_deployment.

### Passo 4: Deploy Automático

Apenas faça o commit e push:
```bash
git add .
git commit -m "feat: Novo fluxo de RH"
git push origin main
```

🔄 O que acontece magicamente:

    O Git Hook roda localmente e verifica se você esqueceu algum import. Se falhar, o push é bloqueado.

    O código sobe para o GitHub.

    O GitHub Actions (ou o Runner no servidor) baixa o código.

    O script _ops/deploy_all.py varre as pastas, encontra seu arquivo novo e faz o Deploy/Update no Prefect automaticamente.

# 🔍 Comandos Úteis

Rodar validação manualmente:
```bash
python pipelines/_ops/check_imports.py
```

Rodar script para teste local:
```bash
python pipelines/flows_prefect/flow_{dominio}.py
```

Rodar deploy manualmente (dentro do servidor):
```bash
docker exec prefect_worker python /app/_ops/deploy_all.py
```
