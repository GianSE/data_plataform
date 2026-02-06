# Use a versão que você preferir (3.6.13 ou a nova 3.6.16)
FROM prefecthq/prefect:3.6.13-python3.12

WORKDIR /app

# --- NOVO BLOCO: Instala dependências de sistema para o MariaDB ---
USER root
RUN apt-get update && \
    apt-get install -y libmariadb-dev gcc pkg-config && \
    rm -rf /var/lib/apt/lists/*
# -----------------------------------------------------------------

COPY prefect-worker/requirements.txt .

# Agora o pip vai conseguir compilar o mariadb
RUN pip install --no-cache-dir -r requirements.txt

# Nota: O comando CMD aqui é sobrescrito pelo seu docker-compose.yml
CMD ["prefect", "worker", "start", "--pool", "process-pool"]