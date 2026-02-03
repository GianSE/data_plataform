# Mesma versão do prefect-server (travada)
FROM prefecthq/prefect:3.6.13-python3.12

# Diretório de trabalho
WORKDIR /app

# Copia requirements
COPY prefect-worker/requirements.txt .

# Instala dependências
RUN pip install --no-cache-dir -r requirements.txt

# Comando que sobe o worker automaticamente
CMD ["prefect", "worker", "start", "--pool", "process-pool", "--overwrite"]
