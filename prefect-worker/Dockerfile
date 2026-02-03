# Mesma versao prefect-server 3.6.13 no docker-compose.yml (Travado via SHA256)
FROM prefecthq/prefect:3.6.13-python3.12

# Copia as pastas de scripts para dentro do container
WORKDIR /app

# Instala dependencias do python no container
COPY prefect-worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
