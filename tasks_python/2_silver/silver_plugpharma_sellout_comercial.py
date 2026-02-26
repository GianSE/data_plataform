import duckdb
import mariadb
import os
import sys
import time
import threading
from datetime import datetime
from tasks_python._utils.monitor_mariadb import DBMonitor
from _settings.config import DB_CONFIG, DUCKDB_SECRET_SQL, setup_minio_env, get_temp_csv_caminho

# Para enxergar um diretório acima
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

# 1. Configura o ambiente (MinIO) automaticamente
setup_minio_env()

# Definição de Caminhos
CSV_PATH = get_temp_csv_caminho("carga_direta_silver.csv")
S3_BRONZE_SOURCE = "s3://bronze/bronze_plugpharma_vendas/**/*.parquet"

# --- FUNÇÃO DO MONITOR VISUAL ---
def monitorar_crescimento_csv(stop_event):
    """Fica olhando o arquivo CSV crescer enquanto o DuckDB trabalha"""
    print("\n👀 Monitor de disco iniciado...")
    while not stop_event.is_set():
        if os.path.exists(CSV_PATH):
            try:
                size_mb = os.path.getsize(CSV_PATH) / (1024 * 1024)
                print(f"\r🦆 DuckDB trabalhando... CSV Gerado: {size_mb:.2f} MB", end="")
            except: pass
        time.sleep(5)

# --- NOVA FUNÇÃO SUBSTITUINDO O POLARS ---
def duckdb_to_csv():
    print(f"📂 [1/3] DuckDB: Agregando BRONZE para CSV: {CSV_PATH}")
    start_time = time.time()
    
    if os.path.exists(CSV_PATH): os.remove(CSV_PATH)

    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(DUCKDB_SECRET_SQL)
        
        # --- CONFIGURAÇÕES DE ESTABILIDADE E RECURSOS ---
        con.execute("SET threads=2;") 
        con.execute("SET memory_limit='2GB';")
        con.execute("SET http_keep_alive=false;")     # Essencial para evitar o erro de cast
        con.execute("SET max_expression_depth=250;")  # Proteção extra para o container

        
        # Calcula o ano e mês atual
        ano_atual = datetime.now().year
        mes_atual = datetime.now().month
        
        # Calcula o ano e mês anterior (lidando com a virada de ano em janeiro)
        ano_anterior = ano_atual if mes_atual > 1 else ano_atual - 1
        mes_anterior = mes_atual - 1 if mes_atual > 1 else 12

        # A query agora filtra EXATAMENTE as partições recentes
        query = f"""
        COPY (
            SELECT 
                cnpj_loja, 
                codigo_interno_produto, 
                COALESCE(SUM(qtd_de_produtos), 0) AS qtd_total_vendida, 
                COALESCE(SUM(valor_liquido_total), 0.0) AS valor_total_liquido,
                make_date(ano_hive, mes_hive, 1) AS data_venda_mes,
                strftime(now(), '%Y-%m-%d %H:%M:%S') AS data_atualizacao
            FROM read_parquet(
                '{S3_BRONZE_SOURCE}', 
                hive_partitioning=true, 
                hive_types={{'ano_hive': 'INTEGER', 'mes_hive': 'INTEGER'}}
            )
            WHERE cnpj_loja IS NOT NULL 
              AND codigo_interno_produto IS NOT NULL 
              AND (
                  (ano_hive = {ano_atual} AND mes_hive = {mes_atual})
                  OR 
                  (ano_hive = {ano_anterior} AND mes_hive = {mes_anterior})
              )
            GROUP BY cnpj_loja, codigo_interno_produto, ano_hive, mes_hive
        ) TO '{CSV_PATH}' (FORMAT CSV, DELIMITER ';', HEADER FALSE, NULL '\\N');
        """
        
        stop_monitor = threading.Event()
        t = threading.Thread(target=monitorar_crescimento_csv, args=(stop_monitor,))
        t.start()
        
        print("🚀 Iniciando processamento com DuckDB (Out-of-Core)...")
        con.execute(query)
        
        stop_monitor.set()
        t.join()
        print(f"\n✅ [DuckDB] Agregação concluída em: {time.time() - start_time:.2f}s")

    except Exception as e:
        print(f"\n❌ Erro DuckDB: {e}")
        try: stop_monitor.set() 
        except: pass
        sys.exit(1)
    finally:
        con.close()

def csv_to_mariadb():
    if not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) == 0:
        print("⚠️ CSV vazio. Pulando MariaDB.")
        return

    tamanho = os.path.getsize(CSV_PATH)
    print(f"\n🐬 [2/3] Iniciando carga INCREMENTAL no MariaDB: {tamanho / 1e6:.2f} MB")
    
    conn_maria = None
    try:
        conn_maria = mariadb.connect(**DB_CONFIG)
        cursor = conn_maria.cursor()
        table_main = "silver_plugpharma_sellout_comercial"

        cursor.execute("SET unique_checks=0")
        cursor.execute("SET foreign_key_checks=0")
        cursor.execute("SET bulk_insert_buffer_size=268435456;") 
        conn_maria.autocommit = False

        monitor = DBMonitor(DB_CONFIG)
        monitor.start(table_name=table_main, total_bytes_csv=tamanho)

        # O segredo está aqui: REPLACE INTO TABLE
        sql_load = f"""
            LOAD DATA LOCAL INFILE '{CSV_PATH}'
            REPLACE INTO TABLE {table_main}
            FIELDS TERMINATED BY ';' LINES TERMINATED BY '\\n'
            (cnpj_loja, codigo_interno_produto, qtd_total_vendida, valor_total_liquido, data_venda_mes, data_atualizacao)
        """
        cursor.execute(sql_load)
        
        conn_maria.commit()
        monitor.stop()
        
        conn_maria.autocommit = True
        cursor.execute("SET unique_checks=1")
        cursor.execute("SET foreign_key_checks=1")

        print(f"✅ Carga Incremental MariaDB finalizada!")

    except Exception as e:
        print(f"❌ Erro MariaDB: {e}")
        if conn_maria:
            try: conn_maria.rollback() 
            except: pass
        sys.exit(1)
    finally:
        if conn_maria: conn_maria.close()

def limpar_temp():
    if os.path.exists(CSV_PATH):
        try: os.remove(CSV_PATH)
        except: pass

if __name__ == "__main__":
    duckdb_to_csv()
    csv_to_mariadb()
    limpar_temp()