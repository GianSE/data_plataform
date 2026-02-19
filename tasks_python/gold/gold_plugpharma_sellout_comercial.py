import duckdb
import mariadb
import os
import sys
import time
import threading
from datetime import datetime
from _utils.monitor import DBMonitor
# ADICIONE O DUCKDB_SECRET_SQL AQUI NO IMPORT
from _settings.config import DB_CONFIG, MINIO_CONFIG, get_temp_csv_caminho, DUCKDB_SECRET_SQL

# Definição de Caminhos
CSV_PATH = get_temp_csv_caminho("carga_direta_gold.csv")
S3_SILVER_SOURCE = "s3://silver/silver_plugpharma_vendas/**/*.parquet"

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
        time.sleep(0.5)

# --- NOVA FUNÇÃO SUBSTITUINDO O POLARS ---
def duckdb_to_csv():
    print(f"📂 [1/3] DuckDB: Agregando Silver para CSV: {CSV_PATH}")
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

        
        
        # O DuckDB faz exatamente a mesma coisa que o Polars (Group By, Soma, Filtros)
        # mas usa o disco como "Swap" caso a RAM fique cheia!
        query = f"""
        COPY (
            SELECT 
                cnpj_loja, 
                codigo_interno_produto, 
                SUM(CAST(qtd_de_produtos AS BIGINT)) AS qtd_total_vendida, 
                SUM(CAST(valor_liquido_total AS DECIMAL(15,2))) AS valor_total_liquido,
                make_date(CAST(ano_venda AS INTEGER), CAST(mes_venda AS INTEGER), 1) AS data_venda_mes,
                now() AS data_atualizacao
            FROM read_parquet('{S3_SILVER_SOURCE}', hive_partitioning=true)
            WHERE cnpj_loja IS NOT NULL 
              AND codigo_interno_produto IS NOT NULL 
              AND ano_venda >= 2022
            GROUP BY cnpj_loja, codigo_interno_produto, ano_venda, mes_venda
        ) TO '{CSV_PATH}' (FORMAT CSV, DELIMITER ';', HEADER FALSE);
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
    print(f"\n🐬 [2/3] Iniciando carga no MariaDB: {tamanho / 1e6:.2f} MB")
    
    conn_maria = None
    try:
        conn_maria = mariadb.connect(**DB_CONFIG)
        cursor = conn_maria.cursor()

        table_main = "gold_plugpharma_sellout_comercial"
        table_new = f"{table_main}_new"
        table_old = f"{table_main}_old"

        ano_atual = datetime.now().year
        ano_inicio = 2022 
        particoes_list = [f"PARTITION p{ano} VALUES LESS THAN ({ano + 1})" for ano in range(ano_inicio, ano_atual + 2)]
        particoes_list.append("PARTITION pmax VALUES LESS THAN MAXVALUE")
        sql_particoes_dinamicas = ",\n        ".join(particoes_list)

        cursor.execute(f"DROP TABLE IF EXISTS {table_old}")
        cursor.execute(f"DROP TABLE IF EXISTS {table_new}")

        cursor.execute(f"""
            CREATE TABLE {table_new} (
                cnpj_loja VARCHAR(20) NOT NULL,
                codigo_interno_produto VARCHAR(10) NOT NULL,
                qtd_total_vendida BIGINT, 
                valor_total_liquido DECIMAL(15,2),
                data_venda_mes DATE NOT NULL,
                data_atualizacao DATETIME,
                PRIMARY KEY (data_venda_mes, codigo_interno_produto, cnpj_loja),
                KEY idx_loja_data (cnpj_loja, data_venda_mes),
                KEY idx_join_produto (codigo_interno_produto)
            ) 
            ENGINE=InnoDB 
            DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci
            PARTITION BY RANGE (YEAR(data_venda_mes)) (
                {sql_particoes_dinamicas}
            );
        """)

        cursor.execute("SET unique_checks=0")
        cursor.execute("SET foreign_key_checks=0")
        cursor.execute("SET bulk_insert_buffer_size=256*1024*1024;") 
        conn_maria.autocommit = False

        monitor = DBMonitor(DB_CONFIG)
        monitor.start(table_name=table_new, total_bytes_csv=tamanho)

        sql_load = f"""
            LOAD DATA LOCAL INFILE '{CSV_PATH}'
            INTO TABLE {table_new}
            FIELDS TERMINATED BY ';' LINES TERMINATED BY '\\n'
            (cnpj_loja, codigo_interno_produto, qtd_total_vendida, valor_total_liquido, data_venda_mes, data_atualizacao)
        """
        cursor.execute(sql_load)
        
        conn_maria.commit()
        monitor.stop()
        
        conn_maria.autocommit = True
        cursor.execute("SET unique_checks=1")
        cursor.execute("SET foreign_key_checks=1")

        print("⚙️ Realizando Swap de tabelas...")
        cursor.execute(f"SHOW TABLES LIKE '{table_main}'")
        if cursor.fetchone():
            cursor.execute(f"RENAME TABLE {table_main} TO {table_old}, {table_new} TO {table_main}")
        else:
            cursor.execute(f"RENAME TABLE {table_new} TO {table_main}")
        
        cursor.execute(f"DROP TABLE IF EXISTS {table_old}")
        conn_maria.commit() 
        print(f"✅ Carga MariaDB finalizada!")

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