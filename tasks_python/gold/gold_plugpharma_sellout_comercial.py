import duckdb
import mariadb
import os
import sys
import time
from datetime import datetime
from _utils.monitor import DBMonitor
from _settings.config import DB_CONFIG, DUCKDB_SECRET_SQL, setup_minio_env, get_temp_csv_caminho

setup_minio_env()
CSV_PATH = get_temp_csv_caminho("carga_direta_gold.csv")
# Varre todas as partições de uma vez só usando o wildcard recursivo **
S3_SILVER_SOURCE = "s3://silver/silver_plugpharma_vendas/**/*.parquet"

def duckdb_to_csv():
    print(f"📂 [1/3] Agregando TODO o período Silver para CSV: {CSV_PATH}")
    start_duck = time.time()
    con_duck = duckdb.connect()
    
    try:
        con_duck.execute("INSTALL httpfs; LOAD httpfs;")
        con_duck.execute(DUCKDB_SECRET_SQL)
        con_duck.execute("SET memory_limit='4GB';")
        
        if os.path.exists(CSV_PATH): os.remove(CSV_PATH)

        # Agregação massiva: O DuckDB resolve o ano_venda e mes_venda das pastas automaticamente
        query = f"""
            COPY (
                SELECT 
                    cnpj_loja, 
                    codigo_interno_produto,
                    SUM(CAST(qtd_de_produtos AS INT)) as qtd_total_vendida,
                    SUM(CAST(valor_liquido_total AS DECIMAL(15,2))) as valor_total_liquido,
                    SUM(CAST(valor_bruto_total AS DECIMAL(15,2))) as valor_total_bruto,
                    SUM(CAST(desconto_total AS DECIMAL(15,2))) as valor_total_desconto,
                    -- Cria a data YYYY-MM-01 baseada nas colunas de partição
                    strptime(concat(ano_venda, '-', mes_venda, '-01'), '%Y-%m-%d')::DATE as data_venda,
                    current_timestamp as data_atualizacao
                FROM read_parquet('{S3_SILVER_SOURCE}', hive_partitioning=1)
                WHERE cnpj_loja IS NOT NULL AND codigo_interno_produto IS NOT NULL
                GROUP BY 1, 2, 7
            ) TO '{CSV_PATH}' (FORMAT CSV, DELIMITER ';', HEADER FALSE);
        """
        con_duck.execute(query)
        print(f"✅ [DuckDB] Agregação total concluída em: {time.time() - start_duck:.2f}s")
            
    except Exception as e:
        print(f"❌ Erro DuckDB: {e}")
        sys.exit(1)
    finally:
        con_duck.close()

def csv_to_mariadb():
    if not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) == 0:
        print("⚠️ CSV vazio. Pulando MariaDB.")
        return

    tamanho = os.path.getsize(CSV_PATH)
    print(f"🐬 [2/3] Iniciando carga no MariaDB: {tamanho / 1e6:.2f} MB")
    
    conn_maria = None
    try:
        conn_maria = mariadb.connect(**DB_CONFIG)
        cursor = conn_maria.cursor()

        table_main = "gold_plugpharma_sellout_comercial"
        table_new = f"{table_main}_new"
        table_old = f"{table_main}_old"

        cursor.execute(f"DROP TABLE IF EXISTS {table_new}")
        cursor.execute(f"""
            CREATE TABLE {table_new} (
                id_fato INT AUTO_INCREMENT PRIMARY KEY,
                cnpj_loja VARCHAR(20),
                codigo_interno_produto VARCHAR(50),
                qtd_total_vendida INT,
                valor_total_liquido DECIMAL(15,2),
                valor_total_bruto DECIMAL(15,2),
                valor_total_desconto DECIMAL(15,2),
                data_venda DATE,
                data_atualizacao DATETIME
            ) ENGINE=Aria TRANSACTIONAL=0;
        """)

        monitor = DBMonitor(DB_CONFIG)
        monitor.start(table_name=table_new, total_bytes_csv=tamanho)

        # ORDEM DAS COLUNAS AJUSTADA PARA BATER COM O SELECT DO DUCKDB
        sql_load = f"""
            LOAD DATA LOCAL INFILE '{CSV_PATH}'
            INTO TABLE {table_new}
            FIELDS TERMINATED BY ';' LINES TERMINATED BY '\\n'
            (cnpj_loja, codigo_interno_produto, qtd_total_vendida, valor_total_liquido, 
             valor_total_bruto, valor_total_desconto, data_venda, data_atualizacao)
        """
        cursor.execute(sql_load)
        conn_maria.commit()
        monitor.stop()
        
        print("⚙️ Criando índices e swap de tabelas...")
        cursor.execute(f"ALTER TABLE {table_new} ADD INDEX idx_dt (data_venda), ADD INDEX idx_cnpj (cnpj_loja);")
        
        cursor.execute(f"DROP TABLE IF EXISTS {table_old}")
        cursor.execute(f"SHOW TABLES LIKE '{table_main}'")
        if cursor.fetchone():
            cursor.execute(f"RENAME TABLE {table_main} TO {table_old}, {table_new} TO {table_main}")
        else:
            cursor.execute(f"RENAME TABLE {table_new} TO {table_main}")
        
        conn_maria.commit()
        print(f"✅ Carga MariaDB finalizada!")

    except Exception as e:
        print(f"❌ Erro MariaDB: {e}")
        sys.exit(1)
    finally:
        if conn_maria: conn_maria.close()
    
def limpar_temp():
    if os.path.exists(CSV_PATH):
        try:
            os.remove(CSV_PATH)
            print(f"🧹 [3/3] Arquivo temporário removido.")
        except Exception as e:
            print(f"⚠️ Erro ao limpar arquivo: {e}")

if __name__ == "__main__":
    duckdb_to_csv()
    csv_to_mariadb()
    limpar_temp()