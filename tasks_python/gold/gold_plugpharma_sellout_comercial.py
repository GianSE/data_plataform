import duckdb
import mariadb
import os
import sys
import time
from datetime import datetime, timedelta
from _utils.monitor import DBMonitor
from _settings.config import DB_CONFIG, DUCKDB_SECRET_SQL, setup_minio_env, get_temp_csv_caminho

# Ajuste de PATH para enxergar módulos internos
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

# 1. Configurações Iniciais
setup_minio_env()
CSV_PATH = get_temp_csv_caminho("carga_gold_sellout.csv")
S3_SILVER_BASE = "s3://silver/silver_plugpharma_sellout_comercial/**/*.parquet"

def duckdb_to_csv():
    print(f"🚀 [1/3] DuckDB: Extraindo Silver para CSV...")
    start_time = time.time()
    
    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(DUCKDB_SECRET_SQL)
        con.execute("SET preserve_insertion_order=false;")
        con.execute("SET memory_limit='3GB';")
        con.execute("SET threads=4;")

        # Query que extrai os dados já agrupados da sua Silver
        # Note: Usamos o union_by_name=True por segurança contra schemas viciados
        query = f"""
        COPY (
            SELECT 
                data_venda,
                cnpj_loja,
                codigo_interno_produto,
                qtd_vendida,
                valor_liquido,
                data_processamento
            FROM read_parquet('{S3_SILVER_BASE}', union_by_name=True)
        ) TO '{CSV_PATH}' (FORMAT CSV, DELIMITER ';', HEADER FALSE);
        """
        con.execute(query)
        
        duration = time.time() - start_time
        print(f"✅ CSV gerado em {duration:.2f}s em: {CSV_PATH}")
        return True
    except Exception as e:
        print(f"❌ Erro no DuckDB: {e}")
        return False
    finally:
        con.close()

def csv_to_mariadb():
    if not os.path.exists(CSV_PATH):
        print("❌ Erro: CSV não encontrado.")
        return

    tamanho = os.path.getsize(CSV_PATH)
    print(f"🐬 [2/3] MariaDB: Iniciando carga de {tamanho / 1e6:.2f} MB...")

    table_prod = "gold_plugpharma_sellout_comercial"
    table_new = f"{table_prod}_new"
    table_old = f"{table_prod}_old"

    conn = None
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Limpeza e DDL
        cursor.execute(f"DROP TABLE IF EXISTS {table_new}")
        cursor.execute(f"DROP TABLE IF EXISTS {table_old}")

        ddl = f"""
        CREATE TABLE {table_new} (
            id_fato INT AUTO_INCREMENT PRIMARY KEY,
            data_venda DATE,
            cnpj_loja VARCHAR(20),
            codigo_interno_produto VARCHAR(50),
            qtd_vendida INT,
            valor_liquido DECIMAL(15,2),
            data_processamento DATETIME
        ) ENGINE=Aria TRANSACTIONAL=0 ROW_FORMAT=PAGE;
        """
        cursor.execute(ddl)

        # Carga via LOAD DATA (O mais rápido)
        print("🚚 Movendo dados para o banco...")
        sql_load = f"""
        LOAD DATA LOCAL INFILE '{CSV_PATH}'
        INTO TABLE {table_new}
        FIELDS TERMINATED BY ';' LINES TERMINATED BY '\\n'
        (data_venda, cnpj_loja, codigo_interno_produto, qtd_vendida, valor_liquido, data_processamento)
        """
        cursor.execute(sql_load)
        conn.commit()

        # Criação de Índices Individuais (Conforme solicitado)
        print("⚙️ Criando índices (data_venda, cnpj_loja, codigo_interno)...")
        # Otimizamos a sessão para criar índices mais rápido
        cursor.execute("SET SESSION aria_sort_buffer_size = 256 * 1024 * 1024;")
        
        cursor.execute(f"CREATE INDEX idx_data_venda ON {table_new} (data_venda);")
        cursor.execute(f"CREATE INDEX idx_cnpj_loja ON {table_new} (cnpj_loja);")
        cursor.execute(f"CREATE INDEX idx_cod_produto ON {table_new} (codigo_interno_produto);")

        # Swap de Tabelas (Blue-Green)
        cursor.execute(f"SHOW TABLES LIKE '{table_prod}'")
        if cursor.fetchone():
            cursor.execute(f"RENAME TABLE {table_prod} TO {table_old}, {table_new} TO {table_prod}")
            cursor.execute(f"DROP TABLE IF EXISTS {table_old}")
        else:
            cursor.execute(f"RENAME TABLE {table_new} TO {table_prod}")

        conn.commit()
        print(f"✅ Tabela {table_prod} atualizada com sucesso!")

    except Exception as e:
        print(f"❌ Erro no MariaDB: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def limpar_cache():
    print("🧹 [3/3] Limpando arquivos temporários...")
    if os.path.exists(CSV_PATH):
        os.remove(CSV_PATH)

if __name__ == "__main__":
    if duckdb_to_csv():
        csv_to_mariadb()
        limpar_cache()
    print("🏁 Fim.")