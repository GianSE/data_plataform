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
S3_SILVER_SOURCE = "s3://silver/silver_plugpharma_vendas/**/*.parquet"

def duckdb_to_csv():
    print(f"📂 [1/3] Agregando Silver para CSV: {CSV_PATH}")
    start_duck = time.time()
    con_duck = duckdb.connect()
    
    try:
        con_duck.execute("INSTALL httpfs; LOAD httpfs;")
        con_duck.execute(DUCKDB_SECRET_SQL)
        con_duck.execute("SET memory_limit='4GB';")
        
        if os.path.exists(CSV_PATH): os.remove(CSV_PATH)

        # AJUSTE 1: MUDAMOS A ORDEM AQUI!
        # Agora ordenamos: 5(Data) -> 2(Produto) -> 1(Loja)
        # Isso prepara o terreno para a nova PK do MariaDB.
        query = f"""
            COPY (
                SELECT 
                    cnpj_loja, 
                    codigo_interno_produto,
                    -- Recomendação: Use BIGINT aqui também para evitar overflow em somas gigantes
                    SUM(CAST(qtd_de_produtos AS BIGINT)) as qtd_total_vendida,
                    SUM(CAST(valor_liquido_total AS DECIMAL(15,2))) as valor_total_liquido,
                    strptime(concat(ano_venda, '-', mes_venda, '-01'), '%Y-%m-%d')::DATE as data_venda_mes,
                    current_timestamp as data_atualizacao
                FROM read_parquet(
                    '{S3_SILVER_SOURCE}', 
                    hive_partitioning=1,
                    schema={{'qtd_de_produtos': 'BIGINT'}} -- <--- O FIX ESTÁ AQUI
                )
                WHERE 
                    cnpj_loja IS NOT NULL 
                    AND codigo_interno_produto IS NOT NULL
                    AND ano_venda >= 2022
                GROUP BY 1, 2, 5
                ORDER BY 5 ASC, 2 ASC, 1 ASC
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

        # ---------------------------------------------------------
        # 0. GERAÇÃO DINÂMICA DE PARTIÇÕES (O Pulo do Gato) 🐱
        # ---------------------------------------------------------
        ano_atual = datetime.now().year
        ano_inicio = 2022 # Seu ano histórico base
        
        # Cria lista de partições de 2022 até o Ano Atual + 1 (pra garantir virada de ano)
        particoes_list = []
        for ano in range(ano_inicio, ano_atual + 2):
            particoes_list.append(f"PARTITION p{ano} VALUES LESS THAN ({ano + 1})")
        
        # Adiciona a partição final (obrigatória)
        particoes_list.append("PARTITION pmax VALUES LESS THAN MAXVALUE")
        
        # Junta tudo numa string SQL
        sql_particoes_dinamicas = ",\n        ".join(particoes_list)
        
        print(f"⚙️ Partições geradas dinamicamente:\n        {sql_particoes_dinamicas}")
        # ---------------------------------------------------------

        cursor.execute(f"DROP TABLE IF EXISTS {table_old}")
        cursor.execute(f"DROP TABLE IF EXISTS {table_new}")

        # Injetamos a variável {sql_particoes_dinamicas} no final do CREATE
        cursor.execute(f"""
            CREATE TABLE {table_new} (
                cnpj_loja VARCHAR(20) NOT NULL,
                codigo_interno_produto VARCHAR(10) NOT NULL,
                qtd_total_vendida INT,
                valor_total_liquido DECIMAL(15,2),
                data_venda_mes DATE NOT NULL,
                data_atualizacao DATETIME,

                -- PRIMARY KEY (Data -> Produto -> Loja)
                PRIMARY KEY (data_venda_mes, codigo_interno_produto, cnpj_loja),

                -- Índices Secundários
                KEY idx_loja_data (cnpj_loja, data_venda_mes),
                KEY idx_join_produto (codigo_interno_produto)
            ) 
            ENGINE=InnoDB 
            DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci
            PARTITION BY RANGE (YEAR(data_venda_mes)) (
                {sql_particoes_dinamicas}
            );
        """)

        # Configurações de performance
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
        try:
            os.remove(CSV_PATH)
            print(f"🧹 [3/3] Arquivo temporário removido.")
        except Exception as e:
            print(f"⚠️ Erro ao limpar arquivo: {e}")

if __name__ == "__main__":
    duckdb_to_csv()
    csv_to_mariadb()
    limpar_temp()