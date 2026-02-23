import duckdb
import mariadb
import os
import sys
import time
import threading
from datetime import datetime
from _utils.monitor import DBMonitor
from _utils.hash_generator import sql_gerar_hash_id
from _settings.config import DB_CONFIG, DUCKDB_SECRET_SQL, setup_minio_env, get_temp_csv_caminho

# Para enxergar um diretório acima
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

# 1. Configura o ambiente (MinIO) automaticamente
setup_minio_env()

# 2. Define o caminho do CSV
CSV_PATH = get_temp_csv_caminho("carga_gold_final.csv")
S3_BASE = "s3://silver/silver_acode_sellin_compras/**/*.parquet"

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

def duckdb_csv():
    print(f"📂 [1/4] DuckDB: Agregando Silver para CSV (INCREMENTAL): {CSV_PATH}")
    start_time = time.time()
    
    if os.path.exists(CSV_PATH): os.remove(CSV_PATH)

    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(DUCKDB_SECRET_SQL)
        
        # --- CONFIGURAÇÕES DE ESTABILIDADE E RECURSOS ---
        con.execute("SET threads=2;") 
        con.execute("SET memory_limit='2GB';")
        con.execute("SET http_keep_alive=false;")
        con.execute("SET max_expression_depth=250;")

        print("🦆 DuckDB: Extraindo dados e gerando CSV...")
        
        # Lê a base com suporte a partições
        rel = con.from_parquet(S3_BASE, hive_partitioning=True)
        rel.create_view("stg_compras")

        # ---------------------------------------------------------
        # LÓGICA INCREMENTAL DE DATAS
        # ---------------------------------------------------------
        ano_atual = datetime.now().year
        mes_atual = datetime.now().month
        ano_anterior = ano_atual if mes_atual > 1 else ano_atual - 1
        mes_anterior = mes_atual - 1 if mes_atual > 1 else 12
        
        # Cria a data de corte: Primeiro dia do mês anterior (ex: '2026-01-01')
        data_corte = f"{ano_anterior}-{mes_anterior:02d}-01"

        query = f"""
        COPY (
            SELECT 
                {sql_gerar_hash_id(['EAN', 'Produto'], 'id_produto')},
                {sql_gerar_hash_id(['Desc_Marca'], 'id_marca')},
                {sql_gerar_hash_id(['Fabricante'], 'id_fabricante')},
                {sql_gerar_hash_id(['Grupo', 'Sub_Classe'], 'id_grupo_subclasse')},
                {sql_gerar_hash_id(['Fornecedor'], 'id_fornecedor')},
                
                EAN AS ean,
                Loja_CNPJ AS loja_cnpj,
                Ano_Mes AS Ano_Mes,
                
                ACODE_Val_Total AS acode_val_total,
                Qtd_Trib AS qtd_trib, 
                
                strftime(now(), '%Y-%m-%d %H:%M:%S') AS data_atualizacao
            FROM stg_compras 
            WHERE Ano_Mes >= CAST('{data_corte}' AS DATE)
            ORDER BY Ano_Mes ASC, id_produto ASC, loja_cnpj ASC
        ) TO '{CSV_PATH}' (FORMAT CSV, DELIMITER ';', HEADER FALSE, NULL '\\N');
        """
        stop_monitor = threading.Event()
        t = threading.Thread(target=monitorar_crescimento_csv, args=(stop_monitor,))
        t.start()
        
        print(f"🚀 Iniciando processamento a partir de {data_corte}...")
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

def csv_mariadb():
    if not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) == 0:
        print("❌ Erro: Arquivo CSV não encontrado ou vazio. Pulo etapa.")
        return

    tamanho = os.path.getsize(CSV_PATH)
    print(f"🐬 [2/4] Iniciando carga INCREMENTAL no MariaDB: {tamanho / 1e6:.2f} MB")

    conn = None
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()

        table_prod = "gold_acode_sellin_comercial"

        # ---------------------------------------------------------
        # GERAÇÃO DINÂMICA DE PARTIÇÕES
        # ---------------------------------------------------------
        ano_atual = datetime.now().year
        ano_inicio = 2022
        particoes_list = []
        for ano in range(ano_inicio, ano_atual + 2):
            particoes_list.append(f"PARTITION p{ano} VALUES LESS THAN ({ano + 1})")
        particoes_list.append("PARTITION pmax VALUES LESS THAN MAXVALUE")
        sql_particoes_dinamicas = ",\n            ".join(particoes_list)
        # ---------------------------------------------------------

        # Usa o IF NOT EXISTS para criar a tabela apenas se for a primeira rodada
        print(f"🔨 Garantindo a estrutura da tabela: {table_prod}")
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table_prod} (
            -- Chaves
            id_produto VARCHAR(16) NOT NULL,
            id_marca VARCHAR(16) NOT NULL,
            id_fabricante VARCHAR(16) NOT NULL,
            id_grupo_subclasse VARCHAR(16) NOT NULL,
            id_fornecedor VARCHAR(16) NOT NULL,
            ean VARCHAR(20),

            -- Contexto
            loja_cnpj VARCHAR(20) NOT NULL, 
            Ano_Mes DATE NOT NULL,
            
            -- Métricas
            acode_val_total DECIMAL(15,4),
            qtd_trib INT, 
            data_atualizacao DATETIME,

            -- PK (Garante o funcionamento do REPLACE)
            PRIMARY KEY (Ano_Mes, id_produto, loja_cnpj),

            -- Índices
            KEY idx_produto (id_produto),
            KEY idx_ean (ean),
            KEY idx_marca (id_marca),
            KEY idx_fabricante (id_fabricante),
            KEY idx_grupo (id_grupo_subclasse),
            KEY idx_fornecedor (id_fornecedor),
            KEY idx_loja (loja_cnpj)

        ) ENGINE=InnoDB 
          DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci
          PARTITION BY RANGE (YEAR(Ano_Mes)) (
            {sql_particoes_dinamicas}
          );
        """
        cursor.execute(ddl)

        cursor.execute("SET unique_checks=0")
        cursor.execute("SET foreign_key_checks=0")
        cursor.execute("SET bulk_insert_buffer_size=268435456") # Valor calculado corrigido
        conn.autocommit = False

        monitor = DBMonitor(DB_CONFIG)
        monitor.start(table_name=table_prod, total_bytes_csv=tamanho)

        print("🚚 Carregando dados...")
        
        # O pulo do gato: REPLACE INTO na tabela principal
        sql_load = f"""
        LOAD DATA LOCAL INFILE '{CSV_PATH}'
        REPLACE INTO TABLE {table_prod}
        FIELDS TERMINATED BY ';' LINES TERMINATED BY '\\n'
        (
            id_produto, 
            id_marca, 
            id_fabricante, 
            id_grupo_subclasse, 
            id_fornecedor, 
            
            ean,
            
            loja_cnpj, 
            Ano_Mes, 
            acode_val_total, 
            qtd_trib, 
            data_atualizacao
        )
        """
        cursor.execute(sql_load)
        conn.commit()
        monitor.stop()

        conn.autocommit = True
        cursor.execute("SET unique_checks=1")
        cursor.execute("SET foreign_key_checks=1")

        print("🏁 Carga Incremental finalizada!")

    except Exception as e:
        print(f"❌ Erro no MariaDB: {e}")
        if conn: 
            try: conn.rollback()
            except: pass
        sys.exit(1)
    finally:
        if conn: conn.close()

def limpar_temp():
    print("🧹 [3/4] Limpeza...")
    if os.path.exists(CSV_PATH):
        try: os.remove(CSV_PATH)
        except Exception: pass

def verificar_integridade():
    print("🔍 [4/4] Verificando integridade referencial...")
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    checks = [
            ("id_produto",         "dim_produto_acode"),
            ("id_marca",           "dim_marca_acode"),
            ("id_fabricante",      "dim_fabricante_acode"),
            ("id_grupo_subclasse", "dim_grupo_subclasse_acode"),
            ("id_fornecedor",      "dim_fornecedor_acode")
        ]
    table_fato = "gold_acode_sellin_comercial"
    
    for col_id, table_dim in checks:
        sql = f"""
            SELECT f.{col_id} FROM {table_fato} f
            LEFT JOIN {table_dim} d ON f.{col_id} = d.{col_id}
            WHERE d.{col_id} IS NULL LIMIT 1
        """
        try:
            cursor.execute(sql)
            if cursor.fetchone(): print(f"⚠️ Órfão em {col_id}")
            else: print(f"✅ {col_id}: OK")
        except: pass
    conn.close()

if __name__ == "__main__":
    duckdb_csv()
    csv_mariadb()
    limpar_temp()
    verificar_integridade()