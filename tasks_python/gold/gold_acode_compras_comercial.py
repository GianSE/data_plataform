import duckdb
import mariadb
import os
import tempfile
import sys
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
S3_BASE = "s3://silver/silver_acode_compras_produto_comercial/**/*.parquet"

def duckdb_csv():
    print(f"📂 [1/4] Arquivo temporário definido: {CSV_PATH}")
    
    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(DUCKDB_SECRET_SQL)

        # Configurações de memória e cache
        con.execute("SET http_keep_alive=false;")
        con.execute("SET preserve_insertion_order=false;") 
        con.execute("PRAGMA disable_object_cache;") 

        print("🦆 DuckDB: Extraindo dados e gerando CSV...")
        
        # IMPORTANTE: A ordem do SELECT deve bater com o LOAD DATA
        # IMPORTANTE: O ORDER BY deve bater com a PRIMARY KEY do MariaDB
        query = f"""
        COPY (
            SELECT 
                {sql_gerar_hash_id(['EAN', 'Produto'], 'id_produto')},          -- 1
                {sql_gerar_hash_id(['Desc_Marca'], 'id_marca')},                -- 2
                {sql_gerar_hash_id(['Fabricante'], 'id_fabricante')},           -- 3
                {sql_gerar_hash_id(['Grupo', 'Sub_Classe'], 'id_grupo_subclasse')}, -- 4
                {sql_gerar_hash_id(['Fornecedor'], 'id_fornecedor')},           -- 5
                CAST(Loja_CNPJ AS VARCHAR(20)) AS loja_cnpj,
                CAST(Ano_Mes AS DATE) AS Ano_Mes,
                CAST(ACODE_Val_Total AS DECIMAL(15,4)) AS acode_val_total,
                CAST(Qtd_Trib AS INT) AS qtd_trib,
                now() AS data_atualizacao
            FROM read_parquet('{S3_BASE}')
            WHERE ano >= 2023
            -- ALINHADO COM A PK (Data -> Produto -> Loja)
            ORDER BY Ano_Mes ASC, id_produto ASC, loja_cnpj ASC
        ) TO '{CSV_PATH}' (FORMAT CSV, DELIMITER ';', HEADER FALSE);
        """
        con.execute(query)
        print("✅ CSV gerado com sucesso.")
    except Exception as e:
        print(f"❌ Erro no DuckDB: {e}")
        sys.exit(1)
    finally:
        con.close()

def csv_mariadb():
    if not os.path.exists(CSV_PATH):
        print("❌ Erro: Arquivo CSV não encontrado. Pulo etapa.")
        return

    tamanho = os.path.getsize(CSV_PATH)
    print(f"🐬 [2/4] Iniciando carga no MariaDB (InnoDB Otimizado): {tamanho / 1e6:.2f} MB")

    conn = None
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()

        table_prod = "gold_acode_compras_produto_comercial"
        table_new = f"{table_prod}_new"
        table_old = f"{table_prod}_old"

        # ---------------------------------------------------------
        # 0. GERAÇÃO DINÂMICA DE PARTIÇÕES
        # ---------------------------------------------------------
        ano_atual = datetime.now().year
        ano_inicio = 2022
        
        particoes_list = []
        for ano in range(ano_inicio, ano_atual + 2):
            particoes_list.append(f"PARTITION p{ano} VALUES LESS THAN ({ano + 1})")
        
        particoes_list.append("PARTITION pmax VALUES LESS THAN MAXVALUE")
        
        # Formatação bonita para injetar no SQL
        sql_particoes_dinamicas = ",\n            ".join(particoes_list)
        
        print(f"⚙️ Partições geradas dinamicamente: {len(particoes_list)} partições.")
        # ---------------------------------------------------------

        # Limpeza
        cursor.execute(f"DROP TABLE IF EXISTS {table_new}")
        cursor.execute(f"DROP TABLE IF EXISTS {table_old}")

        print(f"🔨 Criando tabela otimizada: {table_new}")
        
        ddl = f"""
        CREATE TABLE {table_new} (
            -- Dimensões (IDs)
            id_produto VARCHAR(16) NOT NULL,
            id_marca VARCHAR(16) NOT NULL,
            id_fabricante VARCHAR(16) NOT NULL,
            id_grupo_subclasse VARCHAR(16) NOT NULL,
            id_fornecedor VARCHAR(16) NOT NULL,
            
            -- Contexto
            loja_cnpj VARCHAR(20) NOT NULL, 
            Ano_Mes DATE NOT NULL,
            
            -- Métricas
            acode_val_total DECIMAL(15,4),
            qtd_trib INT, 
            data_atualizacao DATETIME,

            -- 1. PRIMARY KEY (CLUSTERED INDEX)
            -- Estratégia atual: Data -> Produto -> Loja
            PRIMARY KEY (Ano_Mes, id_produto, loja_cnpj),

            -- 2. ÍNDICES SECUNDÁRIOS
            -- Índice vital para buscas de produto que ignoram a loja
            KEY idx_produto (id_produto),
            
            -- Outros índices dimensionais
            KEY idx_marca (id_marca),
            KEY idx_fabricante (id_fabricante),
            KEY idx_grupo (id_grupo_subclasse),
            KEY idx_fornecedor (id_fornecedor),
            
            -- Índice isolado para loja (bom para joins rápidos)
            KEY idx_loja (loja_cnpj)

        ) ENGINE=InnoDB 
          DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci
          PARTITION BY RANGE (YEAR(Ano_Mes)) (
            {sql_particoes_dinamicas}
          );
        """
        cursor.execute(ddl)

        # Configurações de Carga Rápida
        cursor.execute("SET unique_checks=0")
        cursor.execute("SET foreign_key_checks=0")
        cursor.execute("SET bulk_insert_buffer_size=256*1024*1024") 
        conn.autocommit = False

        monitor = DBMonitor(DB_CONFIG)
        monitor.start(table_name=table_new, total_bytes_csv=tamanho)

        print("🚚 Carregando dados (Stream)...")
        sql_load = f"""
        LOAD DATA LOCAL INFILE '{CSV_PATH}'
        INTO TABLE {table_new}
        FIELDS TERMINATED BY ';' LINES TERMINATED BY '\\n'
        (
            id_produto, 
            id_marca, 
            id_fabricante, 
            id_grupo_subclasse, 
            id_fornecedor, 
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

        # Reativa verificações
        conn.autocommit = True
        cursor.execute("SET unique_checks=1")
        cursor.execute("SET foreign_key_checks=1")

        print("🔄 Trocando tabelas...")
        cursor.execute(f"SHOW TABLES LIKE '{table_prod}'")
        if cursor.fetchone():
            cursor.execute(f"RENAME TABLE {table_prod} TO {table_old}, {table_new} TO {table_prod}")
        else:
            cursor.execute(f"RENAME TABLE {table_new} TO {table_prod}")
        
        cursor.execute(f"DROP TABLE IF EXISTS {table_old}")
        conn.commit()
        print("🏁 Carga finalizada!")

    except Exception as e:
        print(f"❌ Erro no MariaDB: {e}")
        if conn: 
            try: conn.rollback()
            except: pass
        sys.exit(1)
    finally:
        if conn: conn.close()

def limpar_temp():
    print("🧹 [3/4] Limpeza de arquivos temporários...")
    if os.path.exists(CSV_PATH):
        try:
            os.remove(CSV_PATH)
            print("✅ Arquivo removido com sucesso.")
        except Exception as e:
            print(f"⚠️ Aviso: Não foi possível remover o arquivo: {e}")

def verificar_integridade():
    print("🔍 [4/4] Verificando integridade referencial...")
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Lista de dimensões para checar
    checks = [
        ("id_produto", "dim_produto_acode"),
        ("id_marca", "dim_marca_acode"),
        ("id_fabricante", "dim_fabricante_acode"),
        ("id_grupo_subclasse", "dim_grupo_subclasse_acode"),
        ("id_fornecedor", "dim_fornecedor_acode")
    ]
    
    table_fato = "gold_acode_compras_produto_comercial"
    
    for col_id, table_dim in checks:
        # CORREÇÃO: Adicionado 'f.' antes de {col_id} no SELECT
        sql = f"""
            SELECT f.{col_id}
            FROM {table_fato} f
            LEFT JOIN {table_dim} d ON f.{col_id} = d.{col_id}
            WHERE d.{col_id} IS NULL
            LIMIT 1
        """
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
            
            if row:
                print(f"⚠️ ALERTA CRÍTICO: Encontrado ID órfão em {col_id}. Exemplo: {row[0]}")
            else:
                print(f"✅ {col_id}: Integridade 100%.")
        except Exception as e:
            print(f"❌ Erro ao verificar {col_id}: {e}")
            
    conn.close()

if __name__ == "__main__":
    # duckdb_csv()
    # csv_mariadb()
    # limpar_temp()
    verificar_integridade()