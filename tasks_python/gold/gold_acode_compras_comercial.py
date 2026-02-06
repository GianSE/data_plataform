import duckdb
import mariadb
import os
import tempfile
import sys
from _utils.monitor import DBMonitor
from _settings.config import DB_CONFIG, DUCKDB_SECRET_SQL, setup_minio_env, get_temp_csv_caminho

# Para enxergar um diretório acima
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)


# 1. Configura o ambiente (MinIO) automaticamente
setup_minio_env()

# 2. Define o caminho do CSV usando a função padronizada
CSV_PATH = get_temp_csv_caminho("carga_gold_final.csv")

# Caminho minio da tabela silver 
S3_BASE = "s3://silver/silver_acode_compras_produto_comercial/**/*.parquet"

# Caminho do arquivo definido globalmente para todas as funções verem
TEMP_DIR = tempfile.gettempdir()

def duckdb_csv():
    print(f"📂 [1/3] Arquivo temporário definido: {CSV_PATH}")
    
    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(DUCKDB_SECRET_SQL)

        # --- CONFIGURAÇÕES CORRETAS PARA EVITAR O ERRO DE CAST/BUFFER ---
        con.execute("SET http_keep_alive=false;") # Evita problemas com conexões persistentes no MinIO
        con.execute("SET preserve_insertion_order=false;") 
        con.execute("PRAGMA disable_object_cache;") # Importante para evitar o erro de ponteiro negativo
        # --------------------------------------------------------------

        print("🦆 DuckDB: Extraindo dados e gerando CSV...")
        
        # --- MUDANÇA PRINCIPAL: IDs convertidos para VARCHAR (Texto) ---
        query = f"""
        COPY (
            SELECT 
                CAST(hash(concat(EAN, Produto)) AS VARCHAR) AS id_produto,
                CAST(hash(Desc_Marca) AS VARCHAR) AS id_marca,
                CAST(hash(Fabricante) AS VARCHAR) AS id_fabricante,
                CAST(hash(concat(Grupo, Sub_Classe)) AS VARCHAR) AS id_grupo_subclasse,
                CAST(hash(Fornecedor) AS VARCHAR) AS id_fornecedor,
                CAST(Loja_CNPJ AS VARCHAR(20)) AS loja_cnpj,
                CAST(Ano_Mes AS DATE) AS Ano_Mes,
                
                CAST(ACODE_Val_Total AS DECIMAL(15,4)) AS acode_val_total,
                CAST(Qtd_Trib AS INT) AS qtd_trib,
                
                now() AS data_atualizacao
            FROM read_parquet('{S3_BASE}')
        ) TO '{CSV_PATH}' (FORMAT CSV, DELIMITER ';', HEADER FALSE);
        """
        con.execute(query)
        print("✅ CSV gerado com sucesso.")
    except Exception as e:
        print(f"❌ Erro no DuckDB: {e}")
        sys.exit(1) # Para o script se falhar aqui
    finally:
        con.close()

def csv_mariadb():
    if not os.path.exists(CSV_PATH):
        print("❌ Erro: Arquivo CSV não encontrado. Pulo etapa.")
        return

    tamanho = os.path.getsize(CSV_PATH)
    print(f"🐬 [2/3] Iniciando carga no MariaDB: {tamanho / 1e6:.2f} MB")

    conn = None
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()

        table_prod = "gold_acode_compras_produto_comercial"
        table_new = f"{table_prod}_new"
        table_old = f"{table_prod}_old"

        # Limpeza preventiva
        cursor.execute(f"DROP TABLE IF EXISTS {table_new}")
        cursor.execute(f"DROP TABLE IF EXISTS {table_old}")

        print(f"🔨 Criando tabela staging: {table_new}")
        
        # --- MUDANÇA NO DDL: IDs agora são VARCHAR(50) ---
        ddl = f"""
        CREATE TABLE {table_new} (
            id_fato INT AUTO_INCREMENT PRIMARY KEY,
            id_produto VARCHAR(50), 
            id_marca VARCHAR(50), 
            id_fornecedor VARCHAR(20),
            id_fabricante VARCHAR(50), 
            id_grupo_subclasse VARCHAR(50),
            loja_cnpj VARCHAR(20), 
            Ano_Mes DATE,
            acode_val_total DECIMAL(15,4),
            qtd_trib INT, 
            data_atualizacao DATETIME
        ) ENGINE=Aria TRANSACTIONAL=0 ROW_FORMAT=PAGE;
        """
        cursor.execute(ddl)

        # Monitoramento
        # (Atenção: removi os ** do DB_CONFIG se você ajustou a classe Monitor como conversamos antes. 
        # Se não ajustou, mantenha os **). Assumindo a versão corrigida:
        monitor = DBMonitor(DB_CONFIG)
        monitor.start(table_name=table_new, total_bytes_csv=tamanho)

        # Carga
        print("🚚 Carregando dados...")
        sql_load = f"""
        LOAD DATA LOCAL INFILE '{CSV_PATH}'
        INTO TABLE {table_new}
        FIELDS TERMINATED BY ';' LINES TERMINATED BY '\\n'
        (id_produto, id_marca, fornecedor_cnpj, id_fabricante, id_grupo_subclasse, 
         loja_cnpj, Ano_Mes, acode_val_total, qtd_trib, data_atualizacao)
        """
        cursor.execute(sql_load)
        conn.commit()

        monitor.stop()
        
        # 1. Otimização de Memória para a Sessão
        print("🚀 Reservando RAM para ordenação de índices...")
        cursor.execute("SET SESSION aria_sort_buffer_size = 512 * 1024 * 1024;") 
        cursor.execute("SET SESSION sort_buffer_size = 512 * 1024 * 1024;")

        # 2. Índices em Bloco
        print("⚙️ Criando todos os índices em bloco (ALTER TABLE)...")
        sql_indices = f"""
        ALTER TABLE {table_new}
            ADD INDEX idx_produto (id_produto),
            ADD INDEX idx_marca (id_marca),
            ADD INDEX idx_fornecedor_cnpj (fornecedor_cnpj),
            ADD INDEX idx_fabricante (id_fabricante),
            ADD INDEX idx_grupo_subclasse (id_grupo_subclasse),
            ADD INDEX idx_loja_cnpj (loja_cnpj),
            ADD INDEX idx_Ano_Mes (Ano_Mes);
        """
        cursor.execute(sql_indices)
        print("✅ Índices criados com sucesso.")

        # Swap
        print("🔄 Trocando tabelas (Blue-Green Deployment)...")
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
            conn.rollback()
        sys.exit(1)
    finally:
        if conn: 
            conn.close()

def limpar_temp():
    print("🧹 [3/3] Limpeza de arquivos temporários...")
    if os.path.exists(CSV_PATH):
        try:
            os.remove(CSV_PATH)
            print("✅ Arquivo removido com sucesso.")
        except Exception as e:
            print(f"⚠️ Aviso: Não foi possível remover o arquivo: {e}")
    else:
        print("ℹ️ Nenhum arquivo para limpar.")

# --- ORQUESTRAÇÃO PRINCIPAL ---
if __name__ == "__main__":
    duckdb_csv()
    csv_mariadb()
    limpar_temp()