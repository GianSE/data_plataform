import duckdb
import mariadb
import os
import tempfile
import sys
from _utils.monitor import DBMonitor
from _utils.hash_generator import sql_gerar_hash_id
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
    print(f"📂 [1/4] Arquivo temporário definido: {CSV_PATH}")
    
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
                -- Ordem Fixa: 1, 2, 3, 4, 5
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
    print(f"🐬 [2/4] Iniciando carga no MariaDB: {tamanho / 1e6:.2f} MB")

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
        
        # --- MUDANÇA NO DDL: IDs agora são BIGINT ---
        ddl = f"""
        CREATE TABLE {table_new} (
            id_fato INT AUTO_INCREMENT PRIMARY KEY,
            id_produto VARCHAR(16),
            id_marca VARCHAR(16),
            id_fabricante VARCHAR(16),
            id_grupo_subclasse VARCHAR(16),
            id_fornecedor VARCHAR(16),
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
        (
            -- TEM QUE SER A MESMA ORDEM DO DUCKDB
            id_produto,          -- 1
            id_marca,            -- 2
            id_fabricante,       -- 3
            id_grupo_subclasse,  -- 4
            id_fornecedor,       -- 5
            
            -- Resto...
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
            ADD INDEX idx_fabricante (id_fabricante),
            ADD INDEX idx_grupo_subclasse (id_grupo_subclasse),
            ADD INDEX idx_fornecedor (id_fornecedor),
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
    print("🧹 [3/4] Limpeza de arquivos temporários...")
    if os.path.exists(CSV_PATH):
        try:
            os.remove(CSV_PATH)
            print("✅ Arquivo removido com sucesso.")
        except Exception as e:
            print(f"⚠️ Aviso: Não foi possível remover o arquivo: {e}")
    else:
        print("ℹ️ Nenhum arquivo para limpar.")

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
        sql = f"""
            SELECT COUNT(DISTINCT f.{col_id}) 
            FROM {table_fato} f
            LEFT JOIN {table_dim} d ON f.{col_id} = d.{col_id}
            WHERE d.{col_id} IS NULL
        """
        cursor.execute(sql)
        orphans = cursor.fetchone()[0]
        
        if orphans > 0:
            print(f"⚠️ ALERTA CRÍTICO: {orphans} IDs de {col_id} na Fato não existem na {table_dim}!")
            
            # Opcional: Mostrar exemplo de ID órfão para você investigar
            sql_exemplo = f"""
                SELECT DISTINCT f.{col_id} 
                FROM {table_fato} f
                LEFT JOIN {table_dim} d ON f.{col_id} = d.{col_id}
                WHERE d.{col_id} IS NULL
                LIMIT 1
            """
            cursor.execute(sql_exemplo)
            ex_id = cursor.fetchone()[0]
            print(f"Exemplo de Hash órfão: {ex_id}")
            
        else:
            print(f"✅ {col_id}: Integridade 100%.")
            
    conn.close()

# --- ORQUESTRAÇÃO PRINCIPAL ---
if __name__ == "__main__":
    duckdb_csv()
    csv_mariadb()
    limpar_temp()
    verificar_integridade()