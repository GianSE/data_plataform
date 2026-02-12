import duckdb
import mariadb
import os
import sys
import hashlib # Necessário para a vacina
from _utils.hash_generator import sql_gerar_hash_id
from _settings.config import DB_CONFIG, DUCKDB_SECRET_SQL, setup_minio_env, get_temp_csv_caminho

# Ajuste de PATH
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

setup_minio_env()
S3_BASE = "s3://silver/silver_acode_compras/**/*.parquet"

DIMENSOES_CONFIG = [
    {
        "tabela": "dim_fabricante_acode",
        "s3_path": S3_BASE,
        "colunas_origem": ["Fabricante"],
        "hash_cols": ["Fabricante"],
        "id_col": "id_fabricante"
    },
    {
        "tabela": "dim_fornecedor_acode",
        "s3_path": S3_BASE,
        "colunas_origem": ["Fornecedor"], 
        "hash_cols": ["Fornecedor"],
        "id_col": "id_fornecedor",
    },
    {
        "tabela": "dim_marca_acode",
        "s3_path": S3_BASE,
        "colunas_origem": ["Desc_Marca"],
        "hash_cols": ["Desc_Marca"],
        "id_col": "id_marca",
        "renames": {"Desc_Marca": "nome_marca"}
    },
    {
        "tabela": "dim_grupo_subclasse_acode",
        "s3_path": S3_BASE,
        "colunas_origem": ["Grupo", "Sub_Classe"],
        "hash_cols": ["Grupo", "Sub_Classe"],
        "id_col": "id_grupo_subclasse"
    },
    {
        "tabela": "dim_produto_acode", 
        "s3_path": S3_BASE,
        "colunas_origem": ["EAN", "Produto"],
        "hash_cols": ["EAN", "Produto"],
        "id_col": "id_produto"
    }
]

def duckdb_csv(config):
    table_name = config["tabela"]
    csv_filename = f"{table_name}.csv"
    CSV_PATH = get_temp_csv_caminho(csv_filename)
    
    print(f"\n📦 [1/3] DuckDB: Processando {table_name}...")
    
    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(DUCKDB_SECRET_SQL)
        con.execute("SET preserve_insertion_order=false;")

        # Gera o SQL do Hash (Versão Hex Slice 15 -> BIGINT)
        hash_sql = sql_gerar_hash_id(config['hash_cols'], config['id_col'])
        
        select_cols = []
        for col in config["colunas_origem"]:
            novo_nome = config.get("renames", {}).get(col, col)
            # Limpeza CRÍTICA: Remove quebras de linha que matam o CSV
            select_cols.append(f"CAST(regexp_replace(\"{col}\", '[\n\r;]', '', 'g') AS VARCHAR) AS \"{novo_nome}\"")
            
        select_str = ", ".join(select_cols)

        # --- A MUDANÇA ESTÁ AQUI: SELECT DISTINCT ---
        # Substituímos GROUP BY ALL por DISTINCT.
        # Isso garante unicidade baseada exatamente nas colunas selecionadas.
        query = f"""
        COPY (
            SELECT DISTINCT
                {hash_sql},
                {select_str},
                now() AS data_atualizacao
            FROM read_parquet('{config['s3_path']}')
        ) TO '{CSV_PATH}' (FORMAT CSV, DELIMITER ';', HEADER FALSE);
        """
        con.execute(query)
        print("   ✅ CSV gerado (Modo DISTINCT).")
        return CSV_PATH

    except Exception as e:
        print(f"   ❌ Erro no DuckDB: {e}")
        return None
    finally:
        con.close()

def csv_mariadb(config, CSV_PATH):
    if not CSV_PATH or not os.path.exists(CSV_PATH):
        print("   ⚠️ CSV não encontrado.")
        return

    table_prod = config["tabela"]
    table_new = f"{table_prod}_new"
    table_old = f"{table_prod}_old"

    print(f"   🐬 [2/3] MariaDB: Carregando {table_prod}...")
    
    conn = None
    try:
        conn = mariadb.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute(f"DROP TABLE IF EXISTS {table_new}")
        cursor.execute(f"DROP TABLE IF EXISTS {table_old}")
        
        # --- DDL: VARCHAR(16) ---
        colunas_ddl = [f"{config['id_col']} VARCHAR(16) PRIMARY KEY"] # Agora é VARCHAR(16)
        colunas_load = [config['id_col']]
        
        for col in config["colunas_origem"]:
            novo_nome = config.get("renames", {}).get(col, col)
            colunas_ddl.append(f"`{novo_nome}` VARCHAR(255)")
            colunas_load.append(novo_nome)

        colunas_ddl.append("data_atualizacao DATETIME")
        colunas_load.append("data_atualizacao")
        
        ddl = f"""
        CREATE TABLE {table_new} (
            {', '.join(colunas_ddl)}
        ) ENGINE=InnoDB TRANSACTIONAL=0 ROW_FORMAT=PAGE;
        """
        cursor.execute(ddl)
        
        sql_load = f"""
        LOAD DATA LOCAL INFILE '{CSV_PATH}'
        INTO TABLE {table_new}
        FIELDS TERMINATED BY ';' LINES TERMINATED BY '\\n'
        ({', '.join(colunas_load)})
        """
        cursor.execute(sql_load)
        linhas_csv = cursor.rowcount
        print(f"   🚚 Carregados {linhas_csv} registros do CSV.")

        # --- 💉 A VACINA (CORREÇÃO DE ÓRFÃOS) ---
        # Injeta manualmente o ID do "ND" para garantir que ele exista.
        # Lógica Python idêntica ao SQL: md5('ND') -> slice 15 -> int
        
        # 1. Gera o ID do ND
        id_nd = hashlib.md5("ND".encode()).hexdigest()[:16]
        
        # Se tiver mais de uma coluna de texto (ex: Grupo, Subclasse), preenche todas com 'ND'
        campos_texto = []
        valores_texto = []
        for col in config["colunas_origem"]:
            nome_campo = config.get("renames", {}).get(col, col)
            campos_texto.append(f"`{nome_campo}`")
            valores_texto.append("'ND - NAO DEFINIDO'") # Valor padrão
            
        sql_vacina = f"""
        INSERT IGNORE INTO {table_new} 
            ({config['id_col']}, {', '.join(campos_texto)}, data_atualizacao)
        VALUES 
            ('{id_nd}', {', '.join(valores_texto)}, NOW());  -- Note as aspas em '{id_nd}'
        """
        cursor.execute(sql_vacina)
        if cursor.rowcount > 0:
            print(f"   💉 Vacina aplicada: Registro 'ND' (ID {id_nd}) criado com sucesso.")
        else:
            print(f"   🛡️ Vacina: Registro 'ND' já existia.")
            
        conn.commit()
        # ----------------------------------------

        # Índices
        if len(config["colunas_origem"]) > 0:
            col_nome = config.get("renames", {}).get(config["colunas_origem"][0], config["colunas_origem"][0])
            try:
                cursor.execute(f"CREATE INDEX idx_busca_{col_nome} ON {table_new} (`{col_nome}`(30))")
            except Exception: pass

        # Swap
        cursor.execute(f"SHOW TABLES LIKE '{table_prod}'")
        if cursor.fetchone():
            cursor.execute(f"RENAME TABLE {table_prod} TO {table_old}, {table_new} TO {table_prod}")
        else:
            cursor.execute(f"RENAME TABLE {table_new} TO {table_prod}")
            
        cursor.execute(f"DROP TABLE IF EXISTS {table_old}")
        conn.commit()
        print(f"   ✅ {table_prod} atualizada!")

    except Exception as e:
        print(f"   ❌ Erro no MariaDB: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def limpar_temp(CSV_PATH):
    if CSV_PATH and os.path.exists(CSV_PATH):
        try: os.remove(CSV_PATH)
        except Exception: pass

if __name__ == "__main__":
    print("🚀 Iniciando Pipeline Dimensões (Modo DISTINCT + VARCHAR(16) + VACINA)...")
    for dim in DIMENSOES_CONFIG:
        caminho = duckdb_csv(dim)
        if caminho:
            csv_mariadb(dim, caminho)
            limpar_temp(caminho)
    print("\n🏁 Fim.")