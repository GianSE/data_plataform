import duckdb
import os
import sys
import time

# Ajuste de PATH
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from _settings.config import DUCKDB_SECRET_SQL, setup_minio_env

# 1. Configura ambiente MinIO
setup_minio_env()

BRONZE_PATH = "s3://bronze/compras-acode/**/*.parquet"
SILVER_PATH = "s3://silver/silver_acode_compras_produto_comercial/"

def processar_silver():
    start_time = time.time()
    print(f"🚀 [STEP 1] Iniciando conexão DuckDB...")
    
    con = duckdb.connect()
    try:
        # --- CONFIGURAÇÃO ---
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(DUCKDB_SECRET_SQL)
        con.execute("SET preserve_insertion_order=false;") # Aumenta performance de escrita

        # --- PASSO 1: MAPEAMENTO ---
        print(f"🔍 [STEP 2] Mapeando arquivos na Bronze...")
        # Criamos uma View para não carregar em memória ainda, mas permitir contagem
        con.execute(f"CREATE OR REPLACE VIEW bronze_view AS SELECT * FROM read_parquet('{BRONZE_PATH}', hive_partitioning = true);")
        
        total_bronze = con.execute("SELECT count(*) FROM bronze_view").fetchone()[0]
        print(f"   📊 Total de registros brutos na Bronze (24 meses): {total_bronze:,}")

        # --- PASSO 2: TRANSFORMAÇÃO EM MEMÓRIA ---

        print(f"🛠️ [STEP 3] Executando agregação e limpeza (In-Memory)...")
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE silver_staged AS 
            SELECT 
                -- 1. Limpeza de Chaves (Para evitar duplicidade por sujeira)
                UPPER(TRIM(COALESCE(EAN, 'ND'))) AS EAN,
                UPPER(TRIM(COALESCE(Produto, 'ND'))) AS Produto,
                UPPER(TRIM(COALESCE(Fabricante, 'ND'))) AS Fabricante,
                UPPER(TRIM(COALESCE(Grupo, 'ND'))) AS Grupo,
                UPPER(TRIM(COALESCE(Sub_Classe, 'ND'))) AS Sub_Classe,
                UPPER(TRIM(COALESCE(Desc_Marca, 'ND'))) AS Desc_Marca,

                -- 2. Tratamento do Fornecedor (Nome)
                -- O ideal seria pegar o nome mais frequente (mode), mas MAX resolve se padronizar
                MAX(UPPER(TRIM(COALESCE(Fornecedor, 'ND')))) AS Fornecedor,
                
                -- 3. Tratamento do CNPJ (Garante 14 dígitos)
                LPAD(CAST(regexp_replace(Fornecedor_CNPJ, '[^0-9]', '', 'g') AS BIGINT)::VARCHAR, 14, '0') AS Fornecedor_CNPJ,
                Loja_CNPJ,
                
                -- 4. Datas e Métricas
                strptime(Ano_Mes, '%Y%m')::DATE AS Ano_Mes, 
                ano, 
                SUM(CAST(ACODE_Val_Total AS DECIMAL(18,4))) AS ACODE_Val_Total,
                SUM(CAST(Qtd_Trib AS BIGINT)) AS Qtd_Trib,
                
                now() AS data_atualizacao
            FROM bronze_view
            WHERE data_emissao >= (date_trunc('month', current_date) - INTERVAL 24 MONTH)
            GROUP BY ALL
        """)

        total_silver = con.execute("SELECT count(*) FROM silver_staged").fetchone()[0]
        print(f"   ✅ Agregação concluída: {total_silver:,} linhas consolidadas.")

        # --- PASSO 3: ESCRITA NO S3 ---
        print(f"📤 [STEP 4] Gravando na Silver (S3) com particionamento...")
        con.execute(f"""
            COPY silver_staged TO '{SILVER_PATH}' (
                FORMAT PARQUET, 
                PARTITION_BY (ano), 
                OVERWRITE_OR_IGNORE 1, 
                COMPRESSION 'SNAPPY'
            );
        """)

        # --- FINALIZAÇÃO ---
        end_time = time.time()
        duration = end_time - start_time
        print(f"🏁 [FINISHED] Silver atualizada com sucesso em {duration:.2f}s!")
        print(f"   📂 Destino: {SILVER_PATH}")

    except Exception as e:
        print(f"❌ [ERRO] Falha no pipeline: {e}")
        sys.exit(1)
    finally:
        con.close()

if __name__ == "__main__":
    processar_silver()