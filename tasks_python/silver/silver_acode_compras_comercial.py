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
        # Criamos uma tabela temporária para validar o resultado antes de subir para o S3
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE silver_staged AS 
            SELECT 
                EAN,
                Loja_CNPJ,
                strptime(Ano_Mes, '%Y%m')::DATE AS Ano_Mes, 
                ano, 
                Produto,
                Fabricante,
                MAX(Fornecedor) AS Fornecedor,
                LPAD(CAST(CAST(Fornecedor_CNPJ AS BIGINT) AS VARCHAR), 14, '0') AS Fornecedor_CNPJ,
                Grupo,
                Sub_Classe,
                Desc_Marca,
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