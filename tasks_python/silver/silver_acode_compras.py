import duckdb
import os
import sys
import time
from datetime import datetime

# Ajuste de PATH
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from _settings.config import DUCKDB_SECRET_SQL, setup_minio_env

# =============================
# 🔧 PARÂMETROS DE PROCESSAMENTO
# =============================
""" RANGE DE PROCESSAMENTO """
# ANO_INICIO = 2023
# ANO_FIM = 2026

""" PARA INCREMENTAL DESCOMENTE AS 2 LINHAS ABAIXO """
ANO_INICIO = datetime.now().year
ANO_FIM = datetime.now().year

# 1. Configura ambiente MinIO
setup_minio_env()

BRONZE_PATH = "s3://bronze/compras-acode/**/*.parquet"
SILVER_ROOT_PATH = "s3://silver/silver_acode_compras"

def processar_silver_limpo():
    total_start_time = time.time()
    
    # Define Janela de Tempo
    # ano_atual = datetime.now().year
    anos_para_processar = list(range(ANO_INICIO, ANO_FIM + 1))

    print(f"🚀 [INIT] Processando de {ANO_INICIO} até {ANO_FIM}")

    con = duckdb.connect()
    try:
        # --- CONFIGURAÇÃO ---
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(DUCKDB_SECRET_SQL)
        con.execute("SET preserve_insertion_order=false;")
        
        # Opcional: Se der erro de memória, descomente abaixo
        # con.execute("SET temp_directory='/tmp/duckdb_temp.tmp';") 

        # --- LOOP MENSAL ---
        for ano_corrente in anos_para_processar:
            print(f"\n🔄 --- Processando ANO: {ano_corrente} ---")
            loop_start = time.time()

            # Aqui está a mágica: Tudo acontece numa query só
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE silver_batch AS 
                
                -- CTE: Define a fonte de dados e corrige o tipo (BIGINT)
                WITH source_data AS (
                    -- 1. Leitura dos Arquivos
                    SELECT * FROM read_parquet('{BRONZE_PATH}', hive_partitioning=true, union_by_name=true)
                    
                    UNION ALL BY NAME
                    
                    -- 2. Linha "Dummy" para forçar DuckDB a aceitar negativos e decimais
                    SELECT 
                        CAST(NULL AS BIGINT) AS Qtd_Trib,        -- Previne Crash de Inteiro
                        CAST(NULL AS DOUBLE) AS ACODE_Val_Total, -- Previne Crash de Decimal
                        NULL AS ano                              -- Garante coluna de filtro
                    WHERE 1=0 -- Remove a linha dummy
                )
                
                -- Agregação Principal
                SELECT 
                    UPPER(TRIM(COALESCE(EAN, 'ND'))) AS EAN,
                    UPPER(TRIM(COALESCE(Produto, 'ND'))) AS Produto,
                    UPPER(TRIM(COALESCE(Fabricante, 'ND'))) AS Fabricante,
                    UPPER(TRIM(COALESCE(Grupo, 'ND'))) AS Grupo,
                    UPPER(TRIM(COALESCE(Sub_Classe, 'ND'))) AS Sub_Classe,
                    UPPER(TRIM(COALESCE(Desc_Marca, 'ND'))) AS Desc_Marca,
                    MAX(UPPER(TRIM(COALESCE(Fornecedor, 'ND')))) AS Fornecedor,
                    LPAD(CAST(regexp_replace(Fornecedor_CNPJ, '[^0-9]', '', 'g') AS BIGINT)::VARCHAR, 14, '0') AS Fornecedor_CNPJ,
                    Loja_CNPJ,
                    strptime(Ano_Mes, '%Y%m')::DATE AS Ano_Mes, 
                    ano, 
                    SUM(CAST(ACODE_Val_Total AS DECIMAL(18,4))) AS ACODE_Val_Total,
                    SUM(CAST(Qtd_Trib AS BIGINT)) AS Qtd_Trib,
                    now() AS data_atualizacao
                FROM source_data
                WHERE ano = {ano_corrente}
                GROUP BY ALL
            """)
            
            # Verifica se gerou dados
            qtd = con.execute("SELECT count(*) FROM silver_batch").fetchone()[0]
            
            if qtd > 0:
                caminho_final = f"{SILVER_ROOT_PATH}/ano={ano_corrente}/data_{ano_corrente}.parquet"
                print(f"   💾 Gravando {qtd:,} linhas em: {caminho_final}")
                
                con.execute(f"""
                    COPY (SELECT * EXCLUDE(ano) FROM silver_batch) TO '{caminho_final}' (
                        FORMAT PARQUET, 
                        COMPRESSION 'ZSTD',
                        COMPRESSION_LEVEL 3,
                        ROW_GROUP_SIZE 100000
                    );
                """)
            else:
                print(f"   ⚠️ Ano vazio. Pulando.")

            con.execute("DROP TABLE silver_batch;")
            print(f"   ⏱️ Tempo: {time.time() - loop_start:.2f}s")

        print(f"\n✅ [SUCESSO] Pipeline finalizado em {time.time() - total_start_time:.2f}s")

    except Exception as e:
        print(f"❌ [ERRO] {e}")
        sys.exit(1)
    finally:
        con.close()

if __name__ == "__main__":
    processar_silver_limpo()