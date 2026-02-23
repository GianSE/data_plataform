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
# Para processamento completo, use: range(2023, 2027)

# ANO_INICIO = 2022
# ANO_FIM = 2026

ANO_INICIO = datetime.now().year
ANO_FIM = datetime.now().year

# 1. Configura ambiente MinIO
setup_minio_env()

BRONZE_PATH = "s3://bronze/bronze_acode_compras/**/*.parquet"
SILVER_ROOT_PATH = "s3://silver/silver_acode_sellin_comercial"

def processar_silver_limpo():
    total_start_time = time.time()
    anos_para_processar = list(range(ANO_INICIO, ANO_FIM + 1))

    print(f"🚀 [INIT] Processando Silver de {ANO_INICIO} até {ANO_FIM}")

    con = duckdb.connect()
    try:
        # --- CONFIGURAÇÃO ---
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(DUCKDB_SECRET_SQL)
        con.execute("SET preserve_insertion_order=false;")
        
        # --- LOOP ANUAL ---
        for ano_corrente in anos_para_processar:
            print(f"\n🔄 --- Processando ANO: {ano_corrente} ---")
            loop_start = time.time()

            # Query simplificada: Sem UPPER/TRIM/REGEXP (confiando na Bronze)
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE silver_batch AS 
                SELECT 
                    -- Dimensões Diretas (Aceitando NULLs)
                    EAN, 
                    Produto, 
                    Fabricante, 
                    Grupo, 
                    Sub_Classe, 
                    Desc_Marca,
                    MAX(Fornecedor) AS Fornecedor,
                    Fornecedor_CNPJ,
                    Loja_CNPJ,
                    
                    -- Tratamento de Tempo
                    strptime(Ano_Mes, '%Y%m')::DATE AS Ano_Mes, 
                    ano_hive, 

                    -- Agregações de Métricas
                    SUM(ACODE_Val_Total) AS ACODE_Val_Total,
                    SUM(Qtd_Trib) AS Qtd_Trib,
                    now() AS data_atualizacao
                FROM read_parquet('{BRONZE_PATH}', hive_partitioning=true, union_by_name=true)
                WHERE ano_hive = {ano_corrente}
                GROUP BY ALL
            """)
            
            # Verifica volume gerado
            qtd = con.execute("SELECT count(*) FROM silver_batch").fetchone()[0]
            
            if qtd > 0:
                caminho_final = f"{SILVER_ROOT_PATH}/ano_hive={ano_corrente}/data_{ano_corrente}.parquet"
                print(f"   💾 Gravando {qtd:,} linhas em: {caminho_final}")
                
                con.execute(f"""
                    COPY (SELECT * EXCLUDE(ano_hive) FROM silver_batch) TO '{caminho_final}' (
                        FORMAT PARQUET, 
                        COMPRESSION 'ZSTD',
                        COMPRESSION_LEVEL 3
                    );
                """)
            else:
                print(f"   ⚠️ Sem dados para o ano {ano_corrente}. Pulando.")

            con.execute("DROP TABLE IF EXISTS silver_batch;")
            print(f"   ⏱️ Tempo do ano: {time.time() - loop_start:.2f}s")

        print(f"\n✅ [SUCESSO] Pipeline Silver finalizado em {time.time() - total_start_time:.2f}s")

    except Exception as e:
        print(f"❌ [ERRO] {e}")
        sys.exit(1)
    finally:
        con.close()

if __name__ == "__main__":
    processar_silver_limpo()