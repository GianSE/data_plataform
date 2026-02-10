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

# 1. Configura ambiente MinIO
setup_minio_env()

BRONZE_PATH = "s3://bronze/compras-acode/**/*.parquet"
SILVER_ROOT_PATH = "s3://silver/silver_acode_compras_produto_comercial"

def processar_silver_em_lotes():
    total_start_time = time.time()
    
    # Define os anos que queremos processar (Atual e Anterior)
    ano_atual = datetime.now().year
    # Lista de anos: [2023, 2024, 2025...] - Ajuste o range conforme sua regra de 24 meses
    anos_para_processar = [ano_atual - 2, ano_atual - 1, ano_atual] 
    
    print(f"🚀 [INIT] Iniciando processamento em lotes para os anos: {anos_para_processar}")

    con = duckdb.connect()
    try:
        # --- CONFIGURAÇÃO GLOBAL ---
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(DUCKDB_SECRET_SQL)
        con.execute("SET preserve_insertion_order=false;")
        
        # Opcional: Forçar uso de disco se a memória apertar
        # con.execute("SET temp_directory='/tmp/duckdb_temp.tmp';") 

        # --- MAPEAR ARQUIVOS (VIEW) ---
        # Criamos a view uma única vez apontando para tudo
        # --- CORREÇÃO: USAR 'REPLACE' PARA FORÇAR TIPAGEM ---
        print(f"🔍 [STEP 1] Mapeando arquivos da Bronze (Com Cast via SQL)...")
        con.execute(f"""
            CREATE OR REPLACE VIEW bronze_view AS 
            SELECT * REPLACE (
                -- Força Qtd_Trib a ser BIGINT (aceita negativos)
                CAST(Qtd_Trib AS BIGINT) AS Qtd_Trib,
                
                -- Força Valor a ser DOUBLE (previne erros de decimal)
                CAST(ACODE_Val_Total AS DOUBLE) AS ACODE_Val_Total
            )
            FROM read_parquet('{BRONZE_PATH}', 
                hive_partitioning = true,
                union_by_name = true
            );
        """)

        # --- LOOP DE PROCESSAMENTO ---
        for ano_corrente in anos_para_processar:
            print(f"\n🔄 --- Processando ANO: {ano_corrente} ---")
            loop_start = time.time()

            # 1. Transformação Filtrada (Carrega só 1 ano em memória)
            print(f"   🛠️ Agregando dados de {ano_corrente}...")
            
            # Note o WHERE ano = {ano_corrente}
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE silver_batch AS 
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
                    -- Não precisamos selecionar a coluna 'ano' aqui se formos usar ela na partição
                    -- mas vamos manter para garantir
                    ano, 
                    SUM(CAST(ACODE_Val_Total AS DECIMAL(18,4))) AS ACODE_Val_Total,
                    SUM(CAST(Qtd_Trib AS BIGINT)) AS Qtd_Trib,
                    now() AS data_atualizacao
                FROM bronze_view
                WHERE ano = {ano_corrente}
                GROUP BY ALL
            """)
            
            qtd_linhas = con.execute("SELECT count(*) FROM silver_batch").fetchone()[0]
            
            if qtd_linhas == 0:
                print(f"   ⚠️ Nenhum dado encontrado para {ano_corrente}. Pulando escrita.")
                continue

            print(f"   ✅ {qtd_linhas:,} linhas geradas.")

            # 2. Escrita Manual na Partição
            # Truque: Escrevemos direto na pasta do ano para não sobrescrever a pasta raiz inteira
            # Removemos a coluna 'ano' do SELECT na hora de gravar, pois ela já está no nome da pasta
            destination_path = f"{SILVER_ROOT_PATH}/ano={ano_corrente}/data_{ano_corrente}.parquet"
            
            print(f"   📤 Gravando em: {destination_path}")
            
            con.execute(f"""
                COPY (SELECT * EXCLUDE(ano) FROM silver_batch) TO '{destination_path}' (
                    FORMAT PARQUET, 
                    COMPRESSION 'SNAPPY'
                );
            """)
            
            # Limpa a tabela temporária para liberar memória para o próximo loop
            con.execute("DROP TABLE silver_batch;")
            
            print(f"   🏁 Ano {ano_corrente} concluído em {time.time() - loop_start:.2f}s")

        # --- FINALIZAÇÃO ---
        total_duration = time.time() - total_start_time
        print(f"\n✅ [FINISHED] Pipeline completo finalizado em {total_duration:.2f}s!")

    except Exception as e:
        print(f"❌ [ERRO] Falha no pipeline: {e}")
        sys.exit(1)
    finally:
        con.close()

if __name__ == "__main__":
    processar_silver_em_lotes()