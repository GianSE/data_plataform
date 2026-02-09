import duckdb
import time
import os
from datetime import datetime, timedelta
from _settings.config import setup_minio_env, DUCKDB_SECRET_SQL

# --- CONFIGURAÇÕES ---
setup_minio_env()
S3_BRONZE_ROOT = "s3://bronze/vendas-plugpharma/"
S3_SILVER = "s3://silver/silver_plugpharma_sellout_comercial/"

def processar_silver_por_mes():
    print(f"🚀 [SILVER - MODO PARTICIONADO]: Iniciando processamento mensal...")
    start_total = time.time()
    
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(DUCKDB_SECRET_SQL)
    
    # Tuning para performance dentro do limite de 4GB
    con.execute("SET memory_limit='3GB';")
    con.execute("SET threads=4;") # Agora podemos usar 4 threads porque o volume por vez é menor
    con.execute("SET preserve_insertion_order=false;")

    # 1. Identificar quais anos e meses processar (Últimos 2 anos)
    # Aqui filtramos para não ler a Bronze inteira desnecessariamente
    data_corte = (datetime.now() - timedelta(days=730))
    
    # Gerar lista de meses para processar
    meses_para_processar = []
    current_date = data_corte
    while current_date <= datetime.now():
        meses_para_processar.append({
            'ano': current_date.year,
            'mes': f"{current_date.month:02d}",
            'mes_nome_pt': [
                'JANEIRO', 'FEVEREIRO', 'MARÇO', 'ABRIL', 'MAIO', 'JUNHO',
                'JULHO', 'AGOSTO', 'SETEMBRO', 'OUTUBRO', 'NOVEMBRO', 'DEZEMBRO'
            ][current_date.month - 1]
        })
        # Avança para o próximo mês
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)

    # 2. Loop de Processamento
    for item in meses_para_processar:
        ano = item['ano']
        mes = item['mes']
        mes_nome = item['mes_nome_pt']
        
        path_bronze = f"{S3_BRONZE_ROOT}ano={ano}/mes={mes}/**/*.parquet"
        
        print(f"📂 Processando: {mes}/{ano}...", end=" ", flush=True)
        t_inicio_mes = time.time()

        try:
            # Verifica se existem arquivos para este mês
            check_files = con.execute(f"SELECT count(*) FROM glob('{path_bronze}')").fetchone()[0]
            if check_files == 0:
                print("⚠️ (Sem ficheiros, a saltar)")
                continue

            output_path = f"{S3_SILVER}ano={ano}/mes_{mes}.parquet"

            con.execute(f"""
                COPY (
                    SELECT 
                        -- Removi o ano/mes do SELECT pois eles já estarão no caminho da pasta
                        CAST('{ano}-{mes}-01' AS DATE) AS data_venda,
                        lpad(regexp_replace("CNPJ.1", '\\D', '', 'g'), 14, '0') AS cnpj_loja,
                        "Código Interno" AS codigo_interno_produto,
                        SUM(TRY_CAST(NULLIF(trim("Quantidade"), '') AS INT)) AS qtd_vendida,
                        SUM(TRY_CAST(replace(replace(NULLIF(trim("Valor Líquido Total"), ''), '.', ''), ',', '.') AS DECIMAL(15,2))) AS valor_liquido,
                        MAX(now()) AS data_processamento
                    FROM read_parquet('{path_bronze}', union_by_name=True) 
                    WHERE (trim(upper("Mês")) = '{mes_nome}' OR "Mês" IS NULL)
                    GROUP BY 1, 2, 3
                ) TO '{output_path}' (
                    FORMAT PARQUET, 
                    COMPRESSION 'SNAPPY'
                )
            """)
            
            t_fim_mes = time.time() - t_inicio_mes
            print(f"✅ ({t_fim_mes:.2f}s)")

        except Exception as e:
            print(f"❌ Erro ao processar {mes}/{ano}: {e}")

    con.close()
    print(f"🏁 [FINISHED]: Silver completa em {time.time() - start_total:.2f}s")

if __name__ == "__main__":
    processar_silver_por_mes()