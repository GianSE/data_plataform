import duckdb
import os
import sys
from _settings.config import DUCKDB_SECRET_SQL, setup_minio_env

# Ajuste de PATH para encontrar módulos pai (config, utils)
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)


# 1. Configura ambiente MinIO
setup_minio_env()

# Definições de Caminho
# O asterisco duplo (**) faz o DuckDB ler recursivamente todas as pastas de dia da Bronze
BRONZE_PATH = "s3://bronze/compras-acode/**/*.parquet"
SILVER_PATH = "s3://silver/silver_acode_compras_produto_comercial/"

def processar_silver():
    print("🚀 Iniciando Consolidação Silver (Agregado + Partição por Ano)...")
    
    con = duckdb.connect()
    try:
        # Configura acesso ao S3/MinIO
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(DUCKDB_SECRET_SQL)

        # Lógica SQL (Exatamente a sua, adaptada para DuckDB)
        # Adicionei a coluna 'ano' extraída da data_emissao para usar no particionamento
        query = f"""
        COPY (
            SELECT 
                EAN,
                Loja_CNPJ,
                -- Converte '202502' (VARCHAR) direto para DATE (2025-02-01)
                strptime(Ano_Mes, '%Y%m')::DATE AS Ano_Mes, 
                
                -- Coluna virtual (HIVE) para particionamento físico
                ano, 
                
                -- Granularidade de Atributos (Detalhamento total)
                Produto,
                Fabricante,
                MAX(Fornecedor) AS Fornecedor,
                LPAD(CAST(Fornecedor_CNPJ AS VARCHAR), 14, '0') AS Fornecedor_CNPJ,
                Grupo,
                Sub_Classe,
                Desc_Marca,
                
                -- Métricas Consolidadas (Soma do mês)
                SUM(ACODE_Val_Total) AS ACODE_Val_Total,
                SUM(Qtd_Trib)        AS Qtd_Trib,
                
                -- Auditoria
                now() AS data_atualizacao

            FROM read_parquet('{BRONZE_PATH}', hive_partitioning = true)
            -- Filtro de 24 meses para performance
            WHERE data_emissao >= (date_trunc('month', current_date) - INTERVAL 24 MONTH)
            
            GROUP BY 
                EAN,
                Loja_CNPJ,
                Ano_Mes,
                ano,
                Produto,
                Fabricante,
                Fornecedor_CNPJ,
                Grupo,
                Sub_Classe,
                Desc_Marca
        ) TO '{SILVER_PATH}' (
            FORMAT PARQUET, 
            PARTITION_BY (ano), 
            OVERWRITE_OR_IGNORE 1, 
            COMPRESSION 'SNAPPY'
        );
        """
        
        print("🦆 DuckDB: Lendo Bronze, Agregando e Escrevendo na Silver...")
        con.execute(query)
        
        print(f"✅ Sucesso! Dados salvos em: {SILVER_PATH}")
        print("   📂 Estrutura criada: /ano=2024/arquivo.parquet, /ano=2025/arquivo.parquet...")

    except Exception as e:
        print(f"❌ Erro ao processar Silver: {e}")
        sys.exit(1)
    finally:
        con.close()

if __name__ == "__main__":
    processar_silver()