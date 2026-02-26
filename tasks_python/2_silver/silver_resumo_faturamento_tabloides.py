import polars as pl
import duckdb
import mariadb
import os
import time
import sys
from _settings.config import DB_CONFIG, DUCKDB_SECRET_SQL, setup_minio_env, get_temp_csv_caminho

# 1. Configura o ambiente (MinIO)
setup_minio_env()

S3_PATH = "s3://bronze/bronze_plugpharma_vendas/**/*.parquet"

# Define o caminho temporário do CSV
CSV_PATH = get_temp_csv_caminho("resumo_faturamento_tabloides.csv")

def executar_etl_hibrido_tabloides():
    start_time = time.time()
    print("🚀 Iniciando Pipeline Híbrido (Zero-Copy: Polars + DuckDB)...")

    # =============================================================
    # 1. POLARS: Extração Ultrarrápida das Dimensões (MariaDB -> RAM)
    # =============================================================
    print("📥 [1/3] Polars: Puxando dimensões do MariaDB para a memória...")
    
    # 1. Criamos o objeto de ligação (connection) diretamente
    conn_leitura = mariadb.connect(**DB_CONFIG)
    
    try:
        # 2. Usamos o pl.read_database normal, passando o objeto 'conn_leitura'
        dim_tabloide = pl.read_database(
            query="SELECT id, nome, data_inicio, data_fim FROM dim_tabloide WHERE data_inicio IS NOT NULL AND data_fim IS NOT NULL", 
            connection=conn_leitura
        )
        
        dim_tabloide_produto = pl.read_database(
            query="SELECT tabloide_id, codigo_barras_normalizado FROM dim_tabloide_produto", 
            connection=conn_leitura
        )
        print(f"   ✅ Dimensões carregadas (Tabloides: {dim_tabloide.height} linhas, Produtos: {dim_tabloide_produto.height} linhas).")
        
    except Exception as e:
        print(f"❌ Erro ao extrair dimensões com Polars: {e}")
        sys.exit(1)
    finally:
        # Fechamos a ligação de leitura para não ficar pendurada enquanto o DuckDB trabalha
        conn_leitura.close()


    # =============================================================
    # 2. DUCKDB: Processamento Massivo (S3 + Memória Polars)
    # =============================================================
    print(f"\n🦆 [2/3] DuckDB: Cruzando Data Lake com a memória e gerando CSV...")
    if os.path.exists(CSV_PATH):
        os.remove(CSV_PATH)

    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(DUCKDB_SECRET_SQL)
        con.execute("SET memory_limit='2GB';")
        con.execute("SET threads=2;")

        # A MÁGICA: O SQL é o seu original. O DuckDB enxerga as variáveis "dim_tabloide" 
        # e "dim_tabloide_produto" do Python e usa direto na query sem copiar dados!
        query_pesada = f"""
        COPY (
            WITH 
            TabloidesUnicos AS (
                SELECT 
                    id AS id_tabloide, 
                    nome AS tabloide, 
                    data_inicio, 
                    data_fim
                FROM dim_tabloide -- Lendo do DataFrame Polars
            ),
            VendasDiariasPorLoja AS (
                SELECT 
                    CAST(data_venda AS DATE) AS dia_venda,
                    cnpj_loja,
                    MAX(razao_social_loja) AS nome_loja,
                    SUM(valor_liquido_total) AS faturamento_total_dia,
                    SUM(qtd_de_produtos) AS unidades_total_dia
                FROM read_parquet('{S3_PATH}')
                GROUP BY CAST(data_venda AS DATE), cnpj_loja
            ),
            FaturamentoGeralPorTabloideELoja AS (
                SELECT 
                    T.id_tabloide,
                    V.cnpj_loja,
                    V.nome_loja,
                    T.tabloide,
                    T.data_inicio,
                    T.data_fim,
                    SUM(V.faturamento_total_dia) AS faturamento_total_geral_periodo,
                    SUM(V.unidades_total_dia) AS unidades_total_geral_periodo
                FROM TabloidesUnicos AS T
                JOIN VendasDiariasPorLoja AS V 
                    ON V.dia_venda BETWEEN T.data_inicio AND T.data_fim
                GROUP BY T.id_tabloide, V.cnpj_loja, V.nome_loja, T.tabloide, T.data_inicio, T.data_fim
            ),
            FaturamentoProdutosTabloidePorLoja AS (
                SELECT 
                    dt.id AS id_tabloide,
                    v.cnpj_loja,
                    dt.nome AS tabloide,
                    dt.data_inicio,
                    dt.data_fim,
                    SUM(v.valor_liquido_total) AS faturamento_produtos_tabloide,
                    SUM(v.qtd_de_produtos) AS unidades_produtos_tabloide
                FROM read_parquet('{S3_PATH}') AS v
                JOIN dim_tabloide_produto AS dtp -- Lendo do DataFrame Polars
                    ON v.codigo_de_barras_normalizado_produto = dtp.codigo_barras_normalizado
                JOIN dim_tabloide AS dt -- Lendo do DataFrame Polars
                    ON dtp.tabloide_id = dt.id
                WHERE CAST(v.data_venda AS DATE) BETWEEN dt.data_inicio AND dt.data_fim
                GROUP BY dt.id, v.cnpj_loja, dt.nome, dt.data_inicio, dt.data_fim
            )
            -- Resultado Final
            SELECT 
                G.id_tabloide,
                G.cnpj_loja,
                G.nome_loja,
                G.tabloide,
                G.data_inicio,
                G.data_fim,
                G.faturamento_total_geral_periodo,
                COALESCE(P.faturamento_produtos_tabloide, 0) AS faturamento_produtos_tabloide,
                G.unidades_total_geral_periodo,
                COALESCE(P.unidades_produtos_tabloide, 0) AS unidades_produtos_tabloide
            FROM FaturamentoGeralPorTabloideELoja AS G
            LEFT JOIN FaturamentoProdutosTabloidePorLoja AS P 
                ON G.id_tabloide = P.id_tabloide AND G.cnpj_loja = P.cnpj_loja
                
        ) TO '{CSV_PATH}' (FORMAT CSV, DELIMITER ';', HEADER FALSE, NULL '\\N');
        """
        
        con.execute(query_pesada)
        tamanho_mb = os.path.getsize(CSV_PATH) / (1024 * 1024)
        print(f"   ✅ CSV gerado com sucesso! Tamanho: {tamanho_mb:.2f} MB")

    except Exception as e:
        print(f"❌ Erro no processamento DuckDB: {e}")
        sys.exit(1)
    finally:
        con.close()


    # =============================================================
    # 3. MARIADB: Carga Bulk (LOAD DATA)
    # =============================================================
    print(f"\n🐬 [3/3] MariaDB: Inserindo dados na tabela final...")
    conn_maria = mariadb.connect(**DB_CONFIG)
    cursor = conn_maria.cursor()

    try:
        # 1. Cria a tabela garantindo a estrutura
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS silver_resumo_faturamento_tabloides (
                id_tabloide INT(11),
                cnpj_loja VARCHAR(14),
                nome_loja VARCHAR(255),
                tabloide VARCHAR(255),
                data_inicio DATE,
                data_fim DATE,
                faturamento_total_geral_periodo DECIMAL(20, 4),
                faturamento_produtos_tabloide DECIMAL(20, 4),
                unidades_total_geral_periodo BIGINT,
                unidades_produtos_tabloide BIGINT,
                PRIMARY KEY (id_tabloide, cnpj_loja, data_inicio)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # 2. Limpa a tabela para a nova carga
        cursor.execute("TRUNCATE TABLE silver_resumo_faturamento_tabloides")

        # 3. Otimizações de Bulk Insert
        cursor.execute("SET unique_checks=0")
        cursor.execute("SET foreign_key_checks=0")
        conn_maria.autocommit = False

        # 4. Carrega o CSV gerado pelo DuckDB
        sql_load = f"""
            LOAD DATA LOCAL INFILE '{CSV_PATH}'
            INTO TABLE silver_resumo_faturamento_tabloides
            FIELDS TERMINATED BY ';' LINES TERMINATED BY '\\n'
            (id_tabloide, cnpj_loja, nome_loja, tabloide, data_inicio, data_fim, 
             faturamento_total_geral_periodo, faturamento_produtos_tabloide, 
             unidades_total_geral_periodo, unidades_produtos_tabloide)
        """
        cursor.execute(sql_load)
        conn_maria.commit()

        # Retorna as proteções
        conn_maria.autocommit = True
        cursor.execute("SET unique_checks=1")
        cursor.execute("SET foreign_key_checks=1")

        print(f"   ✅ Carga no MariaDB finalizada!")
        print(f"\n🎉 Pipeline Híbrido concluído com sucesso em {time.time() - start_time:.2f}s!")

    except Exception as e:
        print(f"❌ Erro na carga do MariaDB: {e}")
        conn_maria.rollback()
    finally:
        conn_maria.close()
        # Limpeza do arquivo temporário
        if os.path.exists(CSV_PATH):
            os.remove(CSV_PATH)

if __name__ == "__main__":
    executar_etl_hibrido_tabloides()