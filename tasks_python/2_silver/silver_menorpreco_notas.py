import polars as pl
import mariadb
import time
import os
import csv
import sys
from _settings.config import DB_CONFIG, get_temp_csv_caminho

# Definição dos caminhos temporários
CSV_TEMP_BRONZE = get_temp_csv_caminho("extracao_bronze_notas_temp.csv")
CSV_TEMP_SILVER = get_temp_csv_caminho("processamento_silver_notas.csv")

def extrair_notas_streaming(conn):
    print(f"📡 [STREAM] Extraindo tabela Notas direto para o disco...")
    start_extracao = time.time()
    
    query = """
        SELECT id_nota, `date`, id_loja, geohash, gtin, descricao, valor, valor_desconto, valor_tabela, data_atualizacao 
        FROM bronze_menorPreco_notas 
        WHERE id_nota IS NOT NULL AND id_nota != '0' AND id_nota != '' AND gtin IS NOT NULL
    """
    
    cursor = conn.cursor(buffered=False)
    cursor.execute(query)
    
    count = 0
    with open(CSV_TEMP_BRONZE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=';')
        
        while True:
            rows = cursor.fetchmany(50000) 
            if not rows:
                break
            writer.writerows(rows)
            count += len(rows)
            if count % 100000 == 0:
                print(f"\r   💾 {count:,} linhas gravadas no disco...", end="")
                sys.stdout.flush()
                
    cursor.close()
    print(f"\n   ✅ Notas extraídas: {count:,} linhas em {round(time.time() - start_extracao, 2)}s")

def executar_etl_polars():
    start_time = time.time()
    print("🚀 Iniciando ETL Anti-Queda (Polars + MariaDB Streaming)...")

    conn = None
    try:
        conn = mariadb.connect(**DB_CONFIG)
        
        # ==========================================
        # 1. EXTRAÇÃO SEGURA
        # ==========================================
        print("\n📥 Extraindo dados menores para a RAM...")
        cidades = pl.read_database("SELECT geohash, cidade_normalizada, microrregiao FROM bronze_cidades WHERE geohash IS NOT NULL", connection=conn)
        
        lojas = pl.read_database("SELECT id_loja, bandeira, latitude, longitude, nome_fantasia, razao_social FROM bronze_menorPreco_lojas", connection=conn)
        lojas = lojas.with_columns(pl.col("id_loja").cast(pl.String))
        
        produtos = pl.read_database("SELECT gtin, id_produto, descricao, fabricante FROM bronze_menorPreco_produtos", connection=conn)
        produtos = produtos.with_columns(pl.col("gtin").cast(pl.String))
        
        extrair_notas_streaming(conn)

        # ==========================================
        # 2. CARREGANDO A TABELA GIGANTE NO POLARS
        # ==========================================
        print("\n⚡ Polars: Lendo as notas do disco...")
        colunas_bronze = ["id_nota", "date", "id_loja", "geohash", "gtin", "descricao", "valor", "valor_desconto", "valor_tabela", "data_atualizacao"]
        
        notas = pl.read_csv(
            CSV_TEMP_BRONZE, 
            separator=";", 
            has_header=False, 
            new_columns=colunas_bronze,
            infer_schema_length=10000, 
            null_values=[""]
        )
        
        notas = notas.with_columns([
            pl.col("gtin").cast(pl.String),
            pl.col("id_loja").cast(pl.String),
            pl.col("geohash").cast(pl.String)
        ])

        # ==========================================
        # 3. PROCESSAMENTO POLARS
        # ==========================================
        print("🗺️ Processando Mapas e Filtros...")
        mapa_geo = cidades.group_by("geohash").agg([
            pl.col("cidade_normalizada").max(),
            pl.col("microrregiao").max()
        ])

        # 🚨 ADICIONADO: Filtro para Panificadora e Açougue (com e sem cedilha)
        lojas_rejeitadas = lojas.filter(
            pl.col("nome_fantasia").str.to_uppercase().str.contains("SUPERMERCADO|MERCEARIA|PANIFICADORA|ACOUGUE|AÇOUGUE") |
            pl.col("razao_social").str.to_uppercase().str.contains("SUPERMERCADO|MERCEARIA|PANIFICADORA|ACOUGUE|AÇOUGUE")
        ).select("id_loja")

        notas_filtradas = notas.join(lojas_rejeitadas, on="id_loja", how="anti")
        notas_filtradas = (
            notas_filtradas
            .join(mapa_geo, left_on="geohash", right_on="geohash", how="left")
            .rename({"cidade_normalizada": "cidade"})
        )

        print("📈 Calculando médias e ranks...")
        notas_filtradas = notas_filtradas.with_columns(pl.col("valor").cast(pl.Float64))
        media_gtin = notas_filtradas.group_by("gtin").agg(pl.col("valor").mean().alias("media_gtin"))
        
        gtins_unicos = notas_filtradas.select("gtin").unique()
        produtos_unicos = (
            produtos.join(gtins_unicos, on="gtin", how="inner")
            .with_columns(
                pl.concat_str([
                    pl.col("gtin"), 
                    pl.col("descricao").fill_null(""), 
                    pl.col("fabricante").fill_null("")
                ], separator=" - ").alias("PRODUTO")
            )
            .group_by("gtin").agg([pl.col("id_produto").max(), pl.col("PRODUTO").max()])
        )

        ranks = (
            notas_filtradas.join(lojas, on="id_loja", how="left")
            .filter(pl.col("bandeira").is_not_null() & ~pl.col("bandeira").is_in(["MERCADO", "OUTRAS BANDEIRAS", "DROGAMAIS"]))
            .group_by(["cidade", "microrregiao", "bandeira"]).agg(pl.len().alias("contagem"))
            .with_columns([
                pl.col("contagem").rank("dense", descending=True).over("cidade").alias("rank_cidade"),
                pl.col("contagem").rank("dense", descending=True).over("microrregiao").alias("rank_microrregiao")
            ])
        )

        # ==========================================
        # 4. CONSOLIDAÇÃO FINAL
        # ==========================================
        print("🧩 Montando DataFrame Silver...")
        df_silver = (
            notas_filtradas
            .join(lojas.with_columns(pl.col("nome_fantasia").fill_null("NÃO ENCONTRADO")), on="id_loja", how="left")
            .join(produtos_unicos.select(["gtin", "PRODUTO"]), on="gtin", how="left")
            .join(media_gtin, on="gtin", how="left")
            .join(ranks.select(["cidade", "microrregiao", "bandeira", "rank_cidade", "rank_microrregiao"]), on=["cidade", "microrregiao", "bandeira"], how="left")
            .with_columns([
                pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False).cast(pl.Date).alias("DateRelacionamento"),
                pl.when(pl.col("media_gtin").is_null()).then(None)
                  .when((pl.col("valor") >= (pl.col("media_gtin") * 1.5)) | (pl.col("valor") <= (pl.col("media_gtin") * 0.5))).then(pl.lit("N"))
                  .otherwise(pl.lit("S")).alias("considerado")
            ])
        )

        colunas_finais = [
            "id_nota", "date", "id_loja", "geohash", "gtin", "valor", "valor_desconto", 
            "valor_tabela", "cidade", "data_atualizacao", "bandeira", "latitude", 
            "longitude", "nome_fantasia", "razao_social", "PRODUTO", "microrregiao", 
            "DateRelacionamento", "rank_cidade", "rank_microrregiao", "considerado"
        ]
        
        print(f"\n📄 Gerando CSV temporário final (Silver): {CSV_TEMP_SILVER}")
        df_silver.select(colunas_finais).write_csv(CSV_TEMP_SILVER, separator=";", include_header=False, null_value="\\N")

        # ==========================================
        # 5. CARGA NO MARIADB (LOAD DATA)
        # ==========================================
        print(f"\n🐬 Iniciando carga no MariaDB (Tabela de Notas)...")
        cursor = conn.cursor()
        
        cursor.execute("TRUNCATE TABLE silver_menorPreco_notas")
        
        cursor.execute("SET unique_checks=0;")
        cursor.execute("SET foreign_key_checks=0;")
        
        sql_load = f"""
            LOAD DATA LOCAL INFILE '{CSV_TEMP_SILVER}'
            REPLACE INTO TABLE silver_menorPreco_notas
            FIELDS TERMINATED BY ';' LINES TERMINATED BY '\\n'
            (id_nota, date, id_loja, geohash, gtin, valor, valor_desconto, valor_tabela, 
             cidade, data_atualizacao, bandeira, latitude, longitude, nome_fantasia, 
             razao_social, PRODUTO, microrregiao, DateRelacionamento, rank_cidade, 
             rank_microrregiao, considerado)
        """
        cursor.execute(sql_load)
        conn.commit()
        
        print(f"✅ ETL concluído com sucesso na tabela de testes em {round(time.time() - start_time, 2)}s!")

    except Exception as e:
        print(f"❌ Erro: {e}")
    finally:
        if conn: conn.close()
        if os.path.exists(CSV_TEMP_BRONZE): os.remove(CSV_TEMP_BRONZE)
        if os.path.exists(CSV_TEMP_SILVER): os.remove(CSV_TEMP_SILVER)

if __name__ == "__main__":
    executar_etl_polars()