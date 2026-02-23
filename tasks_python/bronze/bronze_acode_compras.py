import os
import duckdb
from datetime import datetime
from dateutil.relativedelta import relativedelta
from _settings.config import MINIO_CONFIG, setup_minio_env

""" 
--- CONTROLE DE EXECUÇÃO ---
1 - "incremental"   (reescreve o mes atual e os 2 meses anteriores) 
2 - "range"         (reescreve todos os parquet da DATA_INICIO até DATA_FIM)
3 - "pontual"       (reescreve um mês em específico para tratar erros)
"""

MODO = "incremental"  # incremental | pontual | range

# Usado no modo pontual
ANO_PONTUAL = 2023
MES_PONTUAL = 9

# Usado no modo range
DATA_INICIO = datetime(2023, 1, 1)
DATA_FIM = datetime(2023, 12, 1)


# Caminhos dos Buckets
BUCKET_RAW = "raw/acode_compras"
BUCKET_BRONZE = "bronze/bronze_acode_compras"

class AcodeBronzeETL:
    def __init__(self):
        setup_minio_env()
        self.con = duckdb.connect(database=':memory:')
        
        self.con.execute("INSTALL httpfs;")
        self.con.execute("LOAD httpfs;")
        
        self.con.execute(f"""
            SET s3_endpoint='{MINIO_CONFIG["endpoint"]}';
            SET s3_access_key_id='{MINIO_CONFIG["access_key"]}';
            SET s3_secret_access_key='{MINIO_CONFIG["secret_key"]}';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
        """)

    def processar_mes(self, ano_str, mes_str):
        print(f"🔄 Consolidando Mês: {ano_str}/{mes_str} (Raw -> Bronze)...")
        
        path_raw_glob = f"s3://{BUCKET_RAW}/ano_hive={ano_str}/mes_hive={mes_str}/*.parquet"
        path_bronze = f"s3://{BUCKET_BRONZE}/ano_hive={ano_str}/mes_hive={mes_str}/compras_{ano_str}_{mes_str}.parquet"

        try:
            self.con.execute(f"SELECT 1 FROM read_parquet('{path_raw_glob}') LIMIT 1")
        except Exception:
            print(f"   ⚠️ Nenhuma pasta/arquivo na Raw para {ano_str}/{mes_str}. Pulando.")
            return

        query_transformacao = f"""
            COPY (
                SELECT 
                    Loja, 
                    LPAD(Loja_CNPJ, 14, '0') AS Loja_CNPJ, 
                    Razao_Social, Representante, Fornecedor, 
                    LPAD(Fornecedor_CNPJ, 14, '0') AS Fornecedor_CNPJ,
                    
                    Fabricante, Familia, P_Ativo, Sub_Classe, Classe, Ativa, 
                    Cidade, UF, Grupo, Produto, Holding, Tipo, Desc_Marca, 
                    Chave, Unid_Trib, Ano_Mes, Operacao_Descricao, EAN, Bandeira, PBM,

                    TRY_CAST(NF_Numero AS BIGINT) AS NF_Numero,
                    TRY_CAST(CFOP AS BIGINT) AS CFOP,
                    TRY_CAST(item_numero AS BIGINT) AS item_numero,
                    TRY_CAST(idpk AS BIGINT) AS idpk,

                    TRY_CAST(REPLACE(Qtd_Trib, ',', '.') AS DOUBLE) AS Qtd_Trib,
                    TRY_CAST(REPLACE(ACODE_Val_Total, ',', '.') AS DOUBLE) AS ACODE_Val_Total,
                    TRY_CAST(REPLACE(Val_Trib, ',', '.') AS DOUBLE) AS Val_Trib,
                    TRY_CAST(REPLACE(Descontos, ',', '.') AS DOUBLE) AS Descontos,
                    TRY_CAST(REPLACE(ST, ',', '.') AS DOUBLE) AS ST,
                    TRY_CAST(REPLACE(STRet, ',', '.') AS DOUBLE) AS STRet,
                    TRY_CAST(REPLACE(Frete, ',', '.') AS DOUBLE) AS Frete,
                    TRY_CAST(REPLACE(IPI, ',', '.') AS DOUBLE) AS IPI,
                    TRY_CAST(REPLACE(Val_Prod, ',', '.') AS DOUBLE) AS Val_Prod,
                    TRY_CAST(REPLACE(Impostos, ',', '.') AS DOUBLE) AS Impostos,
                    TRY_CAST(REPLACE(Outros, ',', '.') AS DOUBLE) AS Outros,
                    TRY_CAST(REPLACE(Val_Prod_sem_STRet, ',', '.') AS DOUBLE) AS Val_Prod_sem_STRet,

                    TRY_CAST(data_emissao AS DATE) AS data_emissao,
                    TRY_CAST(Ultima_Atualizacao AS TIMESTAMP) AS Ultima_Atualizacao,
                    TRY_CAST(data_proc AS DATE) AS data_proc

                FROM read_parquet('{path_raw_glob}')
            ) TO '{path_bronze}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
        """
        
        try:
            self.con.execute(query_transformacao)
            print(f"   ✅ Mês {ano_str}/{mes_str} consolidado e tipado com sucesso!")
        except Exception as e:
            print(f"   ❌ Erro ao transformar {ano_str}/{mes_str}: {e}")

    def run(self):
        print(f"🚀 Iniciando tipagem Raw -> Bronze (DuckDB)")
        data_atual = datetime.now()

        if MODO == "incremental":
            print("🗓️ Modo INCREMENTAL (Mês atual e 2 anteriores)...")
            # Mês atual
            self.processar_mes(str(data_atual.year), f"{data_atual.month:02d}")
            
            # Mês anterior
            mes_ant = data_atual - relativedelta(months=1)
            self.processar_mes(str(mes_ant.year), f"{mes_ant.month:02d}")

            # Mês retrasado
            mes_ant2 = data_atual - relativedelta(months=2)
            self.processar_mes(str(mes_ant2.year), f"{mes_ant2.month:02d}")

        elif MODO == "pontual":
            print(f"🎯 Modo PONTUAL -> {ANO_PONTUAL}-{MES_PONTUAL:02d}")
            self.processar_mes(str(ANO_PONTUAL), f"{MES_PONTUAL:02d}")

        elif MODO == "range":
            print(f"📏 Modo RANGE -> {DATA_INICIO.strftime('%Y-%m')} até {DATA_FIM.strftime('%Y-%m')}")
            if DATA_INICIO > DATA_FIM:
                raise ValueError("A DATA_INICIO não pode ser maior que a DATA_FIM")

            data_cursor = DATA_INICIO
            while data_cursor <= DATA_FIM:
                self.processar_mes(str(data_cursor.year), f"{data_cursor.month:02d}")
                data_cursor += relativedelta(months=1)

        else:
            raise ValueError(f"Modo '{MODO}' inválido. Escolha 'incremental', 'pontual' ou 'range'.")
            
        print("\n🏁 Processamento Finalizado.")

if __name__ == "__main__":
    etl = AcodeBronzeETL()
    etl.run()