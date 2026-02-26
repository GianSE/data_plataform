import os
import polars as pl
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
DATA_INICIO = datetime(2020, 1, 1)
DATA_FIM = datetime(2026, 12, 1)

# Caminhos dos Buckets
BUCKET_RAW = "raw/acode_compras"
BUCKET_BRONZE = "bronze/bronze_acode_compras"

# Ajuste automático do endpoint para aceitar o formato esperado pelo Polars (boto3)
endpoint = MINIO_CONFIG["endpoint"]
if not endpoint.startswith("http"):
    endpoint = f"http://{endpoint}"

STORAGE_OPTIONS = {
    "endpoint_url": endpoint,
    "aws_access_key_id": MINIO_CONFIG["access_key"],
    "aws_secret_access_key": MINIO_CONFIG["secret_key"],
    "region_name": MINIO_CONFIG.get("region", "us-east-1"),
    "allow_http": "true"
}

class AcodeBronzeETL:
    def __init__(self):
        setup_minio_env() 

    def processar_mes(self, ano_str, mes_str):
        print(f"🔄 Consolidando Mês: {ano_str}/{mes_str} (Raw -> Bronze com Polars)...")
        
        path_raw_glob = f"s3://{BUCKET_RAW}/ano_hive={ano_str}/mes_hive={mes_str}/*.parquet"
        path_bronze = f"s3://{BUCKET_BRONZE}/ano_hive={ano_str}/mes_hive={mes_str}/compras_{ano_str}_{mes_str}.parquet"

        # ==========================================
        # 1. LEITURA LAZY BLINDADA
        # ==========================================
        try:
            q = pl.scan_parquet(
                path_raw_glob, 
                storage_options=STORAGE_OPTIONS,
                missing_columns="insert" 
            )
            # Lê o esquema real que veio do arquivo
            schema_disponivel = q.collect_schema().names()
            
        except Exception as e:
            # Qualquer erro aqui (pasta vazia, 404, NoSuchKey) apenas avisa e pula o mês
            print(f"   ⚠️ Nenhuma pasta/arquivo na Raw para {ano_str}/{mes_str}. Pulando.")
            return

        # ==========================================
        # 2. TRANSFORMAÇÕES DINÂMICAS (ANTI-FALHAS)
        # ==========================================
        transformacoes = []
        
        # Strings (CNPJs)
        if 'Loja_CNPJ' in schema_disponivel:
            transformacoes.append(pl.col('Loja_CNPJ').cast(pl.Utf8).str.zfill(14))
        if 'Fornecedor_CNPJ' in schema_disponivel:
            transformacoes.append(pl.col('Fornecedor_CNPJ').cast(pl.Utf8).str.zfill(14))
            
        # Datas e Timestamps
        if 'data_emissao' in schema_disponivel:
            transformacoes.append(pl.col('data_emissao').cast(pl.Date, strict=False))
        if 'data_proc' in schema_disponivel:
            transformacoes.append(pl.col('data_proc').cast(pl.Date, strict=False))
        if 'Ultima_Atualizacao' in schema_disponivel:
            transformacoes.append(pl.col('Ultima_Atualizacao').cast(pl.Datetime, strict=False))

        # Inteiros
        cols_int = ['NF_Numero', 'CFOP', 'item_numero', 'idpk']
        for c in cols_int:
            if c in schema_disponivel:
                transformacoes.append(pl.col(c).cast(pl.Int32, strict=False))

        # Floats
        cols_float = [
            'Qtd_Trib', 'ACODE_Val_Total', 'Val_Trib', 'Descontos', 'ST', 
            'STRet', 'Frete', 'IPI', 'Val_Prod', 'Impostos', 'Outros', 'Val_Prod_sem_STRet'
        ]
        for c in cols_float:
            if c in schema_disponivel:
                transformacoes.append(
                    pl.col(c).cast(pl.Utf8).str.replace_all(',', '.').cast(pl.Float64, strict=False)
                )

        # ==========================================
        # 3. SELEÇÃO E EXECUÇÃO (STREAMING)
        # ==========================================
        cols_select = [
            'Loja', 'Loja_CNPJ', 'Razao_Social', 'Representante', 'Fornecedor', 
            'Fornecedor_CNPJ', 'Fabricante', 'Familia', 'P_Ativo', 'Sub_Classe', 
            'Classe', 'Ativa', 'Cidade', 'UF', 'Grupo', 'Produto', 'Holding', 
            'Tipo', 'Desc_Marca', 'Chave', 'Unid_Trib', 'Ano_Mes', 'Operacao_Descricao', 
            'EAN', 'Bandeira', 'PBM', 'NF_Numero', 'CFOP', 'item_numero', 'idpk', 
            'Qtd_Trib', 'ACODE_Val_Total', 'Val_Trib', 'Descontos', 'ST', 'STRet', 
            'Frete', 'IPI', 'Val_Prod', 'Impostos', 'Outros', 'Val_Prod_sem_STRet', 
            'data_emissao', 'Ultima_Atualizacao', 'data_proc'
        ]

        try:
            # Filtra apenas as colunas que realmente existem no esquema deste mês
            cols_validadas = [pl.col(c) for c in cols_select if c in schema_disponivel]
            
            # Executa com Streaming protegido
            q = q.with_columns(transformacoes).select(cols_validadas)
            df_final = q.collect(streaming=True)

            if df_final.height > 0:
                df_final.write_parquet(
                    path_bronze, 
                    storage_options=STORAGE_OPTIONS,
                    compression="zstd",
                    compression_level=3,
                    row_group_size=100_000
                )
                print(f"   ✅ Mês {ano_str}/{mes_str} consolidado ({df_final.height:,} linhas) com sucesso!")
            else:
                print(f"   ⚠️ Mês {ano_str}/{mes_str} sem dados resultantes após os filtros.")

        except Exception as e:
            # Se houver erro de tipagem massiva ou arquivos corrompidos mid-stream, não quebra o loop
            print(f"   ❌ Erro durante o processamento de {ano_str}/{mes_str}: {e}")

    def run(self):
        print(f"🚀 Iniciando tipagem Raw -> Bronze (Polars Streaming)")
        data_atual = datetime.now()

        if MODO == "incremental":
            print("🗓️ Modo INCREMENTAL (Mês atual e 2 anteriores)...")
            self.processar_mes(str(data_atual.year), f"{data_atual.month:02d}")
            
            mes_ant = data_atual - relativedelta(months=1)
            self.processar_mes(str(mes_ant.year), f"{mes_ant.month:02d}")

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