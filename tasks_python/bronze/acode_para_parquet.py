import polars as pl
import urllib.parse
import s3fs
from sqlalchemy import create_engine, text

# 1. Configurações
DB_CONFIG = {
    "user": "drogamais", 
    "password": "dB$MYSql@2119",
    "host": "10.48.12.20", 
    "port": 3306,
    "database": "drogamais"
}

MINIO_CONFIG = {
    "endpoint": "http://192.168.21.251:9000",
    "access_key": "minioadmin",      
    "secret_key": "minioadmin",   
    "bucket": "bronze"
}

password_safe = urllib.parse.quote_plus(DB_CONFIG["password"])
db_uri = f"mysql+pymysql://{DB_CONFIG['user']}:{password_safe}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

def extrair_dados_acode_otimizado():
    fs = s3fs.S3FileSystem(
        key=MINIO_CONFIG["access_key"],
        secret=MINIO_CONFIG["secret_key"],
        endpoint_url=MINIO_CONFIG["endpoint"]
    )
    
    target_folder = f"{MINIO_CONFIG['bucket']}/compras-acode"
    engine = create_engine(db_uri)

    # PASSO 1: Pegar todas as datas únicas de processamento
    print("🔍 Consultando datas disponíveis no MariaDB...")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT DISTINCT CAST(data_proc AS DATE) as dt FROM dw_tb_acode_temp WHERE data_proc IS NOT NULL ORDER BY dt"))
        datas = [row[0] for row in result]
    
    if not datas:
        print("⚠️ Nenhuma data encontrada para processar.")
        return

    print(f"📅 Processando {len(datas)} dias de dados...")

    # PASSO 2: Iterar por data, lendo e subindo um arquivo por dia dentro da pasta do mês
    for data in datas:
        ano = data.year
        mes = f"{data.month:02d}"
        
        # O arquivo leva o nome da data completa para não sobrescrever outros dias do mesmo mês
        s3_path = f"{target_folder}/ano={ano}/mes={mes}/{data}.parquet"

        # Se o arquivo já existir, podemos pular (opcional, para ser mais rápido em re-execuções)
        # if fs.exists(s3_path): continue

        query_dia = f"""
            SELECT 
                id, Loja, CAST(Loja_CNPJ AS CHAR) AS Loja_CNPJ,
                Razao_Social, Representante, Fornecedor, 
                CAST(Fornecedor_CNPJ AS CHAR) AS Fornecedor_CNPJ,
                Fabricante, Familia, P_Ativo, Sub_Classe, Classe, Ativa, Cidade, UF, Grupo,
                Produto, Holding, Tipo, Desc_Marca, Chave, NF_Numero, Unid_Trib, 
                CAST(Qtd_Trib AS DOUBLE) AS Qtd_Trib,
                Val_Trib, Descontos, ST, STRet, CFOP, Frete, IPI, Ano_Mes, Val_Prod,
                Operacao_Descricao, CAST(ACODE_Val_Total AS DOUBLE) AS ACODE_Val_Total,
                Impostos, Outros, Val_Prod_sem_STRet, EAN, Bandeira, PBM, 
                data_emissao, data_proc, item_numero, idpk, Ultima_Atualizacao
            FROM dw_tb_acode_temp
            WHERE CAST(data_proc AS DATE) = '{data}'
        """
        
        # Lê do banco e já converte
        df_dia = pl.read_database(query=query_dia, connection=engine)
        
        if not df_dia.is_empty():
            with fs.open(s3_path, mode='wb') as f:
                df_dia.write_parquet(f, compression="snappy")
            print(f"✅ Sincronizado: {s3_path} | {df_dia.height} linhas")

    print("\n🏁 Bronze atualizada com sucesso!")

if __name__ == "__main__":
    extrair_dados_acode_otimizado()