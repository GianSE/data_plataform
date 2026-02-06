import polars as pl
import urllib.parse
import s3fs
from sqlalchemy import create_engine, text, URL
from _settings.config import setup_minio_env

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

def extrair_dados_acode_otimizado():
    setup_minio_env()
    
    # URL Segura e Moderna (MariaDB Connector)
    connection_url = URL.create(
        drivername="mariadb+mariadb", 
        username=DB_CONFIG['user'], # Fallback para o hardcoded se precisar
        password=DB_CONFIG['password'],
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        database=DB_CONFIG['database']
    )
    
    # Config do MinIO para o Polars
    storage_options = {
        "key": MINIO_CONFIG["access_key"],
        "secret": MINIO_CONFIG["secret_key"],
        "endpoint_url": f"http://{MINIO_CONFIG['endpoint']}",
    }

    target_bucket = f"s3://{MINIO_CONFIG['bucket']}/compras-acode"

    print("🔍 Consultando datas disponíveis no MariaDB...")
    
    # PASSO 1: Pegar datas (Usando Polars direto, sem criar engine na mão)
    try:
        q_datas = "SELECT DISTINCT CAST(data_proc AS DATE) as dt FROM dw_tb_acode_temp WHERE data_proc IS NOT NULL ORDER BY dt"
        df_datas = pl.read_database(query=q_datas, connection=connection_url)
        datas = df_datas["dt"].to_list()
    except Exception as e:
        print(f"❌ Erro ao conectar no banco: {e}")
        return

    if not datas:
        print("⚠️ Nenhuma data encontrada para processar.")
        return

    print(f"📅 Processando {len(datas)} dias de dados...")

    # PASSO 2: Loop de Extração
    for data in datas:
        # Garante que data é objeto date (se vier datetime)
        str_data = str(data) # YYYY-MM-DD
        ano, mes, _ = str_data.split('-')

        # Caminho Limpo (Hive Style)
        s3_path = f"{target_bucket}/ano={ano}/mes={mes}/{str_data}.parquet"

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
            WHERE CAST(data_proc AS DATE) = '{str_data}'
        """
        
        try:
            # Lê com Polars (usa o driver mariadb rápido)
            df_dia = pl.read_database(query=query_dia, connection=connection_url)
            
            if not df_dia.is_empty():
                # Escreve direto pro S3 (sem abrir 'with fs.open')
                df_dia.write_parquet(
                    s3_path, 
                    compression="snappy",
                    use_pyarrow=True,
                    pyarrow_options={"storage_options": storage_options}
                )
                print(f"✅ Sincronizado: {s3_path} | {df_dia.height} linhas")
            else:
                print(f"⚠️ {str_data}: Dia vazio.")
                
        except Exception as e:
            print(f"❌ Erro ao processar dia {str_data}: {e}")

    print("\n🏁 Bronze atualizada com sucesso!")

if __name__ == "__main__":
    extrair_dados_acode_otimizado()