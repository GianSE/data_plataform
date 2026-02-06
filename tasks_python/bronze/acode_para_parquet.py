import polars as pl
import s3fs
from sqlalchemy import URL
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
    "endpoint": "192.168.21.251:9000", # Tirei o http:// daqui para facilitar no s3fs
    "access_key": "minioadmin",      
    "secret_key": "minioadmin",   
    "bucket": "bronze"
}

def extrair_dados_acode_incremental():
    setup_minio_env()
    
    # URL completa para o Polars/Pandas
    minio_endpoint_full = f"http://{MINIO_CONFIG['endpoint']}"

    # --- NOVO: Configurar Sistema de Arquivos (S3FS) para listar arquivos ---
    fs = s3fs.S3FileSystem(
        key=MINIO_CONFIG["access_key"],
        secret=MINIO_CONFIG["secret_key"],
        client_kwargs={'endpoint_url': minio_endpoint_full},
        use_listings_cache=False
    )

    target_bucket_path = f"{MINIO_CONFIG['bucket']}/compras-acode" # Caminho físico para o s3fs

    # ---------------------------------------------------------
    # PASSO 1: Descobrir o que JÁ EXISTE no MinIO
    # ---------------------------------------------------------
    print("🔍 Verificando arquivos existentes no MinIO...")
    datas_no_minio = set()
    
    try:
        # Procura recursivamente (**) por todos os arquivos .parquet
        arquivos_existentes = fs.glob(f"{target_bucket_path}/**/*.parquet")
        
        for arquivo in arquivos_existentes:
            # arquivo vem como: bronze/compras-acode/ano=2023/mes=10/2023-10-25.parquet
            nome_arquivo = arquivo.split('/')[-1] # Pega '2023-10-25.parquet'
            data_str = nome_arquivo.replace('.parquet', '')
            datas_no_minio.add(data_str)
            
        print(f"📦 Encontrados {len(datas_no_minio)} dias já processados no MinIO.")
    except Exception as e:
        print(f"⚠️ Bucket ainda vazio ou erro ao listar: {e}")
        # Se der erro (ex: bucket não existe), assume que não tem nada
        datas_no_minio = set()

    # ---------------------------------------------------------
    # PASSO 2: Consultar datas disponíveis no Banco
    # ---------------------------------------------------------
    connection_url = URL.create(
        drivername="mariadb+mariadb", 
        username=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        database=DB_CONFIG['database']
    )

    print("🔍 Consultando datas disponíveis no MariaDB...")
    try:
        q_datas = "SELECT DISTINCT CAST(data_proc AS DATE) as dt FROM dw_tb_acode_temp WHERE data_proc IS NOT NULL ORDER BY dt"
        df_datas = pl.read_database(query=q_datas, connection=connection_url)
        # Convertemos para string (YYYY-MM-DD) para comparar com o MinIO
        datas_banco = [str(d) for d in df_datas["dt"].to_list()]
    except Exception as e:
        print(f"❌ Erro ao conectar no banco: {e}")
        return

    # ---------------------------------------------------------
    # PASSO 3: O Pulo do Gato (A Diferença)
    # ---------------------------------------------------------
    # Convertemos para SET para fazer operação matemática de conjuntos
    set_banco = set(datas_banco)
    
    # O que tem no banco MENOS o que tem no minio = O que falta processar
    datas_para_processar = sorted(list(set_banco - datas_no_minio))

    if not datas_para_processar:
        print("✅ Data Lake já está atualizado! Nenhuma data nova para baixar.")
        return

    print(f"🚀 Iniciando carga incremental: {len(datas_para_processar)} novos dias encontrados.")

    # Config do MinIO para o Polars (escrita)
    storage_options = {
        "key": MINIO_CONFIG["access_key"],
        "secret": MINIO_CONFIG["secret_key"],
        "endpoint_url": minio_endpoint_full,
    }
    
    full_s3_path_prefix = f"s3://{MINIO_CONFIG['bucket']}/compras-acode"

    # ---------------------------------------------------------
    # PASSO 4: Processar apenas o Delta
    # ---------------------------------------------------------
    for str_data in datas_para_processar:
        print(f"⏳ Processando: {str_data} ...")
        
        ano, mes, _ = str_data.split('-')
        s3_path = f"{full_s3_path_prefix}/ano={ano}/mes={mes}/{str_data}.parquet"

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
            df_dia = pl.read_database(query=query_dia, connection=connection_url)
            
            if not df_dia.is_empty():
                df_dia.write_parquet(
                    s3_path, 
                    compression="snappy",
                    use_pyarrow=True,
                    pyarrow_options={"storage_options": storage_options}
                )
                print(f"   -> Salvo no MinIO ({df_dia.height} linhas)")
            else:
                print(f"   -> Dia vazio (ignorado).")
                
        except Exception as e:
            print(f"❌ Erro ao processar dia {str_data}: {e}")

    print("\n🏁 Sincronização incremental finalizada!")

if __name__ == "__main__":
    extrair_dados_acode_incremental()