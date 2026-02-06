import os
import sys
import polars as pl
from datetime import date, timedelta
from typing import List
from sqlalchemy.engine import URL

from _settings.config import MINIO_CONFIG, setup_minio_env

# Ajuste de PATH
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir) 
sys.path.append(parent_dir)

# Configurações DB
DB_ACODE = {
    "user": "egtec_xml_rd_15",
    "password": "32XD#bdCA5R15dm",
    "host": "db-xml-rd.acode.com.br",
    "port": 3306,
    "database": "acode_master_redes"
}

BUCKET_BRONZE = "s3://bronze/compras-acode"

class AcodeBronzeETL:
    def __init__(self):
        setup_minio_env()
        
        # Cria a URL de forma estruturada e segura
        connection_url = URL.create(
            drivername="mariadb+mariadb",  # Driver oficial
            username=DB_ACODE['user'],
            password=DB_ACODE['password'],
            host=DB_ACODE['host'],
            port=DB_ACODE['port'],
            database=DB_ACODE['database']
        )
        
        # O Polars aceita o objeto URL direto ou convertido para string
        self.db_uri = connection_url  
        
        # Configuração de Storage
        self.storage_options = {
            "key": MINIO_CONFIG["access_key"], 
            "secret": MINIO_CONFIG["secret_key"],
            "endpoint_url": f"http://{MINIO_CONFIG['endpoint']}",
        }

    def verificar_total_s3(self, data_proc) -> int:
        """
        Versão 100% Polars: Usa scan_parquet (Lazy) para contar linhas 
        sem baixar o arquivo inteiro.
        """
        ano, mes, _ = data_proc.split('-')
        s3_path = f"{BUCKET_BRONZE}/ano={ano}/mes={mes}/{data_proc}.parquet"
        
        try:
            # scan_parquet cria um plano preguiçoso (não lê os dados, só metadados)
            # select(pl.len()) é extremamente otimizado em parquet
            q = pl.scan_parquet(
                s3_path, 
                storage_options=self.storage_options
            ).select(pl.len())
            
            # collect() executa a query e retorna o número
            return q.collect().item()
        except Exception:
            # Se o arquivo não existe, retorna 0
            return 0

    def obter_total_esperado(self, data_proc) -> int:
        sql = f"SELECT SUM(Registros) as total FROM si_15_cubo_xml_analitico_diario_totalizador WHERE data_proc = '{data_proc}'"
        try:
            df = pl.read_database(query=sql, connection=self.db_uri)
            total = df["total"].item()
            return int(total) if total is not None else 0
        except Exception:
            return 0

    def obter_datas_com_retroativo(self) -> List[date]:
        print("🔍 Verificando datas na tabela de retroativos...")
        sql = "SELECT DISTINCT data_proc FROM si_15_cubo_xml_analitico_diario_retroativo WHERE data_proc IS NOT NULL"
        try:
            df = pl.read_database(query=sql, connection=self.db_uri)
            return df["data_proc"].to_list()
        except Exception as e:
            print(f"❌ Erro ao buscar retroativos: {e}")
            return []

    def extrair_e_salvar(self, data_proc):
        print(f"⬇️ Substituindo {data_proc} (Overwrite)...")
        
        sql_full = f"""
            SELECT *, 'DIARIO' as origem_sistema FROM si_15_cubo_xml_analitico_diario WHERE data_proc = '{data_proc}'
            UNION ALL
            SELECT *, 'RETROATIVO' as origem_sistema FROM si_15_cubo_xml_analitico_diario_retroativo WHERE data_proc = '{data_proc}'
        """
        
        try:
            df_final = pl.read_database(query=sql_full, connection=self.db_uri)
        except Exception as e:
            print(f"❌ Erro ao ler do MariaDB: {e}")
            return

        if df_final.is_empty():
            print(f"⚠️ {data_proc}: Vazio. Ignorando.")
            return 

        # Definição do Caminho
        ano, mes, _ = data_proc.split('-')
        s3_path = f"{BUCKET_BRONZE}/ano={ano}/mes={mes}/{data_proc}.parquet"

        # Ajustes
        if 'data_emissao' in df_final.columns:
            df_final = df_final.with_columns(pl.col('data_emissao').cast(pl.Date, strict=False))

        if 'data_proc' in df_final.columns:
            df_final = df_final.drop(['data_proc'])

        try:
            # Escrita com as mesmas storage_options definidas no __init__
            df_final.write_parquet(
                s3_path, 
                compression='snappy', 
                use_pyarrow=True, 
                pyarrow_options={"storage_options": self.storage_options}
            )
            print(f"✅ {data_proc} salvo: {s3_path} ({df_final.height} linhas)")
        except Exception as e:
            print(f"❌ Erro ao salvar no MinIO: {e}")

    def run(self):
        print("🚀 Sincronização Bronze (Pure Polars)...")
        
        # Lógica de Loop mantida igual...
        print("📅 [CHECK RECENTE] Verificando últimos 7 dias...")
        datas_recentes = []
        for i in range(1, 8):
            datas_recentes.append(date.today() - timedelta(days=i))
            
        for data_alvo in sorted(datas_recentes, reverse=True):
            str_dia = data_alvo.strftime('%Y-%m-%d')
            qtd_remota = self.obter_total_esperado(str_dia)
            qtd_s3 = self.verificar_total_s3(str_dia) # Agora usa Polars Scan
            
            print(f"   📊 {str_dia} -> MariaDB: {qtd_remota} | S3: {qtd_s3}")

            if qtd_s3 != qtd_remota:
                print(f"   🔄 Divergência em {str_dia}. Atualizando...")
                self.extrair_e_salvar(str_dia)
            else:
                print(f"   👍 {str_dia} OK.")

        # 2. RETROATIVOS
        datas_retro = self.obter_datas_com_retroativo()
        if datas_retro:
            print("\n📅 [CHECK RETROATIVOS]...")
            str_recentes = [d.strftime('%Y-%m-%d') for d in datas_recentes]

            for data_alvo in sorted(datas_retro, reverse=True):
                str_dia = str(data_alvo)
                if str_dia in str_recentes: continue

                qtd_remota = self.obter_total_esperado(str_dia)
                qtd_s3 = self.verificar_total_s3(str_dia)

                print(f"   📊 {str_dia} -> MariaDB: {qtd_remota} | S3: {qtd_s3}")

                if qtd_s3 != qtd_remota:
                    print(f"   🔄 [RETRO] Atualizando {str_dia}...")
                    self.extrair_e_salvar(str_dia)
                else:
                    print(f"   👍 {str_dia} OK.")
        
        print("\n🏁 Fim.")

if __name__ == "__main__":
    etl = AcodeBronzeETL()
    etl.run()