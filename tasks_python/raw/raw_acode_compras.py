import os
import sys
import polars as pl
from datetime import date, timedelta
from typing import List, Set
import mariadb
import pyarrow.fs as pafs
import pyarrow.parquet as pq  

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

# IMPORTANTE: Atualizado para o bucket RAW
BUCKET_RAW = "raw/acode_compras" 

class AcodeRawETL: # Mudei o nome da classe para refletir a nova arquitetura
    def __init__(self):
        setup_minio_env()
        
        self.arrow_fs = pafs.S3FileSystem(
            access_key="minioadmin",
            secret_key="minioadmin",
            endpoint_override="192.168.21.251:9000",
            scheme="http"
        )

    def _get_connection(self):
        try:
            return mariadb.connect(
                user=DB_ACODE["user"],
                password=DB_ACODE["password"],
                host=DB_ACODE["host"],
                port=DB_ACODE["port"],
                database=DB_ACODE["database"],
                connect_timeout=10
            )
        except mariadb.Error as e:
            print(f"❌ Erro MariaDB: {e}")
            return None

    def verificar_total_s3(self, data_proc) -> int:
        ano, mes, _ = data_proc.split('-')
        caminho_arquivo = f"{BUCKET_RAW}/ano_hive={ano}/mes_hive={mes}/{data_proc}.parquet"
        
        try:
            info = self.arrow_fs.get_file_info(caminho_arquivo)
            if info.type == pafs.FileType.NotFound:
                return 0

            # Conta as linhas pelo metadado. Funciona mesmo se for tudo VARCHAR!
            meta = pq.read_metadata(caminho_arquivo, filesystem=self.arrow_fs)
            return meta.num_rows

        except Exception as e:
            return 0

    def obter_total_esperado(self, data_proc) -> int:
        sql = f"SELECT SUM(Registros) as total FROM si_15_cubo_xml_analitico_diario_totalizador WHERE CAST(data_proc AS DATE) = '{data_proc}'"
        conn = self._get_connection()
        if not conn: return 0
        try:
            df = pl.read_database(query=sql, connection=conn)
            total = df["total"].item()
            return int(total) if total is not None else 0
        except Exception:
            return 0
        finally:
            conn.close()

    def obter_datas_retroativas_ativas(self) -> Set[str]:
        sql = "SELECT DISTINCT CAST(data_proc AS DATE) as data_proc FROM si_15_cubo_xml_analitico_diario_retroativo WHERE data_proc IS NOT NULL"
        conn = self._get_connection()
        if not conn: return set()
        try:
            df = pl.read_database(query=sql, connection=conn)
            return set(df["data_proc"].dt.strftime("%Y-%m-%d").to_list())
        except Exception:
            return set()
        finally:
            conn.close()

    def extrair_e_salvar(self, data_proc):
        print(f"⬇️ Baixando {data_proc}...")
        conn = self._get_connection()
        if not conn: return

        try:
            sql_retro = f"""
                SELECT *, 'RETROATIVO' as origem_sistema 
                FROM si_15_cubo_xml_analitico_diario_retroativo 
                WHERE CAST(data_proc AS DATE) = '{data_proc}'
            """
            df_final = pl.read_database(query=sql_retro, connection=conn)

            if not df_final.is_empty():
                print(f"   ✨ Fonte: RETROATIVO ({df_final.height} linhas).")
            else:
                sql_diario = f"""
                    SELECT *, 'DIARIO' as origem_sistema 
                    FROM si_15_cubo_xml_analitico_diario 
                    WHERE CAST(data_proc AS DATE) = '{data_proc}'
                """
                df_final = pl.read_database(query=sql_diario, connection=conn)
                
                if not df_final.is_empty():
                    print(f"   📦 Fonte: DIÁRIO ({df_final.height} linhas).")

        except Exception as e:
            print(f"❌ Erro DB: {e}")
            return
        finally:
            conn.close()

        if df_final.is_empty():
            print(f"⚠️ {data_proc}: Vazio no MariaDB.")
            return 

        # Transformação Estrutural RAW
        ano, mes, _ = data_proc.split('-')
        s3_path = f"{BUCKET_RAW}/ano_hive={ano}/mes_hive={mes}/{data_proc}.parquet"

        # 1. Remove 'id' e 'data_proc' (já que a data tá no nome da pasta/arquivo)
        col_remover = [col for col in ['id', 'data_proc'] if col in df_final.columns]
        if col_remover:
            df_final = df_final.drop(col_remover)

        # 2. Converte TUDO para VARCHAR (Protege a ingestão na RAW)
        df_final = df_final.select(pl.all().cast(pl.Utf8))

        # Carga Blindada
        try:
            with self.arrow_fs.open_output_stream(s3_path) as f:
                df_final.write_parquet(
                    f, 
                    compression='zstd', 
                    use_pyarrow=True
                )
            print(f"✅ Salvo: {s3_path}")
        except Exception as e:
            print(f"❌ Erro Salvar S3: {e}")

    def processar_dia(self, str_dia):
        qtd_remota = self.obter_total_esperado(str_dia)
        qtd_s3 = self.verificar_total_s3(str_dia) 
        
        print(f"   📊 {str_dia} -> MariaDB: {qtd_remota} | S3: {qtd_s3}")

        if qtd_s3 != qtd_remota:
            print(f"   🔄 Divergência! Atualizando...")
            self.extrair_e_salvar(str_dia)
        else:
            print(f"   👍 OK.")

    def run(self):
        print("🚀 Sincronização RAW (Landing Zone)...")
        
        # FASE 1
        print("\n📅 [FASE 1] Últimos 7 dias...")
        ultimos_dias = set()
        for i in range(1, 8):
            d = (date.today() - timedelta(days=i)).strftime('%Y-%m-%d')
            ultimos_dias.add(d)
        
        for str_dia in sorted(list(ultimos_dias), reverse=True):
            self.processar_dia(str_dia)

        # FASE 2
        print("\n📅 [FASE 2] Pendências Retroativas...")
        datas_retro = self.obter_datas_retroativas_ativas()
        
        if datas_retro:
            datas_para_processar = datas_retro - ultimos_dias
            if datas_para_processar:
                print(f"   🔍 Processando {len(datas_para_processar)} dias antigos...")
                for str_dia in sorted(list(datas_para_processar), reverse=True):
                    self.processar_dia(str_dia)
            else:
                print("   ℹ️ Retroativos já atualizados.")
        else:
            print("   ℹ️ Sem retroativos.")
        
        print("\n🏁 Fim.")

if __name__ == "__main__":
    etl = AcodeRawETL()
    etl.run()