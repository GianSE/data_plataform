import os
import sys
import pandas as pd
import duckdb
from datetime import date, timedelta
from typing import List
from sqlalchemy import create_engine
from _settings.config import MINIO_CONFIG, setup_minio_env, DUCKDB_SECRET_SQL

# 1. Ajuste de PATH e Imports locais
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir) 
sys.path.append(parent_dir)


# Configurações conforme seu ambiente na Drogamais
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
        self.con_duck = None

    def get_engine(self):
        user, password = DB_ACODE["user"], DB_ACODE["password"]
        host, port, db = DB_ACODE["host"], DB_ACODE["port"], DB_ACODE["database"]
        conn_str = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"
        return create_engine(conn_str)

    def _init_duckdb(self):
        if not self.con_duck:
            self.con_duck = duckdb.connect()
            self.con_duck.execute("INSTALL httpfs; LOAD httpfs;")
            self.con_duck.execute(DUCKDB_SECRET_SQL)

    def verificar_total_s3(self, data_proc) -> int:
        self._init_duckdb()
        s3_path = f"{BUCKET_BRONZE}/ano=*/mes=*/data_proc={data_proc}/*.parquet"
        try:
            query = f"SELECT COUNT(*) as qtd FROM read_parquet('{s3_path}')"
            return self.con_duck.execute(query).fetchone()[0]
        except Exception:
            return 0

    def obter_total_esperado(self, data_proc) -> int:
        sql = f"SELECT SUM(Registros) as total FROM si_15_cubo_xml_analitico_diario_totalizador WHERE data_proc = '{data_proc}'"
        try:
            engine = self.get_engine()
            res = pd.read_sql(sql, engine)
            return int(res["total"].iloc[0]) if not res.empty and res["total"].iloc[0] is not None else 0
        except Exception:
            return 0

    def obter_datas_com_retroativo(self) -> List[date]:
        print("🔍 Verificando datas na tabela de retroativos...")
        sql = "SELECT DISTINCT data_proc FROM si_15_cubo_xml_analitico_diario_retroativo WHERE data_proc IS NOT NULL"
        try:
            engine = self.get_engine()
            return pd.read_sql(sql, engine)["data_proc"].tolist()
        except Exception as e:
            print(f"❌ Erro ao buscar retroativos: {e}")
            return []

    def extrair_e_salvar(self, data_proc):
        print(f"⬇️ Substituindo {data_proc} (Carga Completa - Overwrite)...")
        
        # 1. Busca SEMPRE o dia completo (Diário + Retroativo)
        # Isso evita duplicidade e garante que o Lake espelhe o MariaDB fielmente
        sql_full = f"""
            SELECT *, 'DIARIO' as origem_sistema FROM si_15_cubo_xml_analitico_diario WHERE data_proc = '{data_proc}'
            UNION ALL
            SELECT *, 'RETROATIVO' as origem_sistema FROM si_15_cubo_xml_analitico_diario_retroativo WHERE data_proc = '{data_proc}'
        """
        
        try:
            df_final = pd.read_sql(sql_full, self.get_engine())
        except Exception as e:
            print(f"❌ Erro ao ler do MariaDB: {e}")
            return

        # 2. Prepara o caminho do S3
        dt_obj = pd.to_datetime(data_proc)
        s3_folder = f"{BUCKET_BRONZE}/ano={dt_obj.year}/mes={dt_obj.month:02d}/data_proc={data_proc}"
        s3_path = f"{s3_folder}/{data_proc}_v1.parquet"

        # 3. Validação de Vazio
        if df_final.empty:
            print(f"⚠️ {data_proc}: Nenhum dado encontrado na origem. Abortando gravação.")
            return 

        # 4. Correção de Tipagem (Para evitar erro pyarrow)
        if 'data_emissao' in df_final.columns:
            df_final['data_emissao'] = pd.to_datetime(df_final['data_emissao'])

        # 5. Salva (Sobrescrevendo o arquivo anterior)
        storage_options = {
            "key": MINIO_CONFIG["access_key"], 
            "secret": MINIO_CONFIG["secret_key"],
            "client_kwargs": {"endpoint_url": f"http://{MINIO_CONFIG['endpoint']}"}
        }
        
        # Remove a coluna de partição se ela existir no DF
        if 'data_proc' in df_final.columns:
            df_final = df_final.drop(columns=['data_proc'])
            
        df_final.to_parquet(s3_path, index=False, storage_options=storage_options, compression='snappy')
        print(f"✅ {data_proc} corrigido e sincronizado no Lake ({len(df_final)} registros).")
    def run(self):
        print("🚀 Iniciando Sincronização Bronze (Drogamais)...")
        
        # 1. LOOP DE SEGURANÇA - ÚLTIMOS 7 DIAS
        # Isso garante que se o diário for D-1, D-2 ou até D-7, o script captura
        print("📅 [CHECK RECENTE] Verificando integridade dos últimos 7 dias...")
        
        datas_recentes = []
        for i in range(1, 8):
            data_checa = (date.today() - timedelta(days=i))
            datas_recentes.append(data_checa)
            
        for data_alvo in sorted(datas_recentes, reverse=True):
            str_dia = data_alvo.strftime('%Y-%m-%d')
            
            qtd_remota = self.obter_total_esperado(str_dia)
            qtd_s3 = self.verificar_total_s3(str_dia)
            
            print(f"   📊 Debug {str_dia} -> Remoto (Totalizador): {qtd_remota} | Local (S3): {qtd_s3}")

            if qtd_s3 != qtd_remota:
                print(f"   🔄 Divergência detectada em {str_dia}. Sincronizando...")
                self.extrair_e_salvar(str_dia)
            else:
                print(f"   👍 {str_dia}: Integridade OK.")

        # 2. RETROATIVOS - Verificar datas específicas sinalizadas pela Acode
        # (Aqui pegamos qualquer data, mesmo de 90 dias atrás, que esteja na tabela de retroativo)
        datas_retro = self.obter_datas_com_retroativo()
        
        if not datas_retro:
            print("\n🔍 Nenhuma data adicional encontrada na tabela de retroativos.")
        else:
            # Lista de strings das datas recentes para não processar em duplicidade
            str_recentes = [d.strftime('%Y-%m-%d') for d in datas_recentes]
            
            print("\n📅 [CHECK RETROATIVOS] Analisando datas sinalizadas no banco...")

            for data_alvo in sorted(datas_retro, reverse=True):
                str_dia = data_alvo.strftime('%Y-%m-%d')
                
                # Pula se já foi verificado no loop de 7 dias
                if str_dia in str_recentes:
                    continue

                qtd_remota = self.obter_total_esperado(str_dia)
                qtd_s3 = self.verificar_total_s3(str_dia)

                print(f"   📊 Debug {str_dia} -> Remoto: {qtd_remota} | Local: {qtd_s3}")

                if qtd_s3 != qtd_remota:
                    print(f"   🔄 [RETRO] Divergência em {str_dia}. Atualizando...")
                    self.extrair_e_salvar(str_dia)
                else:
                    print(f"   👍 {str_dia}: Sincronizado.")

        print("\n🏁 Processo finalizado.")

if __name__ == "__main__":
    etl = AcodeBronzeETL()
    etl.run()