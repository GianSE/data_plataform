import time
import threading
import pymysql

class DBMonitor:
    def __init__(self, db_config):
        """
        Inicializa o monitor com as configurações de conexão.
        """
        self.db_config = db_config
        self.stop_event = threading.Event()
        self.thread = None

    def _monitor_loop(self, table_name, total_expected_bytes):
        """
        Loop interno que roda em paralelo.
        """
        conn = None
        try:
            # Cria uma conexão separada apenas para monitoramento
            conn = pymysql.connect(**self.db_config)
            cursor = conn.cursor()
            
            print(f"\n📊 Monitor iniciado para tabela: {table_name}")
            
            start_time = time.time()
            
            while not self.stop_event.is_set():
                # Consulta o tamanho atual da tabela
                sql = f"""
                SELECT data_length, table_rows 
                FROM information_schema.tables 
                WHERE table_schema = '{self.db_config['database']}' 
                AND table_name = '{table_name}'
                """
                cursor.execute(sql)
                result = cursor.fetchone()
                
                if result:
                    bytes_loaded = result[0] or 0
                    rows = result[1] or 0
                    
                    mb_loaded = bytes_loaded / (1024 * 1024)
                    
                    # Cálculo de porcentagem (estimado)
                    pct = 0.0
                    if total_expected_bytes > 0:
                        pct = (bytes_loaded / total_expected_bytes) * 100
                        # Trava em 99.9% até terminar de fato
                        if pct > 99.9: pct = 99.9
                    
                    # Velocidade (MB/s)
                    elapsed = time.time() - start_time
                    speed = mb_loaded / elapsed if elapsed > 0 else 0
                    
                    # \r faz o cursor voltar ao inicio da linha (efeito de atualização)
                    print(f"\r⏳ Carregando: {mb_loaded:,.2f} MB ({pct:.1f}%) | Linhas: {rows:,} | Vel: {speed:.1f} MB/s", end="")
                
                time.sleep(2) # Atualiza a cada 2 segundos

        except Exception as e:
            print(f"\n⚠️ Erro no monitor: {e}")
        finally:
            if conn: conn.close()

    def start(self, table_name, total_bytes_csv):
        """
        Inicia a thread de monitoramento.
        """
        self.stop_event.clear()
        self.thread = threading.Thread(
            target=self._monitor_loop, 
            args=(table_name, total_bytes_csv)
        )
        self.thread.start()

    def stop(self):
        """
        Para o monitoramento.
        """
        if self.thread and self.thread.is_alive():
            self.stop_event.set()
            self.thread.join()
            print("\n✅ Monitoramento finalizado.")