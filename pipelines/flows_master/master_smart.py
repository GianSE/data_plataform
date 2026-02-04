import os
import sys
from prefect import flow
from catalog import FLOWS_CATALOG

current_dir = os.path.dirname(os.path.abspath(__file__))
pipelines_dir = os.path.dirname(current_dir)
sys.path.append(pipelines_dir)

from flows_prefect._shared.deployment import rodar_deployment, gerenciar_run 

@flow(name="Master Flow - Config Driven", retries=3, retry_delay_seconds=60)
def master_smart():
    """
    Orquestrador Mestre: Lê o catálogo e dispara os deployments na ordem correta.
    """
    # Dicionário para guardar os "Recibos" (Futures) das execuções
    execucoes = {}

    print(f"📋 Iniciando Orquestração de {len(FLOWS_CATALOG)} fluxos...")

    for item in FLOWS_CATALOG:
        try:
            nome_id = item["id"]
            deploy = item["deploy_path"]
            dependencias = item["upstream"] 

            # 1. Resolve as dependências
            wait_list = []
            for dep_id in dependencias:
                if dep_id in execucoes:
                    wait_list.append(execucoes[dep_id])
                else:
                    print(f"⚠️ Erro Config: Dependência '{dep_id}' não encontrada ou falhou antes.")

            # 2. Manda rodar (Submit)
            msg_dep = f" (Aguarda: {dependencias})" if dependencias else " (Inicial)"
            print(f"🚀 Agendando: {nome_id}{msg_dep}")
            
            future = rodar_deployment.submit(
                nome_etapa=nome_id,
                deployment_id=deploy,
                wait_for=wait_list 
            )
            
            # 3. Guarda o recibo
            execucoes[nome_id] = future

        except Exception as e:
            print(f"❌ CRÍTICO: Falha ao agendar '{item.get('id', 'DESCONHECIDO')}'. Erro: {e}")
            print("⚠️ Continuando para o próximo fluxo...")
            continue 

if __name__ == "__main__":  
    gerenciar_run(
        pipeline_flow=master_smart,
        entrypoint_name="flow_master/master_smart.py:master_smart",
        deploy_name="Master Flow - Config Driven",
        cron_schedule="0 4 * * *"  # <--- Todo dia às 04:00 da manhã
    )