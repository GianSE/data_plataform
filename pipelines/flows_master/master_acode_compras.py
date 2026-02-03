from prefect import flow
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from flows_prefect._shared.deployment import gerenciar_run, rodar_deployment

@flow(name="Master - Acode Compras")
def master_acode_compras():
    print("🎬 [Acode Compras] Iniciando Orquestração Diária...")

    # rodar_deployment(nome_etapa="voce escolhe", deployment_id="@flow(name)/deploy_name")
    
    # PASSO 1: Bronze
    rodar_deployment(nome_etapa="Bronze Compras Acode",deployment_id="Bronze Acode Compras/Pipeline Bronze Acode Compras")

    # PASSO 2: Scripts dependentes da Bronze Acode
    rodar_deployment(nome_etapa="Comercial Sell-In Acode",deployment_id="Pipeline Acode Compras Comercial/Pipeline Acode Compras Comercial")
    
    print("\n🏁 Ciclo Acode Compras concluído com sucesso!")

if __name__ == "__main__":
    gerenciar_run(
        pipeline_flow=master_acode_compras,
        entrypoint_name="master_acode_compras:master_acode_compras",
        deploy_name="Orquestrador Acode Compras",
        cron_schedule="0 4 * * *"
    )