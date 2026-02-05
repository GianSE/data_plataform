from prefect import flow
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from flows_prefect._shared.wrappers import python_task
from flows_prefect._shared.deployment import gerenciar_run

PY_PATH = "/app/tasks_python/gold"

@flow(name="Gold Acode Compras Comercial")
def pipeline():
    
    python_task(
        script_name="gold_acode_compras_comercial", 
        python_base_path=PY_PATH
    )

if __name__ == "__main__":
    gerenciar_run(
        pipeline_flow=pipeline,
        entrypoint_name="gold/flow_gold_acode_compras_comercial.py:pipeline",
        deploy_name="Gold Acode Compras Comercial",
        tags=["Mariadb", "Comercial Vermelho"],
        cron_schedule="0 4 * * *"
    )