from prefect import flow
import os
import sys
from flows_prefect._shared.wrappers import python_task, standard_flow
from flows_prefect._shared.deployment import gerenciar_run

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)


PY_PATH = "/app/tasks_python/silver"

@standard_flow(name="Silver Plugpharma Sellout Comercial")
def pipeline():

    python_task(
        script_name="silver_plugpharma_sellout_comercial", 
        python_base_path=PY_PATH
    )

if __name__ == "__main__":
    gerenciar_run(
        pipeline_flow=pipeline,
        entrypoint_name="silver/flow_silver_plugpharma_sellout_comercial.py:pipeline",
        deploy_name="Silver Plugpharma Sellout Comercial",
        tags=["Mariadb", "Fato", "Comercial Vermelho Sellout"],
        cron_schedule="0 5 * * *"
    )