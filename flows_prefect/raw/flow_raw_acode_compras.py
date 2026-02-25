from prefect import flow
import os
import sys
from flows_prefect._shared.wrappers import python_task, standard_flow
from flows_prefect._shared.deployment import gerenciar_run

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)


PY_PATH = "/app/tasks_python/raw"

@standard_flow(name="Raw Acode Compras")
def pipeline():

    python_task(
        script_name="raw_acode_compras", 
        python_base_path=PY_PATH
    )

if __name__ == "__main__":
    gerenciar_run(
        pipeline_flow=pipeline,
        entrypoint_name="raw/flow_raw_acode_compras.py:pipeline",
        deploy_name="Raw Acode Compras",
        tags=["MinIO", "Raw"],
        cron_schedule="30 3 * * *"
    )