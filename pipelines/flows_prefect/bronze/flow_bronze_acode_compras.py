from prefect import flow
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from flows_prefect._shared.wrappers import python_task
from flows_prefect._shared.deployment import gerenciar_run

PY_PATH = "/app/tasks_python/bronze/python"

@flow(name="Pipeline Bronze Acode Compras", timeout_seconds=7200)
def pipeline():

    python_task(
        script_name="bronze_acode_compras", 
        python_base_path=PY_PATH
    )

if __name__ == "__main__":
    gerenciar_run(
        pipeline_flow=pipeline,
        entrypoint_name="flow_bronze_acode_compras.py:pipeline",
        deploy_name="Pipeline Bronze Acode Compras"
    )