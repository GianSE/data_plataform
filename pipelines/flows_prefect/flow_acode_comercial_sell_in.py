from prefect import flow
from _shared.wrappers import python_task
from _shared.deployment import gerenciar_run

PY_PATH = "/app/tasks_python/comercial_sell_in"

@flow(name="Pipeline Acode Compras Comercial")
def pipeline():

    silver = python_task(
        script_name="silver_acode_compras_produto_comercial", 
        python_base_path=PY_PATH
    )
    
    dim = python_task(
        script_name="gold_acode_dimensoes", 
        python_base_path=PY_PATH, 
        wait_for=[silver]
    )
    
    python_task(
        script_name="gold_acode_compras_produto_comercial", 
        python_base_path=PY_PATH, 
        wait_for=[dim]
    )

if __name__ == "__main__":
    gerenciar_run(
        pipeline_flow=pipeline,
        entrypoint_name="flow_comercial_sell_in.py:pipeline",
        deploy_name="Pipeline Acode Compras Comercial",
        cron_schedule=None
    )