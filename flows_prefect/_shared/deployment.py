import sys
from prefect import task 
from prefect.client.schemas.schedules import CronSchedule
from prefect.deployments import run_deployment

def gerenciar_run(pipeline_flow, entrypoint_name, deploy_name, cron_schedule=None, tags=None):
    """
    Gerencia se o flow deve ser executado localmente ou deployado no Prefect.
    """
    if len(sys.argv) > 1 and sys.argv[1] == "deploy":
        print(f"[DEPLOY] Iniciando: {deploy_name}")
        
        # Garante que tags seja uma lista, mesmo se vier None
        tags_list = tags if tags else []
        
        # Prepara a lista de agendamentos
        schedules = []
        if cron_schedule:
            schedules.append(CronSchedule(cron=cron_schedule, timezone="America/Sao_Paulo"))

        pipeline_flow.from_source(
            source="/app/flows_prefect", 
            entrypoint=entrypoint_name
        ).deploy(
            name=deploy_name,
            work_pool_name="process-pool",
            job_variables={"env": {"PYTHONPATH": "/app:/app/flows_prefect:/app/tasks_python"}},
            tags=tags_list,
            schedules=schedules,
        )
    else:
        print("[TESTE] Executando Flow em modo LOCAL...")
        pipeline_flow()

@task(name="Disparar Deployment", task_run_name="Deploy: {nome_etapa}")
def rodar_deployment(nome_etapa, deployment_id):
    print(f"\n [RUN] [{nome_etapa}] Iniciando execução...")
    try:
        run_deployment(
            name=deployment_id,
            timeout_seconds=None 
        )
        print(f" [OK] [{nome_etapa}] Finalizado com sucesso!")
    except Exception as e:
        print(f" [FAILED] [{nome_etapa}] FALHOU!")
        raise e