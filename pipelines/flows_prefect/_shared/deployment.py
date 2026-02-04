import sys
from prefect import task  # <--- Importante
from prefect.client.schemas.schedules import CronSchedule
from prefect.deployments import run_deployment

def gerenciar_run(pipeline_flow, entrypoint_name, deploy_name, cron_schedule=None):
    if len(sys.argv) > 1 and sys.argv[1] == "deploy":
        print(f"[DEPLOY] Iniciando: {deploy_name}")
        
        # Prepara a lista de agendamentos (vazia se não houver cron)
        schedules = []
        if cron_schedule:
            schedules.append(CronSchedule(cron=cron_schedule, timezone="America/Sao_Paulo"))

        pipeline_flow.from_source(
            source="/app/pipelines/flows_prefect", 
            entrypoint=entrypoint_name
        ).deploy(
            name=deploy_name,
            work_pool_name="process-pool",
            job_variables={"env": {"PYTHONPATH": "/app:/app/pipelines:/app/tasks_python"}},
            schedules=schedules, # <--- Usa a lista dinâmica
        )
    else:
        print("[TESTE] Executando Flow em modo LOCAL...")
        pipeline_flow()

@task(name="Disparar Deployment", task_run_name="Deploy: {nome_etapa}")
def rodar_deployment(nome_etapa, deployment_id):
    """
    Roda um deployment e cuida dos logs.
    Agora é uma TASK do Prefect, então aceita .submit() e wait_for.
    """
    print(f"\n [RUN] [{nome_etapa}] Iniciando execução...")
    
    try:
        # O run_deployment já espera terminar (bloqueante)
        run_deployment(
            name=deployment_id,
            timeout_seconds=None 
        )
        print(f" [OK] [{nome_etapa}] Finalizado com sucesso!")
        
    except Exception as e:
        print(f" [FAILED] [{nome_etapa}] FALHOU!")
        # É crucial lançar o erro (raise) para a task ficar "Failed" 
        # e impedir que as dependentes (wait_for) rodem.
        raise e