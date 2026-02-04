import sys
from prefect import task  # <--- Importante
from prefect.client.schemas.schedules import CronSchedule
from prefect.deployments import run_deployment

def gerenciar_run(pipeline_flow, entrypoint_name, deploy_name, cron_schedule):
    """
    Gere o deploy com agendamento customizado ou execução local.
    Usa os argumentos da linha de comando: 'python flow.py deploy'
    """
    if len(sys.argv) > 1 and sys.argv[1] == "deploy":
        print(f" Iniciando Deploy: {deploy_name} com Cron: {cron_schedule}")
        
        pipeline_flow.from_source(
            source="/app/pipelines/flows_prefect", 
            entrypoint=entrypoint_name
        ).deploy(
            name=deploy_name,
            work_pool_name="process-pool",
            # Garante que o Python encontre os módulos dentro do container
            job_variables={"env": {"PYTHONPATH": "/app:/app/pipelines:/app/tasks_python"}},
            schedules=[
                CronSchedule(cron=cron_schedule, timezone="America/Sao_Paulo")
            ],
        )
    else:
        print("🧪 Executando Flow em modo de TESTE LOCAL...")
        pipeline_flow()

@task(name="Disparar Deployment", task_run_name="Deploy: {nome_etapa}")
def rodar_deployment(nome_etapa, deployment_id):
    """
    Roda um deployment e cuida dos logs.
    Agora é uma TASK do Prefect, então aceita .submit() e wait_for.
    """
    print(f"\n⬇️  [{nome_etapa}] Iniciando execução...")
    
    try:
        # O run_deployment já espera terminar (bloqueante)
        run_deployment(
            name=deployment_id,
            timeout_seconds=None 
        )
        print(f"✅ [{nome_etapa}] Finalizado com sucesso!")
        
    except Exception as e:
        print(f"❌ [{nome_etapa}] FALHOU!")
        # É crucial lançar o erro (raise) para a task ficar "Failed" 
        # e impedir que as dependentes (wait_for) rodem.
        raise e