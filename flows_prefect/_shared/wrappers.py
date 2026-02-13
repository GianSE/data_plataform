import subprocess
import prefect
from prefect import task, flow

@task(
    retries=1, 
    retry_delay_seconds=60, 
    task_run_name="Python: {script_name}"
)
def python_task(script_name: str, python_base_path: str):
    logger = prefect.get_run_logger() 
    script_full_path = f"{python_base_path}/{script_name}.py"
    
    logger.info(f"🚀 Iniciando execução do script: {script_full_path}")

    with subprocess.Popen(
        ["python", "-u", script_full_path], 
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    ) as proc:
        for line in proc.stdout:
            logger.info(line.strip())
            print(line.strip())

    if proc.returncode != 0:
        # Se houver erro, o Prefect detecta aqui e inicia o retry automaticamente
        logger.error(f"❌ Falha detectada no script {script_name}. Preparando retry...")
        raise Exception(f"Erro no script {script_name}")

def standard_flow(name: str, **kwargs):
    return flow(
        name=name,
        retries=2,
        retry_delay_seconds=60,
        **kwargs
    )