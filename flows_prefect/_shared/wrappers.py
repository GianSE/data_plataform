import subprocess
import prefect
from prefect import task

@task(retries=3, task_run_name="Python: {script_name}")
def python_task(script_name: str, python_base_path: str):
    # 1. Pega o logger oficial do Prefect para esta execução
    logger = prefect.get_run_logger() 
    script_full_path = f"{python_base_path}/{script_name}.py"
    
    # 2. Abre o processo e lê cada linha assim que ela é impressa
    with subprocess.Popen(
        ["python", "-u", script_full_path], # '-u' evita que o log fique preso na memória
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    ) as proc:
        for line in proc.stdout:
            # 3. Envia a linha para a aba de Logs da UI
            logger.info(line.strip())
            # 4. Também mantém a visualização no teu terminal
            print(line.strip())

    if proc.returncode != 0:
        raise Exception(f"Erro no script {script_name}")