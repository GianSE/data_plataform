import subprocess
from prefect import task

@task(retries=1, task_run_name="Python: {script_name}")
def python_task(script_name: str, python_base_path: str):
    """Executa scripts Python genéricos"""
    script_full_path = f"{python_base_path}/{script_name}.py"
    result = subprocess.run(["python", script_full_path], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Erro no script {script_name}: {result.stderr}")
    return result.stdout
