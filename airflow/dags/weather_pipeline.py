from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import os

# Define default arguments for the DAG
default_args = {
    "owner": "Julia",
    "start_date": datetime(2024, 1, 1),
    "catchup": False,
}

# Define DAG
dag = DAG(
    "weather_pipeline",
    default_args=default_args,
    schedule="0 6 * * *",  # Run daily
    catchup=False
)

# Define functions to execute scripts
def run_script(script_path):
    """Runs a Python script using subprocess"""
    subprocess.run(["python", script_path], check=True)

# Define Airflow tasks
download_task = PythonOperator(
    task_id="download_data",
    python_callable=run_script,
    op_args=["scripts/fetch/download_data.py"],
    dag=dag
)

clean_task = PythonOperator(
    task_id="clean_data",
    python_callable=run_script,
    op_args=["scripts/process/clean_data.py"],
    dag=dag
)

detect_task = PythonOperator(
    task_id="detect_waves",
    python_callable=run_script,
    op_args=["scripts/analysis/detect_waves.py"],
    dag=dag
)

# Define task dependencies (Run in sequence)
download_task >> clean_task >> detect_task

if __name__ == "__main__":
    dag.test()  # This is optional but helps test your DAG manually
