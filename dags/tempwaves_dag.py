from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'tempwaves_pipeline',
    default_args=default_args,
    description='Daily pipeline for temperature wave detection',
    schedule_interval='0 0 * * *',  # Runs at midnight UTC
    catchup=False,
)

# Define Tasks
download_data = BashOperator(
    task_id='download_data',
    bash_command='python /app/scripts/fetch/download_data.py',
    dag=dag,
)

clean_data = BashOperator(
    task_id='clean_data',
    bash_command='python /app/scripts/process/clean_data.py',
    dag=dag,
)

detect_waves = BashOperator(
    task_id='detect_waves',
    bash_command='python /app/scripts/analysis/detect_waves.py',
    dag=dag,
)

restart_api = BashOperator(
    task_id='restart_api',
    bash_command='pkill -f "uvicorn" || true && uvicorn scripts.api.app:app --host 0.0.0.0 --port 8000 &',
    dag=dag,
)

# Task Dependencies
download_data >> clean_data >> detect_waves >> restart_api
