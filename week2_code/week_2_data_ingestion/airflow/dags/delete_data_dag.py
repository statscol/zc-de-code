import os 
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from datetime import datetime

dataflow=DAG(
    'delete_files_unused',
    description='none',
    max_active_runs=1,
    schedule_interval="0 6 2 * *",
    start_date= days_ago(1)
)

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")

with dataflow:
    
    delete_task= BashOperator(
        task_id="delete_files",
        bash_command=f'rm -r {AIRFLOW_HOME}/yellow*.csv',
    )

    check_data = BashOperator(
        task_id="check_files",
        bash_command=f"ls {AIRFLOW_HOME} -al",
    )

    delete_task >> check_data    







