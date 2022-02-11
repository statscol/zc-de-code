import os 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from ingest_data import send_to_postgres

def save_file():
    print("this part now")

workflow=DAG(
    'download_taxi_and_save_data',
    description='Download data from NYC',
    schedule_interval="0 6 2 * *",
    start_date=datetime(2021, 1, 1),
    max_active_runs=2,
    tags=['download','taxi','yellow'],
)

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")

PG_HOST=os.environ.get("PG_HOST")
PG_USER=os.environ.get("PG_USER")
PG_PASSWORD=os.environ.get("PG_PASSWORD")
PG_PORT=os.environ.get("PG_PORT")
PG_DB=os.environ.get("PG_DB")


BASE_URL='https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_'
URL_FILE=BASE_URL+"{{ execution_date.strftime(\'%Y-%m\') }}.csv"
FILE_OUT=AIRFLOW_HOME+"/yellow_data_{{ execution_date.strftime(\'%Y-%m\') }}.csv"

with workflow:

    download_task= BashOperator(
        task_id="download_file",
        bash_command=f"curl -sSL {URL_FILE} -o {FILE_OUT}"
    )

    send_to_pg=PythonOperator(
        task_id="upload_to_pg",
        python_callable=send_to_postgres,
        op_kwargs={

            "filename":FILE_OUT,
            "user":PG_USER,
            "password":PG_PASSWORD,
            "host":PG_HOST,
            "port":PG_PORT,
            "db":PG_DB, 
            "table_name":BASE_URL.split("/")[-1]+"taxi",
            "user":PG_USER
           
        }
        )

    download_task >> send_to_pg    







