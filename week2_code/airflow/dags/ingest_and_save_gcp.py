import os 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

import pyarrow.csv as pv
import pyarrow.parquet as pq

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


workflow=DAG(
    'download_and_save_to_gcs',
    description='Download data from NYC and save it to gcs',
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    max_active_runs=3,
    tags=['download','taxi','yellow','gcp'],
)

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")

TABLE_NAME="yellow_nyc_taxi"
BASE_URL='https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_'
URL_FILE=BASE_URL+"{{ execution_date.strftime(\'%Y-%m\') }}.csv"
FILE_OUT=AIRFLOW_HOME+"/yellow_data_{{ execution_date.strftime(\'%Y-%m\') }}.csv"

with workflow:

    download_task= BashOperator(
        task_id="download_file",
        bash_command=f"curl -sSL {URL_FILE} -o {FILE_OUT}"
    )

    parquetize_file=PythonOperator(
        task_id="parquet_file",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file":FILE_OUT           
        }
        )
    
    save_to_gcp=LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=FILE_OUT.replace(".csv",".parquet"),
        dst="raw/",
        bucket="dtc_data_lake_de-dataeng"
    )


    download_task >> parquetize_file >> save_to_gcp











