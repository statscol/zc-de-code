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


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")

TABLE_NAME="yellow_nyc_taxi"

workflow_bq=DAG(
    'upload_big_query',
    description='create table in bigquery using parquet files',
    schedule_interval=None,
    start_date=datetime(2022, 2, 1),
    max_active_runs=1,
    tags=['bigquery','taxi','yellow','gcp'],
)

with workflow_bq:

    create_bq_table=GCSToBigQueryOperator(
        task_id='gcs_to_bigquery_taxi',
        bucket='dtc_data_lake_de-dataeng',
        source_format='PARQUET',
        max_bad_records=10,
        source_objects=['raw/yellow_data*.parquet'],
        destination_project_dataset_table=f"yellow_taxi_data.{TABLE_NAME}",
        write_disposition='WRITE_TRUNCATE',
    )

    delete_task= BashOperator(
        task_id="delete_data_files",
        bash_command='rm -rf {}/yellow*.{{{}}} && ls -al'.format(AIRFLOW_HOME,"csv,parquet"),
    )

    create_bq_table  >> delete_task 









