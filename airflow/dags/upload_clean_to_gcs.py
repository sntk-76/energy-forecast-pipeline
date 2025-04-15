import os 
from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import  LocalFilesystemToGCSOperator
from datetime import datetime

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/keys/gcp-sa-key.json'

default_args = {

    'owner':'airflow',
    'start_date':datetime(2024,1,1),
    'retries':1
}

with DAG(
    dag_id='upload_clean_data',
    description='uploading the transformed clean data to the gcs bucket',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    upload_file = LocalFilesystemToGCSOperator(
        task_id='upload_clean_data',
        src='/opt/airflow/data/clean_data.csv',
        dst='clean/clean_data.csv',
        bucket='energy-forecast-pipeline_bucket'
    )

upload_file    