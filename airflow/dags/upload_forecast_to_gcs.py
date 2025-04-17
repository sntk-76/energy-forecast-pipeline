import os
from airflow.providers.google.cloud.transfers.local_to_gcs import  LocalFilesystemToGCSOperator
from datetime import datetime
from airflow import DAG

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/opt/airflow/keys/gcp-sa-key.json'

default_args = {
    
    'owner':'airflow',
    'start_date':datetime(2024, 1, 1),
    'retries' : 1
}

with DAG(
    dag_id='upload_forecast_data',
    description='uploading the predicted data to the GCP bucket in forecast data folder',
    default_args=default_args,
    schedule_interval=None,
) as dag :
    upload_file = LocalFilesystemToGCSOperator(
        task_id = 'upload_forecast_data',
        src='/opt/airflow/data/energy_transformed_forecast_data.csv',
        dst='forecast/T_energy_forecast.csv',
        bucket='energy-forecast-pipeline_bucket'
    )

upload_file    
