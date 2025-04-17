from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import  datetime

default_arg = {
    'owner':'airflow',
    'start_date':datetime(2024,1,1),
    'retries':1
}

with DAG(
    dag_id='transfer_forecast_data_to_bq',
    description='transfer forecast data from gcs bucket to the bq forecast data set',
    schedule_interval=None,
    default_args=default_arg,
    catchup=False
) as dag :
    transfer_data = GCSToBigQueryOperator(
    task_id = 'transfer_forecast_data',
    bucket = 'energy-forecast-pipeline_bucket',
    source_objects = ['forecast/T_energy_forecast.csv'],
    destination_project_dataset_table = 'energy-forecast-pipeline.forcast_data.forecast_data',
    schema_fields = [
        {'name':'ds','type':'TIMESTAMP','mode':'NULLABLE'},
        {'name':'trend','type':'FLOAT','mode':'NULLABLE'},
        {'name':'yhat_lower','type':'FLOAT','mode':'NULLABLE'},
        {'name':'yhat_upper','type':'FLOAT','mode':'NULLABLE'},
        {'name':'trend_lower','type':'FLOAT','mode':'NULLABLE'},
        {'name':'trend_upper','type':'FLOAT','mode':'NULLABLE'},
        {'name':'extra_regressors_multiplicative','type':'FLOAT','mode':'NULLABLE'},
        {'name':'extra_regressors_multiplicative_lower','type':'FLOAT','mode':'NULLABLE'},
        {'name':'extra_regressors_multiplicative_upper','type':'FLOAT','mode':'NULLABLE'},
        {'name':'multiplicative_terms','type':'FLOAT','mode':'NULLABLE'},
        {'name':'multiplicative_terms_lower','type':'FLOAT','mode':'NULLABLE'},
        {'name':'multiplicative_terms_upper','type':'FLOAT','mode':'NULLABLE'},
        {'name':'solar','type':'FLOAT','mode':'NULLABLE'},
        {'name':'solar_lower','type':'FLOAT','mode':'NULLABLE'},
        {'name':'solar_upper','type':'FLOAT','mode':'NULLABLE'},
        {'name':'weekly','type':'FLOAT','mode':'NULLABLE'},
        {'name':'weekly_lower','type':'FLOAT','mode':'NULLABLE'},
        {'name':'weekly_upper','type':'FLOAT','mode':'NULLABLE'},
        {'name':'wind','type':'FLOAT','mode':'NULLABLE'},
        {'name':'wind_lower','type':'FLOAT','mode':'NULLABLE'},
        {'name':'wind_upper','type':'FLOAT','mode':'NULLABLE'},
        {'name':'yearly','type':'FLOAT','mode':'NULLABLE'},
        {'name':'yearly_lower','type':'FLOAT','mode':'NULLABLE'},
        {'name':'yearly_upper','type':'FLOAT','mode':'NULLABLE'},
        {'name':'additive_terms','type':'FLOAT','mode':'NULLABLE'},
        {'name':'additive_terms_lower','type':'FLOAT','mode':'NULLABLE'},
        {'name':'additive_terms_upper','type':'FLOAT','mode':'NULLABLE'},
        {'name':'yhat','type':'FLOAT','mode':'NULLABLE'}

    ],
    skip_leading_rows = 1,
    source_format='CSV',
    write_disposition='WRITE_TRUNCATE',
    autodetect = False,
    )
transfer_data