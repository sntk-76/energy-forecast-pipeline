from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import  datetime

default_arg = {
    'owner':'airflow',
    'start_date':datetime(2024,1,1),
    'retries':1
}

with DAG(
    dag_id='transfer_clean_data_to_bq',
    description='transfer clean data from gcs bucket to the bq clean data set',
    schedule_interval=None,
    default_args=default_arg,
    catchup=False
) as dag :
    transfer_data = GCSToBigQueryOperator(
    task_id = 'transfer_data',
    bucket = 'energy-forecast-pipeline_bucket',
    source_objects = ['clean/clean_data.csv'],
    destination_project_dataset_table = 'energy-forecast-pipeline.cleaned_data.clean_data',
    schema_fields = [
        {'name' : 'utc_timestamp','type': 'TIMESTAMP','mode':'NULLABLE'},
        {'name' : 'cet_cest_timestamp','type': 'TIMESTAMP','mode':'NULLABLE'},
        {'name' : 'de_load_actual_entsoe_transparency','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_load_forecast_entsoe_transparency','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_solar_capacity','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_solar_generation_actual','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_solar_profile','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_wind_capacity','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_wind_generation_actual','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_wind_profile','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_wind_offshore_capacity','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_wind_offshore_generation_actual','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_wind_offshore_profile','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_wind_onshore_capacity','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_wind_onshore_generation_actual','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_wind_onshore_profile','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_50hertz_load_actual_entsoe_transparency','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_50hertz_load_forecast_entsoe_transparency','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_50hertz_solar_generation_actual','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_50hertz_wind_generation_actual','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_50hertz_wind_offshore_generation_actual','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_50hertz_wind_onshore_generation_actual','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_amprion_load_actual_entsoe_transparency','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_amprion_load_forecast_entsoe_transparency','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_amprion_solar_generation_actual','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_amprion_wind_onshore_generation_actual','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_tennet_load_actual_entsoe_transparency','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_tennet_load_forecast_entsoe_transparency','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_tennet_solar_generation_actual','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_tennet_wind_generation_actual','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_tennet_wind_offshore_generation_actual','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_tennet_wind_onshore_generation_actual','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_transnetbw_load_actual_entsoe_transparency','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_transnetbw_load_forecast_entsoe_transparency','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_transnetbw_solar_generation_actual','type': 'FLOAT','mode':'NULLABLE'},
        {'name' : 'de_transnetbw_wind_onshore_generation_actual','type': 'FLOAT','mode':'NULLABLE'},
    ],
    skip_leading_rows = 1,
    source_format='CSV',
    write_disposition='WRITE_TRUNCATE',
    autodetect = False,
    )
transfer_data