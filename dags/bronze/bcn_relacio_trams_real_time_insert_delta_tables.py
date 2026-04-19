from airflow.decorators import dag, task
from datetime import datetime, timedelta
import os
import io

default_args = {
    'owner': 'upc',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='bcn_relacio_trams_real_time_temporal_to_persistent',
    default_args=default_args,
    description='Batch transformation from raw hf_rain_prediction to Delta Lake using delta-rs',
    schedule=None, # Triggered after the ingestion DAG
    start_date=datetime(2026, 4, 10),
    catchup=False,
    tags=['kaggleHuggingFace', 'bcn_trams_congestion_real_time', 'bronze'],
)
def bcn_relacio_trams_real_time_temporal_to_persistent():

    @task(task_id='bcn_relacio_trams_real_time_temporal_to_persistent')
    def process_temporal_to_persistent():
        import pandas as pd
        import boto3
        import json
        from deltalake import DeltaTable, write_deltalake
        from DELTA_LAKE.delta_lake import DeltaLake
        from Clients.seaweedfs import Seaweedfs
        
        LANDING_BUCKET = "landing-zone"
        LANDING_PREFIX = "ajuntament_bcn/relacio_trams/real_time/"
        TRUSTED_BUCKET = "trusted-zone"
        TRUSTED_PREFIX = LANDING_PREFIX
        temporal_path = f"s3://{LANDING_BUCKET}/{LANDING_PREFIX}"
        persistent_path = f"s3://{TRUSTED_BUCKET}/{TRUSTED_PREFIX}"
        storage_options = {
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "AWS_ENDPOINT_URL": S3_ENDPOINT,
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "AWS_ALLOW_HTTP": "true" 
        }
        s3_client = Seaweedfs()
        s3_client.connect()
        delta_lake = DeltaLake()
        response = s3_client.list_all_objects(Bucket=LANDING_BUCKET, Prefix=LANDING_PREFIX)
        df_new = pd.concat(response, ignore_index=True)
        s3_client.create_bucket(TRUSTED_BUCKET)
        delta_lake.connect_load_specific_one(df_new, persistent_path, storage_options)
        
    process_temporal_to_persistent()

bcn_relacio_trams_real_time_temporal_to_persistent()