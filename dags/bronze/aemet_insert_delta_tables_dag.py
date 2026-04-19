from airflow.decorators import dag, task
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'upc',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='aemet_temporal_to_persistent',
    default_args=default_args,
    description='Batch transformation from raw AEMET JSON to Delta Lake using delta-rs',
    schedule=None, # Triggered after the ingestion DAG
    start_date=datetime(2026, 3, 28),
    catchup=False,
    tags=['aemet', 'weather', 'bronze'],
)
def aemet_temporal_to_persistent_pipeline():

    @task(task_id='process_aemet_delta_table_task')
    def process_temporal_to_persistent():
        import pandas as pd
        from Clients.seaweedfs import Seaweedfs
        import boto3
        import json
        from deltalake import DeltaTable, write_deltalake
        from DELTA_LAKE.delta_lake import DeltaLake

        LANDING_BUCKET = "landing-zone"
        LANDING_PREFIX = "aemet/"
        persistent_path = "s3://trusted-zone/aemet_predictions/"
        storage_options = {
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "AWS_ENDPOINT_URL": S3_ENDPOINT,
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "AWS_ALLOW_HTTP": "true" 
        }
        predicate = "target.id = source.id AND target.elaborado = source.elaborado"

        s3_client = Seaweedfs()
        s3_client.connect()
        all_data = s3_client.list_all_objects(Bucket=LANDING_BUCKET, Prefix=LANDING_PREFIX)
        df_new = pd.DataFrame(all_data)

        for col in df_new.columns:
            if df_new[col].dtype == 'object':
                df_new[col] = df_new[col].astype(str)
        
        if 'id' not in df_new.columns or 'elaborado' not in df_new.columns:
            raise ValueError("Required keys ('id', 'elaborado') missing from AEMET data.")
        
        delta_lake.coonect_and_load(df_new, persistent_path, storage_options, predicate)

    process_temporal_to_persistent()

aemet_temporal_to_persistent_pipeline()