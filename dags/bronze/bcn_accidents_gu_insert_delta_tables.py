from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models import Variable
import os
import io

default_args = {
    'owner': 'upc',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='bcn_accidents_gu_temporal_to_persistent',
    default_args=default_args,
    description='Batch transformation from raw BCN Accidents GU JSON to Delta Lake using delta-rs',
    schedule=None, # Triggered after the ingestion DAG
    start_date=datetime(2026, 4, 10),
    catchup=False,
    tags=['ajuntament_bcn', 'accidents', 'gu', 'bronze'],
)
def bcn_accidents_gu_temporal_to_persistent_pipeline():

    @task(task_id='process_bcn_accidents_gu_delta_table_task')
    def process_temporal_to_persistent():
        import pandas as pd
        import boto3
        import json
        from deltalake import DeltaTable, write_deltalake
        from DELTA_LAKE.delta_lake import DeltaLake
        from Clients.seaweedfs import Seaweedfs
        AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID", default_var=os.getenv("AWS_ACCESS_KEY_ID"))
        AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY", default_var=os.getenv("AWS_SECRET_ACCESS_KEY"))
        S3_ENDPOINT = Variable.get("S3_ENDPOINT", default_var=os.getenv("S3_ENDPOINT"))
        LANDING_BUCKET = "landing-zone"
        LANDING_PREFIX = "ajuntament_bcn/accidents_gu/"
        TRUSTED_BUCKET = "trusted-zone"
        TRUSTED_PREFIX = LANDING_PREFIX
        persistent_path = f"s3://{TRUSTED_BUCKET}/{TRUSTED_PREFIX}"
        storage_options = {
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "AWS_ENDPOINT_URL": S3_ENDPOINT,
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "AWS_ALLOW_HTTP": "true" 
        }
        predicate = "target.Numero_expedient = source.Numero_expedient"
        s3_client = Seaweedfs()
        s3_client.connect()
        delta_lake = DeltaLake()
        response = s3_client.list_all_objects(LANDING_BUCKET, LANDING_PREFIX)
        
        if not response:
            print("No s'han trobat dades noves per processar.")
            return

        df_new = pd.DataFrame(response)
        cols_float = [
            "Coordenada_UTM_X_ED50",
            "Coordenada_UTM_Y_ED50"
        ]

        for col in cols_float:
            if col in df_new.columns:
                df_new[col] = pd.to_numeric(df_new[col], errors="coerce")

        s3_client.create_bucket(TRUSTED_BUCKET)
        delta_lake.connect_and_load(df_new, persistent_path, storage_options, predicate)

    process_temporal_to_persistent()

bcn_accidents_gu_temporal_to_persistent_pipeline()