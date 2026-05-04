from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models import Variable
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
        from Clients.seaweedfs import Seaweedfs, persistent_delta_uri_for_landing_key
        from DELTA_LAKE.delta_lake import DeltaLake

        AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID", default_var=os.getenv("AWS_ACCESS_KEY_ID"))
        AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY", default_var=os.getenv("AWS_SECRET_ACCESS_KEY"))
        S3_ENDPOINT = Variable.get("S3_ENDPOINT", default_var=os.getenv("S3_ENDPOINT"))
        LANDING_BUCKET = "landing-zone"
        LANDING_PREFIX = "aemet/"
        TRUSTED_BUCKET = "persistent-landing-zone"
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
        delta_lake = DeltaLake()
        json_keys = s3_client.list_object_keys(
            Bucket=LANDING_BUCKET,
            Prefix=LANDING_PREFIX,
            suffix=".json",
        )
        if not json_keys:
            print("No s'han trobat fitxers JSON AEMET a landing.")
            return

        s3_client.create_bucket(TRUSTED_BUCKET)
        for key in json_keys:
            records = s3_client.read_json_records(LANDING_BUCKET, key)
            df_new = pd.DataFrame(records)
            for col in df_new.columns:
                if df_new[col].dtype == 'object':
                    df_new[col] = df_new[col].astype(str)
            if 'id' not in df_new.columns or 'elaborado' not in df_new.columns:
                raise ValueError(
                    f"Falten claus ('id', 'elaborado') al JSON {key!r}."
                )
            persistent_path = persistent_delta_uri_for_landing_key(TRUSTED_BUCKET, key)
            delta_lake.connect_and_load(df_new, persistent_path, storage_options, predicate)
            print(f"AEMET: Delta per fitxer (sense fusionar amb altres): {key} → {persistent_path}")

    process_temporal_to_persistent()

aemet_temporal_to_persistent_pipeline()