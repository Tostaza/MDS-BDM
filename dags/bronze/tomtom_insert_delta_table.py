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
    dag_id='tomtom_temporal_to_persistent',
    default_args=default_args,
    description='Batch transformation from tomtom using delta-rs',
    schedule=None, # Triggered after the ingestion DAG
    start_date=datetime(2026, 4, 10),
    catchup=False,
    tags=['ajuntament_bcn', 'trams_congestion', 'bronze'],
)
def bcn_trams_congestion_temporal_to_persistent_pipeline():

    @task(task_id='tomtom_trams_congestion_delta_table_task')
    def process_temporal_to_persistent():
        import pandas as pd
        from Clients.seaweedfs import Seaweedfs
        from DELTA_LAKE.delta_lake import DeltaLake

        S3_ENDPOINT = Variable.get("S3_ENDPOINT", default_var=os.getenv("S3_ENDPOINT"))
        AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID", default_var=os.getenv("AWS_ACCESS_KEY_ID"))
        AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY", default_var=os.getenv("AWS_SECRET_ACCESS_KEY"))
        
        LANDING_BUCKET = "landing-zone"
        LANDING_PREFIX = "TOMTOM/"
        TRUSTED_BUCKET = "persistent-landing-zone"
        TRUSTED_PREFIX = "TOMTOM/"
        
        persistent_path = f"s3://{TRUSTED_BUCKET}/{TRUSTED_PREFIX}"

        storage_options = {
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "AWS_ENDPOINT_URL": S3_ENDPOINT,
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "AWS_ALLOW_HTTP": "true" 
        }
        
        predicate = "target.point_id = source.point_id"

        s3_client = Seaweedfs()
        s3_client.connect()
        delta_lake = DeltaLake()

        response = s3_client.list_all_objects(LANDING_BUCKET, LANDING_PREFIX)
        
        if not response:
            print("No s'han trobat dades noves per processar.")
            return

        df_new = pd.DataFrame(response)

        s3_client.create_bucket(TRUSTED_BUCKET)

        delta_lake.connect_and_load(df_new, persistent_path, storage_options, predicate)

    process_temporal_to_persistent()

bcn_trams_congestion_temporal_to_persistent_pipeline()