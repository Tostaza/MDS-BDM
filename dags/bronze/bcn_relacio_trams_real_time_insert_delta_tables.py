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
        from DELTA_LAKE.delta_lake import DeltaLake
        from Clients.seaweedfs import Seaweedfs, persistent_delta_uri_for_landing_key
        
        AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID", default_var=os.getenv("AWS_ACCESS_KEY_ID"))
        AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY", default_var=os.getenv("AWS_SECRET_ACCESS_KEY"))
        S3_ENDPOINT = Variable.get("S3_ENDPOINT", default_var=os.getenv("S3_ENDPOINT"))
        LANDING_BUCKET = "landing-zone"
        LANDING_PREFIX = "ajuntament_bcn/relacio_trams/real_time/"
        TRUSTED_BUCKET = "persistent-landing-zone"
        storage_options = {
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "AWS_ENDPOINT_URL": S3_ENDPOINT,
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "AWS_ALLOW_HTTP": "true" 
        }
        predicate = "target.latitud = source.latitud AND target.longitud = source.longitud" # Para hacer el merge.
        s3_client = Seaweedfs()
        s3_client.connect()
        delta_lake = DeltaLake()
        csv_keys = s3_client.list_object_keys(
            Bucket=LANDING_BUCKET,
            Prefix=LANDING_PREFIX,
            suffix=".csv",
        )
        if not csv_keys:
            print("No s'han trobat dades noves per processar.")
            return

        s3_client.create_bucket(TRUSTED_BUCKET)
        for key in csv_keys:
            df_new = s3_client.read_csv_object(Bucket=LANDING_BUCKET, Key=key)
            persistent_path = persistent_delta_uri_for_landing_key(TRUSTED_BUCKET, key)
            delta_lake.connect_and_load(df_new, persistent_path, storage_options, predicate)
            print(f"Relació trams RT: una taula Delta per CSV: {key} → {persistent_path}")
        
    process_temporal_to_persistent()

bcn_relacio_trams_real_time_temporal_to_persistent()