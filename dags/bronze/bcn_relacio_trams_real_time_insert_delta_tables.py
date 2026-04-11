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

merge_condition = """
target.Humidity = source.Humidity AND
target.Temperature = source.Temperature AND
target.`Heat Index` = source.`Heat Index` AND
target.`Atmospheric pressure` = source.`Atmospheric pressure` AND
target.Altitude = source.Altitude AND
target.Rain = source.Rain
"""

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

        S3_ENDPOINT = "http://seaweedfs:8333"
        AWS_ACCESS_KEY_ID = "admin"
        AWS_SECRET_ACCESS_KEY = "admin"
        
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

        s3_client = boto3.client(
            's3',
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )

        response = s3_client.list_objects_v2(Bucket=LANDING_BUCKET, Prefix=LANDING_PREFIX) # prefix es el nombre de la carpeta a la cual acceder para retrieve data.
        if 'Contents' not in response:
            print("No new files found in the Landing Zone for BCN Relacio Trams.")
            return

        all_data = []
        processed_keys = []
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('.csv'):
                file_obj = s3_client.get_object(Bucket=LANDING_BUCKET, Key=key)
                file_content = file_obj['Body'].read().decode('utf-8')
                try:
                    data = pd.read_csv(io.StringIO(file_content))
                    all_data.append(data)
                except pd.errors.ParserError:
                    print(f"Error parsing CSV from {key}")

        if not all_data:
            print("No valid CSV data found.")
            return

        df_new = pd.concat(all_data, ignore_index=True)

        try:
            s3_client.create_bucket(Bucket=TRUSTED_BUCKET)
        except (s3_client.exceptions.BucketAlreadyExists, s3_client.exceptions.BucketAlreadyOwnedByYou):
            print(f"Bucket '{TRUSTED_BUCKET}' already exists")

        try:
            write_deltalake(
                persistent_path,
                df_new,
                storage_options=storage_options,
                mode="append"
            )
            print("Data appended successfully.")

        except Exception as e:
            print(f"Table not found. Initializing... ({e})")

            write_deltalake(
                persistent_path,
                df_new,
                storage_options=storage_options,
                mode="overwrite"
            )

    process_temporal_to_persistent()

bcn_relacio_trams_real_time_temporal_to_persistent()