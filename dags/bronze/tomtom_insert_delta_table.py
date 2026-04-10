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
        import boto3
        import json
        from deltalake import DeltaTable, write_deltalake

        S3_ENDPOINT = "http://seaweedfs:8333"
        AWS_ACCESS_KEY_ID = "admin"
        AWS_SECRET_ACCESS_KEY = "admin"
        
        TEMPORAL_BUCKET = "temporal-landing-zone"
        TEMPORAL_PREFIX = "TOMTOM/tomtom_maps_coordinator"
        PERSISTENT_BUCKET = "persistent-landing-zone"
        PERSISTENT_PREFIX = TEMPORAL_PREFIX
        
        temporal_path = f"s3://{TEMPORAL_BUCKET}/{TEMPORAL_PREFIX}"
        persistent_path = f"s3://{PERSISTENT_BUCKET}/{PERSISTENT_PREFIX}"

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

        response = s3_client.list_objects_v2(Bucket=TEMPORAL_BUCKET, Prefix=TEMPORAL_PREFIX) # prefix es el nombre de la carpeta a la cual acceder para retrieve data.
        if 'Contents' not in response:
            print("No new files found in the Temporal Landing Zone for TOMTOM.")
            return

        all_data = []
        processed_keys = []
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('.csv'):
                file_obj = s3_client.get_object(Bucket=TEMPORAL_BUCKET, Key=key)
                file_content = file_obj['Body'].read().decode('utf-8')
                try:
                    data = pd.read_csv(io.StringIO(file_content), sep=",")
                    all_data.append(data)
                except pd.errors.ParserError:
                    print(f"Error parsing CSV from {key}")

        if not all_data:
            print("No valid CSV data found.")
            return

        df_new = pd.concat(all_data, ignore_index=True)

        try:
            s3_client.create_bucket(Bucket=PERSISTENT_BUCKET)
        except (s3_client.exceptions.BucketAlreadyExists, s3_client.exceptions.BucketAlreadyOwnedByYou):
            print(f"Bucket '{PERSISTENT_BUCKET}' already exists")

        try:

            dt = DeltaTable(persistent_path, storage_options=storage_options)
            (
                dt.merge(
                    source=df_new,
                    predicate="""
                        target.latitude = source.latitude AND
                        target.longitude = source.longitude
                    """,
                    source_alias="source",
                    target_alias="target"
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
            print("Data updated via MERGE successfully.")
            
        except Exception as table_not_found_e:
            print(f"Table not found. Initializing for the first time... (Details: {table_not_found_e})")

            try:
                write_deltalake(
                    persistent_path,
                    df_new,
                    storage_options=storage_options,
                    mode="overwrite"
                )
                print("Delta table initialized successfully.")
            
            except OSError as bucket_not_found_e:
                print(f"Error at creating or accessing bucket {PERSISTENT_BUCKET}: {bucket_not_found_e} ")

    process_temporal_to_persistent()

bcn_trams_congestion_temporal_to_persistent_pipeline()