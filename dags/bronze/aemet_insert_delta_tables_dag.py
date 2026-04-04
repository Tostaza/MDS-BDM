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
        import boto3
        import json
        from deltalake import DeltaTable, write_deltalake

        S3_ENDPOINT = "http://seaweedfs:8333"
        AWS_ACCESS_KEY_ID = "admin"
        AWS_SECRET_ACCESS_KEY = "admin"
        
        TEMPORAL_BUCKET = "landing-zone"
        TEMPORAL_PREFIX = "aemet/"
        
        persistent_path = "s3://persistent-landing-zone/aemet_predictions/"

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

        response = s3_client.list_objects_v2(Bucket=TEMPORAL_BUCKET, Prefix=TEMPORAL_PREFIX)
        if 'Contents' not in response:
            print("No new files found in the Temporal Landing Zone for AEMET.")
            return

        all_data = []
        processed_keys = []
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('.json'):
                file_obj = s3_client.get_object(Bucket=TEMPORAL_BUCKET, Key=key)
                file_content = file_obj['Body'].read().decode('utf-8')
                try:
                    data = json.loads(file_content)
                    all_data.append(data)
                    # processed_keys.append(key)
                except json.JSONDecodeError:
                    print(f"Error decoding JSON from {key}")

        if not all_data:
            print("No valid JSON data found.")
            return

        df_new = pd.DataFrame(all_data)

        for col in df_new.columns:
            if df_new[col].dtype == 'object':
                df_new[col] = df_new[col].astype(str)

        if 'id' not in df_new.columns or 'elaborado' not in df_new.columns:
            raise ValueError("Required keys ('id', 'elaborado') missing from AEMET data.")

        try:
            dt = DeltaTable(persistent_path, storage_options=storage_options)
            (
                dt.merge(
                    source=df_new,
                    predicate="target.id = source.id AND target.elaborado = source.elaborado",
                    source_alias="source",
                    target_alias="target"
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
            print(f"AEMET data updated successfully. Merged {len(df_new)} records.")
            
        except Exception as e:
            print(f"AEMET table not found. Initializing for the first time... (Details: {e})")
            write_deltalake(
                persistent_path,
                df_new,
                storage_options=storage_options,
                mode="overwrite"
            )
            print("AEMET Delta table initialized successfully.")

        # if processed_keys:
        #     delete_objects = {'Objects': [{'Key': k} for k in processed_keys]}
        #     s3_client.delete_objects(Bucket=TEMPORAL_BUCKET, Delete=delete_objects)
        #     print(f"Cleaned up {len(processed_keys)} files from {TEMPORAL_BUCKET}.")

    process_temporal_to_persistent()

aemet_temporal_to_persistent_pipeline()