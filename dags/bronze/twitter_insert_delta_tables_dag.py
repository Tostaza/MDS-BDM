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
    dag_id='transit_temporal_to_persistent',
    default_args=default_args,
    description='Batch transformation from raw JSON to Delta Lake using delta-rs',
    schedule=None, # None, since it will be triggered by the ingestion dag with schedule '*/10 * * * *'
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['twitter', "social_media", 'bronze'],
)
def transit_temporal_to_persistent_pipeline():

    @task(task_id='process_delta_table_task')
    def process_temporal_to_persistent():
        """
        Function that reads raw JSONs from SeaweedFS using boto3/pandas 
        and saves/updates them in Delta Lake format using delta-rs.
        """
        import pandas as pd
        import boto3
        import json
        from deltalake import DeltaTable, write_deltalake

        S3_ENDPOINT = "http://seaweedfs:8333"
        AWS_SECRET_ACCESS_KEY = "admin"
        AWS_ACCESS_KEY_ID = "admin"
        
        TEMPORAL_BUCKET = "landing-zone"
        TEMPORAL_PREFIX = "transit_posts/"
        
        temporal_path = f"s3://{TEMPORAL_BUCKET}/{TEMPORAL_PREFIX}"
        persistent_path = "s3://persistent-landing-zone/transit_posts/"

        # delta-rs S3 configuration
        storage_options = {
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "AWS_ENDPOINT_URL": S3_ENDPOINT,
            "AWS_REGION": "us-east-1", # Required by the underlying Rust core
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true", # Required for local S3 clones like SeaweedFS/MinIO
            "AWS_ALLOW_HTTP": "true" # This is important to avoid the error: URL scheme is not allowed 
        }

        # 1. Read raw data (JSON) using boto3
        s3_client = boto3.client(
            's3',
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )

        response = s3_client.list_objects_v2(Bucket=TEMPORAL_BUCKET, Prefix=TEMPORAL_PREFIX)
        if 'Contents' not in response:
            print("No new files found in the Temporal Landing Zone.")
            return

        all_data = []
        for obj in response['Contents']:
            if obj['Key'].endswith('.json'):
                file_obj = s3_client.get_object(Bucket=TEMPORAL_BUCKET, Key=obj['Key'])
                print("File object obtained")
                file_content = file_obj['Body'].read().decode('utf-8')
                try:
                    data = json.loads(file_content)
                    all_data.append(data)
                except json.JSONDecodeError:
                    print(f"Error decoding JSON from {obj['Key']}")

        if not all_data:
            print("No valid JSON data found.")
            return

        # Convert all data to Pandas DataFrame for further insert in Delta Lake
        df_new = pd.DataFrame(all_data)

        # Fix data types to avoid errors when infering the schema when creating the table
        for col in df_new.columns:
            if df_new[col].dtype == 'object':
                df_new[col] = df_new[col].astype(str)

        if 'link' not in df_new.columns:
            raise ValueError("The 'link' column is missing from the source data.")

        try:

            dt = DeltaTable(persistent_path, storage_options=storage_options)
            (
                dt.merge(
                    source=df_new,
                    predicate="target.link = source.link",
                    source_alias="source",
                    target_alias="target"
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
            print("Data updated via MERGE successfully.")
            
        except Exception as e:
            print(f"Table not found. Initializing for the first time... (Details: {e})")
            write_deltalake(
                persistent_path,
                df_new,
                storage_options=storage_options,
                mode="overwrite"
            )
            print("Delta table initialized successfully.")

        # IMPORTANT: At this point, the JSONs are still in the landing-zone.
        # As discussed, it is highly recommended to add boto3 logic here to delete or move
        # the processed objects from 'response['Contents']' to avoid reprocessing them.

    # We invoke the task within the DAG flow
    process_temporal_to_persistent()

# Instantiate the DAG by calling the main function
transit_temporal_to_persistent_pipeline()