from airflow.decorators import dag, task
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'bdm_expert',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id='traffic_cameras_bronze_ingestion',
    default_args=default_args,
    description='Moves raw files and catalogs XML and image paths in Delta Bronze tables',
    schedule=None, 
    start_date=datetime(2026, 3, 31),
    catchup=False,
    tags=['sct', 'traffic', 'bronze', 'medallion'],
)
def traffic_cameras_bronze_pipeline():

    @task(task_id='move_and_catalog_task')
    def move_and_catalog():
        import pandas as pd
        import boto3
        from deltalake import write_deltalake, DeltaTable
        import re

        S3_ENDPOINT = "http://seaweedfs:8333"
        AWS_SECRET_ACCESS_KEY = "admin"
        AWS_ACCESS_KEY_ID = "admin"
        
        TEMPORAL_BUCKET = "landing-zone"
        TEMPORAL_PREFIX = "traffic_cameras"
        
        PERSISTENT_BUCKET = "persistent-landing-zone"
        PERSISTENT_PREFIX = "traffic_cameras"
        
        storage_options = {
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
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
            return

        
        metadata_xml = []
        metadata_images = []

        regex = rf"^{TEMPORAL_PREFIX}/(\d{{8}}_\d{{6}})/"
        for obj in response['Contents']:
            old_key = obj['Key']
            timestamp = re.match(regex, old_key).group(1)
            filename = old_key.split('/')[-1]
    
            try:
                if old_key.endswith('.xml'):
                    xml_content = s3_client.get_object(Bucket=TEMPORAL_BUCKET, Key=old_key)['Body'].read().decode('utf-8')
                    metadata_xml.append({"timestamp": timestamp, "xml_content": xml_content})
                
                else:
                    new_key = f"{PERSISTENT_PREFIX}/images/raw/{filename}"
                    s3_client.copy_object(
                        CopySource={'Bucket': TEMPORAL_BUCKET, 'Key': old_key},
                        Bucket=PERSISTENT_BUCKET, Key=new_key
                    )
                    metadata_images.append({"timestamp": timestamp, "url": f"s3://{PERSISTENT_BUCKET}/{new_key}"})
            
            except Exception as e:
                print(f"Error processing {old_key}: {e}")

        
        if not metadata_xml :
            print("No xml data found.")
            return        
        if not metadata_images:
            print("No images data found.")
            return
        

        xml_df = pd.DataFrame(metadata_xml)
        for col in xml_df.columns:
            if xml_df[col].dtype == 'object':
                xml_df[col] = xml_df[col].astype(str)

        img_df = pd.DataFrame(metadata_images)
        for col in img_df.columns:
            if img_df[col].dtype == 'object':
                img_df[col] = img_df[col].astype(str)

        if 'timestamp' not in xml_df.columns:
            raise ValueError("The 'timestamp' column is missing from the source data.")

        if 'timestamp' not in img_df.columns:
            raise ValueError("The 'timestamp' column is missing from the source data.")

        persistent_path_xml = f"s3://{PERSISTENT_BUCKET}/{PERSISTENT_PREFIX}/xml"
        persistent_path_img = f"s3://{PERSISTENT_BUCKET}/{PERSISTENT_PREFIX}/images/metadata"
        try:
            dt = DeltaTable(persistent_path_xml, storage_options=storage_options)
            (
                dt.merge(
                    source=xml_df,
                    predicate="target.timestamp = source.timestamp",
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
                persistent_path_xml,
                xml_df,
                storage_options=storage_options,
                mode="overwrite"
            )
            print("Delta table initialized successfully.")

        try:
            dt = DeltaTable(persistent_path_img, storage_options=storage_options)
            (
                dt.merge(
                    source=img_df,
                    predicate="target.timestamp = source.timestamp",
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
                persistent_path_img,
                img_df,
                storage_options=storage_options,
                mode="overwrite"
            )
            print("Delta table initialized successfully.")
        
    move_and_catalog()

traffic_cameras_bronze_pipeline()