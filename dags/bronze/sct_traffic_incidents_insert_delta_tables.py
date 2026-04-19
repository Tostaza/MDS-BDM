from airflow.decorators import dag, task
from datetime import datetime, timedelta
import os
import io

default_args = {
    'owner': 'bdm_expert',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id='sct_traffic_incidents_temporal_to_persistent',
    default_args=default_args,
    description='Batch transformation from raw SCT Traffic Incidents XML to Delta Lake using delta-rs',
    schedule=None, 
    start_date=datetime(2026, 4, 10),
    catchup=False,
    tags=['sct', 'traffic', 'bronze'],
)
def sct_traffic_incidents_bronze_pipeline():

    @task(task_id='move_and_catalog_task')
    def move_and_catalog():
        import pandas as pd
        from Clients.seaweedfs import Seaweedfs
        from DELTA_LAKE.delta_lake import DeltaLake

        S3_ENDPOINT = "http://seaweedfs:8333"
        AWS_SECRET_ACCESS_KEY = "admin"
        AWS_ACCESS_KEY_ID = "admin"
        
        LANDING_BUCKET = "landing-zone"
        LANDING_PREFIX = "sct/traffic_incidents/"
        
        TRUSTED_BUCKET = "trusted-zone"
        TRUSTED_PREFIX = "sct/traffic_incidents/"
        
        storage_options = {
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_ENDPOINT_URL": S3_ENDPOINT,
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "AWS_ALLOW_HTTP": "true" 
        }
        
        predicate = "target.file_name = source.file_name"

        s3_client = Seaweedfs()
        s3_client.connect()
        delta_lake = DeltaLake()
        response = s3_client.s3_client.list_objects_v2(Bucket=LANDING_BUCKET, Prefix=LANDING_PREFIX)
        if 'Contents' not in response:
            print("No new files found in the Landing Zone for SCT Traffic Incidents.")
            return
        persistent_path_xml = f"s3://{TRUSTED_BUCKET}/{TRUSTED_PREFIX}"
        metadata_xml = []
        for obj in response['Contents']:
            key = obj['Key']

            if key.endswith('.xml'):
                xml_content = s3_client.get_object(
                    Bucket=LANDING_BUCKET,
                    Key=key
                )['Body'].read().decode('utf-8')

                metadata_xml.append({
                    "file_name": key,
                    "xml_content": xml_content
                })

        df_new = pd.DataFrame(metadata_xml)
        s3_client.create_bucket(TRUSTED_BUCKET)
        delta_lake.coonect_and_load(df_new, persistent_path_xml, storage_options, predicate)
        
    move_and_catalog()

sct_traffic_incidents_bronze_pipeline()