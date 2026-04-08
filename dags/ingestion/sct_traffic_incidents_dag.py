import os
import requests
import boto3
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable

default_args = {
    'owner': 'upc',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='sct_traffic_incidents_ingestion',
    default_args=default_args,
    schedule='*/5 * * * *',
    catchup=False,
    tags=['sct', 'traffic', 'ingestion'],
)
def sct_traffic_incidents_ingestion():


    # Incidències viàries en temps real a Catalunya
    @task
    def ingest_data():
        from Clients.seaweedfs import Seaweedfs

        TEMPORAL_LANDING_BUCKET = "temporal-landing-zone"

        url = "https://www.gencat.cat/transit/opendata/incidenciesGML.xml"
        response = requests.get(url)
        response.raise_for_status()

        client = Seaweedfs()
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        key = f"sct/traffic_incidents/{timestamp}.xml"
        client.upload_object(
            bucket=TEMPORAL_LANDING_BUCKET,
            key=key,
            obj_bytes=response.text
        )
        print(f"Uploaded traffic incidents to {TEMPORAL_LANDING_BUCKET} as {key}")

    ingest_data()

sct_traffic_incidents_ingestion()
