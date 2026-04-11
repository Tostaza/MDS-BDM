
import os
import json
import requests
import boto3
from datetime import datetime, timedelta, timezone
from airflow.decorators import dag, task
from airflow.models import Variable
from io import BytesIO
from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
import requests
from datetime import datetime, timedelta
from io import BytesIO
from airflow.decorators import dag, task

default_args = {
    'owner': 'upc',
    'depends_on_past': False,
    'start_date': datetime.now(tz=timezone.utc),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='bcn_relacio_trams_real_time_ingestion',
    default_args=default_args,
    schedule="*/5 * * * *",
    catchup=False,
    tags=['opendata', 'csv', 'transit', 'real_time', 'phrase'],
)
def ingest_trams_congestion_phrase():

    @task
    def ingest_data():

        from Clients.seaweedfs import Seaweedfs 
        
        TEMPORAL_LANDING_BUCKET = "temporal-landing-zone"
        client = Seaweedfs()
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

        catalog_url = "https://opendata-ajuntament.barcelona.cat/data/dataset/8319c2b1-4c21-4962-9acd-6db4c5ff1148/resource/2d456eb5-4ea6-4f68-9794-2f3f1a58a933/download"
        
        try:
            response = requests.get(catalog_url, headers=headers, timeout=30)
            response.raise_for_status() 
            file_data = BytesIO(response.content)
            
            name = "openData-bcn"
            key = f"ajuntament_bcn/trams_congestion/real_time/{name}_real_time_congestion_phrase.csv"
            client.upload_object(TEMPORAL_LANDING_BUCKET, key, file_data)
            print(f"Archivo subido exitosamente a {key}")

        except requests.exceptions.RequestException as e:
            print(f"Error en la descarga: {e}")
            raise

    ingest_data()

ingest_trams_congestion_phrase_dag = ingest_trams_congestion_phrase()