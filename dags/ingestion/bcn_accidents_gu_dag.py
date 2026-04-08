import os
import json
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
    dag_id='ingest_accidents_gu',
    default_args=default_args,
    schedule='@yearly',
    catchup=False,
    tags=['opendata', 'json', 'pollution'],
)
def ingest_accidents_gu():

    @task
    def ingest_data():
        from Clients.seaweedfs import Seaweedfs
        import pandas as pd
        import io

        TEMPORAL_LANDING_BUCKET = "temporal-landing-zone"

        catalog_url = f"https://opendata-ajuntament.barcelona.cat/data/api/3/action/package_show?id=accidents-gu-bcn"
        response = requests.get(catalog_url)
        response.raise_for_status()
        
        catalog_json = response.json()

        client = Seaweedfs()
    
        years = 3
        for i in range(years):
            url = catalog_json["resources"][i]["url"]
            name = catalog_json["resources"][i]["name"]
            response = requests.get(url)
            
            df = pd.read_csv(io.StringIO(response.text))
            key = f"ajuntament_bcn/accidents_gu/{name}.csv"
            client.upload_dataframe(TEMPORAL_LANDING_BUCKET, key, df)
            

    ingest_data()

ingest_accidents_gu()
