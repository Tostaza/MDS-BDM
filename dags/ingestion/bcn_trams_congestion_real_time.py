import os
import json
import requests
import boto3
from datetime import datetime, timedelta, timezone
from airflow.decorators import dag, task
from airflow.models import Variable

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'upc',
    'depends_on_past': False,
    'start_date': datetime.now(tz=timezone.utc),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='bcn_trams_congestion_real_time_ingestion',
    default_args=default_args,
    schedule="*/5 * * * *",
    catchup=False,
    tags=['opendata', 'csv', 'transit', 'scheduled', 'ingestion'],
)
def ingest_trams_congestion():

    @task
    def ingest_data():
        from Clients.seaweedfs import Seaweedfs
        import pandas as pd
        import io

        TEMPORAL_LANDING_BUCKET = "temporal-landing-zone"
        client = Seaweedfs()
        headers = {
            "User-Agent": "Mozilla/5.0"
        }

        catalog_url = f"https://opendata-ajuntament.barcelona.cat/data/api/3/action/package_show?id=trams"
        response = requests.get(catalog_url, headers = headers)
        response.raise_for_status()
        
        catalog_json = response.json()
        result = catalog_json["result"]

        url = result["resources"][1]["url"]
        print(url)
        name = result["resources"][1]["name"]
        response = requests.get(url)
        print(response.text)
        df = pd.read_csv(response.text, sep=";")
        key = key = f"ajuntament_bcn/trams_congestion/{name}+'real_time'"
        client.upload_dataframe(TEMPORAL_LANDING_BUCKET, key, df)

    ingest_data()

ingest_trams_congestion()
