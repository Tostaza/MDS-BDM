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
    dag_id='bcn_historic_trams_congestion_ingestion',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['opendata', 'csv', 'transit', 'manual', 'ingestion'],
)
def ingest_trams_congestion():

    @task
    def ingest_data():
        from Clients.seaweedfs import Seaweedfs
        import pandas as pd
        import io

        LANDING_BUCKET = "landing-zone"
        client = Seaweedfs()
        client.connect()

        catalog_url = f"https://opendata-ajuntament.barcelona.cat/data/api/3/action/package_show?id=trams"
        extractor = ExtractorJSON()
        catalog_json, _ = extractor.extract_data(catalog_url)
        result = catalog_json["result"]
    
        months = 16 # (2025 and 2026)
        for i in range(months):
            url = result["resources"][i+1]["url"]
            name = result["resources"][i+1]["name"]
            response = requests.get(url)
            
            df = pd.read_csv(io.StringIO(response.text))
            key = f"ajuntament_bcn/trams_congestion/{name}"
            client.upload_dataframe(LANDING_BUCKET, key, df)


    trigger_temporal_to_persistent_task = TriggerDagRunOperator(
        task_id='trigger_bcn_trams_congestion_temporal_to_persistent',
        trigger_dag_id='bcn_trams_congestion_temporal_to_persistent',
        wait_for_completion=False
    )

    ingest_data() >> trigger_temporal_to_persistent_task

ingest_trams_congestion()
