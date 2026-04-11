import os
import json
import requests
import boto3
from datetime import datetime, timedelta, timezone
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'upc',
    'depends_on_past': False,
    'start_date': datetime.now(tz=timezone.utc),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='bcn_historic_accidents_gu_ingest',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['opendata', 'json', 'pollution', 'manual', 'ingestion'],
)
def ingest_accidents_gu():


    @task
    def ingest_data():
        from Clients.seaweedfs import Seaweedfs
        import pandas as pd
        import io

        LANDING_BUCKET = "landing-zone"

        catalog_url = f"https://opendata-ajuntament.barcelona.cat/data/api/3/action/package_show?id=accidents-gu-bcn"
        response = requests.get(catalog_url)
        response.raise_for_status()
        
        catalog_json = response.json()
        result = catalog_json["result"]

        client = Seaweedfs()
    
        years = 5
        # In the catalog, results are ordered by year (e.g. [2025, 2024, 2023...])
        for i in range(years):
            url = result["resources"][i]["url"]
            name = result["resources"][i]["name"]
            response = requests.get(url)
            
            df = pd.read_csv(io.StringIO(response.text))
            key = f"ajuntament_bcn/accidents_gu/{name}"
            client.upload_dataframe(LANDING_BUCKET, key, df)
            

    trigger_temporal_to_persistent_task = TriggerDagRunOperator(
        task_id='trigger_bcn_accidents_gu_temporal_to_persistent',
        trigger_dag_id='bcn_accidents_gu_temporal_to_persistent',
        wait_for_completion=False
    )

    ingest_data() >> trigger_temporal_to_persistent_task

ingest_accidents_gu()
