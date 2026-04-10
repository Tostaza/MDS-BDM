import os
import json
import requests
import boto3
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'upc',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='ingest_relacio_trams',
    default_args=default_args,
    schedule='@yearly',
    catchup=False,
    tags=['opendata', 'csv', 'transit'],
)
def ingest_relacio_trams():


    @task
    def ingest_data():
        from Clients.seaweedfs import Seaweedfs
        import pandas as pd
        import io

        TEMPORAL_LANDING_BUCKET = "temporal-landing-zone"
        client = Seaweedfs()

        url = "https://opendata-ajuntament.barcelona.cat/data/dataset/1090983a-1c40-4609-8620-14ad49aae3ab/resource/c97072a3-3619-4547-84dd-f1999d2a3fec/download/transit_relacio_trams_format_long.csv"
        response = requests.get(url)
        
        df = pd.read_csv(io.StringIO(response.text))
        key = f"ajuntament_bcn/relacio_trams/relacio_trams_format_long.csv"
        client.upload_dataframe(TEMPORAL_LANDING_BUCKET, key, df)

    trigger_temporal_to_persistent_task = TriggerDagRunOperator(
        task_id='trigger_bcn_relacio_trams_temporal_to_persistent',
        trigger_dag_id='bcn_relacio_trams_temporal_to_persistent',
        wait_for_completion=False
    )

    ingest_data() >> trigger_temporal_to_persistent_task
            

ingest_relacio_trams()
