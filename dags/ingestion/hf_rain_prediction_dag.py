import os
import boto3
# from huggingface_hub import hf_hub_download
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
from Clients.seaweedfs import Seaweedfs


default_args = {
    'owner': 'upc',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='ingest_hf_rain_prediction',
    default_args=default_args,
    schedule=None, # This should be executed only once
    catchup=False,
    tags=['huggingface', 'csv', 'weather'],
)
def hf_rainfall_ingestion():

    REPOSITORY = "kanishka089"
    DATASET = "RainPrediction"
    
    TEMPORAL_LANDING_BUCKET = "temporal-landing-zone"

    @task
    def ingest_from_hf():
        from datasets import load_dataset
        import pandas as pd

        client = Seaweedfs()
        dataset = load_dataset(f"{REPOSITORY}/{DATASET}")
        df = dataset["train"].to_pandas()

        key = f"huggingface/{REPOSITORY}/{DATASET}.csv"
        client.upload_dataframe(TEMPORAL_LANDING_BUCKET, key, df)
        print(f"Uploaded {REPOSITORY}/{DATASET} dataset to {TEMPORAL_LANDING_BUCKET}")

    ingest_from_hf()

hf_rainfall_ingestion()
