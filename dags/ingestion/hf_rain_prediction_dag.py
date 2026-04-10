import os
import boto3
# from huggingface_hub import hf_hub_download
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
from Clients.seaweedfs import Seaweedfs
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


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


    trigger_temporal_to_persistent_task = TriggerDagRunOperator(
        task_id='trigger_hfrain_prediction_temporal_to_persistent',
        trigger_dag_id='hf_rain_temporal_to_persistent',
        wait_for_completion=False
    )

    ingest_from_hf() >> trigger_temporal_to_persistent_task

hf_rainfall_ingestion()
