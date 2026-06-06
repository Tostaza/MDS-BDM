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
    dag_id='bcn_rt_trams_congestion_ingestion',
    default_args=default_args,
    schedule="*/5 * * * *",
    catchup=False,
    tags=['opendata', 'csv', 'transit', 'scheduled', 'ingestion'],
)
def ingest_trams_congestion():

    @task
    def ingest_data(**context):
        from Clients.seaweedfs import Seaweedfs
        from Extractors.json import ExtractorJSON
        from Kafka.Kafka import Kafka
        import pandas as pd
        import io
        from kafka import KafkaProducer
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError

        LANDING_BUCKET = "landing-zone"
        client = Seaweedfs()
        client.connect()

        catalog_url = f"https://opendata-ajuntament.barcelona.cat/data/api/3/action/package_show?id=trams"
        extractor = ExtractorJSON()
        catalog_json, _ = extractor.extract_data(catalog_url)

        result = catalog_json["result"]
    
        url = result["resources"][0]["url"]
        name = result["resources"][0]["name"]
        response = requests.get(url)
        
        df = pd.read_csv(
            io.StringIO(response.text),
            sep="#",
            header=None,
            names=["idTram", "data", "estatActual", "estatPrevist"],
        )

        logical_ts = context["logical_date"].strftime("%Y%m%d_%H%M%S")
        base_name = name.rsplit(".", 1)[0] if "." in name else name
        landing_key = f"ajuntament_bcn/trams_congestion/real_time/{base_name}_{logical_ts}.csv"
        client.upload_dataframe(LANDING_BUCKET, landing_key, df)

        try:
            client_kafka = Kafka()
            client_kafka.connect('trams_congestion')
            client_kafka.load_to_kafka(df)
        except Exception as kafka_err:
            print(f"[WARN rt_trams] Kafka no disponible, s'omet: {kafka_err}")

    trigger_temporal_to_persistent_task = TriggerDagRunOperator(
        task_id='trigger_bcn_trams_congestion_real_time_temporal_to_persistent',
        trigger_dag_id='bcn_trams_congestion_real_time_temporal_to_persistent',
        wait_for_completion=False
    )

    ingest_data() >> trigger_temporal_to_persistent_task

ingest_trams_congestion()
