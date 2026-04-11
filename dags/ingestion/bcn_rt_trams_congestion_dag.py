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
        import pandas as pd
        import io
        from kafka import KafkaProducer
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError

        LANDING_BUCKET = "landing-zone"
        client = Seaweedfs()

        catalog_url = f"https://opendata-ajuntament.barcelona.cat/data/api/3/action/package_show?id=trams"
        response = requests.get(catalog_url)
        response.raise_for_status()
        
        catalog_json = response.json()
        result = catalog_json["result"]
    
        url = result["resources"][0]["url"]
        name = result["resources"][0]["name"]
        response = requests.get(url)
        
        df = pd.read_csv(io.StringIO(response.text))

        topic_name = 'trams_congestion'
        bootstrap_servers = Variable.get("KAFKA_BOOTSTRAP_SERVERS", default_var=os.getenv("KAFKA_BOOTSTRAP_SERVERS")).split(",")
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
            new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
            admin_client.create_topics(new_topics=[new_topic], validate_only=False)
            print(f"Topic '{topic_name}' created successfully.")
        except TopicAlreadyExistsError:
            print("Topic already exists")
        except Exception as e:
            print(f"Could not verify/create topic (may not have connection): {e}")

        records = df.to_dict(orient='records')
        for record in records:
            producer.send(topic_name, value=record)
        producer.flush()
        print(f"Sent {len(records)} registers to Kafka ({topic_name}).")


    trigger_temporal_to_persistent_task = TriggerDagRunOperator(
        task_id='trigger_bcn_trams_congestion_temporal_to_persistent',
        trigger_dag_id='bcn_trams_congestion_temporal_to_persistent',
        wait_for_completion=False
    )

    ingest_data() >> trigger_temporal_to_persistent_task

ingest_trams_congestion()
