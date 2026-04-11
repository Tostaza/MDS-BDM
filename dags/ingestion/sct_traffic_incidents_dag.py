import os
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
    dag_id='sct_traffic_incidents_ingestion',
    default_args=default_args,
    schedule='*/5 * * * *',
    catchup=False,
    tags=['sct', 'traffic', 'scheduled', 'ingestion'],
)
def sct_traffic_incidents_ingestion():

    @task
    def ingest_data(**context):
        from Clients.seaweedfs import Seaweedfs
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError
        from kafka import KafkaProducer
        import json
        
        LANDING_BUCKET = "landing-zone"

        logical_date = context['logical_date']
        timestamp = logical_date.isoformat()

        url = "https://www.gencat.cat/transit/opendata/incidenciesGML.xml"
        response = requests.get(url)
        response.raise_for_status()

        client = Seaweedfs()
        
        key = f"sct/traffic_incidents/{timestamp}.xml"
        client.upload_object(
            bucket=LANDING_BUCKET,
            key=key,
            obj_bytes=response.text
        )
        print(f"Uploaded traffic incidents to {LANDING_BUCKET} as {key}")


        bootstrap_servers = Variable.get("KAFKA_BOOTSTRAP_SERVERS", default_var=os.getenv("KAFKA_BOOTSTRAP_SERVERS")).split(",")
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        topic_name = 'traffic_incidents'
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
            new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
            admin_client.create_topics(new_topics=[new_topic], validate_only=False)
            print(f"Topic '{topic_name}' created successfully.")
        except TopicAlreadyExistsError:
            print("Topic already exists")
        except Exception as e:
            print(f"Could not verify/create topic (may not have connection): {e}")



        kafka_event = {
            "timestamp": logical_date.isoformat(),
            "url": f"s3://{LANDING_BUCKET}/{key}",
        }
        
        producer.send(topic_name, value=kafka_event)
        producer.flush()

    trigger_temporal_to_persistent_task = TriggerDagRunOperator(
        task_id='trigger_sct_traffic_incidents_temporal_to_persistent',
        trigger_dag_id='sct_traffic_incidents_temporal_to_persistent',
        wait_for_completion=False
    )

    ingest_data() >> trigger_temporal_to_persistent_task



sct_traffic_incidents_ingestion()