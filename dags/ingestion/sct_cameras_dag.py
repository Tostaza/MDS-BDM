from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta, timezone
import requests
import pandas as pd
import xml.etree.ElementTree as ET
import boto3
import os
from urllib.parse import urlparse
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

default_args = {
    'owner': 'upc',
    'start_date': datetime.now(tz=timezone.utc),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

ROADS = [] # ["B-10", "B-20", "Ronda del Mig", "B-23", "C-31", "C-32", "C-58"]   
CITIES = ["barcelona", "l'hospitalet de llobregat", "badalona", "sant adrià de besòs", "montgat", "sant boi"]

BUCKET = "landing-zone"

@dag(
    dag_id='sct_traffic_cameras_full_ingestion',
    default_args=default_args,
    schedule='*/10 * * * *', # Frecuencia de 10 min
    catchup=False,
    tags=['sct', 'traffic', 'scheduled', 'ingestion']
)
def traffic_cameras_dag():

    @task
    def get_camera_metadata():
        url = 'https://www.gencat.cat/transit/opendata/cameres.xml'
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        
        namespaces = {
            "wfs": "http://www.opengis.net/wfs",
            "gml": "http://www.opengis.net/gml", 
            "cite": "http://www.opengeospatial.net/cite",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance"
        }
        for prefix, uri in namespaces.items():
            ET.register_namespace(prefix, uri)
        root = ET.fromstring(response.text)
        ns = {"gml": "http://www.opengis.net/gml", "cite": "http://www.opengeospatial.net/cite"}
        
        for feature in root.findall("gml:featureMember", ns):
            cam = feature.find("cite:cameres", ns)
            if cam is not None:
                carretera = cam.findtext("cite:carretera", default="", namespaces=ns).strip()
                municipi = cam.findtext("cite:municipi", default="", namespaces=ns).strip()
                link = cam.findtext("cite:link", default="", namespaces=ns).strip()
                
                keep = True
                if CITIES and municipi.lower() not in CITIES:
                    keep = False
                if ROADS and carretera.lower() not in ROADS:
                    keep = False
                
                if not keep:
                    root.remove(feature)

        xml_filtered = ET.tostring(root, encoding='utf-8', xml_declaration=True).decode('utf-8')
        return xml_filtered

    @task
    def ingest_images(xml, bucket: str, **context):
        import boto3
        import json
        from kafka import KafkaProducer

        aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID", default_var=os.getenv("AWS_ACCESS_KEY_ID"))
        aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY", default_var=os.getenv("AWS_SECRET_ACCESS_KEY"))
        endpoint_url = Variable.get("S3_ENDPOINT", default_var=os.getenv("S3_ENDPOINT"))
        s3_client = boto3.client(
            "s3",
            aws_access_key_id= aws_access_key_id,
            aws_secret_access_key= aws_secret_access_key,
            endpoint_url= endpoint_url
        )

        bootstrap_servers = Variable.get("KAFKA_BOOTSTRAP_SERVERS", default_var=os.getenv("KAFKA_BOOTSTRAP_SERVERS")).split(",")
        print(bootstrap_servers)
        print(type(bootstrap_servers))
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        try:
            admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
            new_topic = NewTopic(name='traffic_cameras', num_partitions=1, replication_factor=1)
            admin_client.create_topics(new_topics=[new_topic], validate_only=False)
            print("Topic 'traffic_cameras' created successfully.")
        except TopicAlreadyExistsError:
            print("Topic already exists")
        except Exception as e:
            print(f"Could not verify/create topic (may not have connection): {e}")


        # timestamp = context['logical_date'].strftime("%Y%m%d_%H%M%S")
        timestamp = context['logical_date'].isoformat()
        root = ET.fromstring(xml)   
        ns = {"gml": "http://www.opengis.net/gml", "cite": "http://www.opengeospatial.net/cite"}
        
        for i, feature in enumerate(root.findall("gml:featureMember", ns)):
            cam = feature.find("cite:cameres", ns)
            if cam is not None:
                carretera = cam.findtext("cite:carretera", default="", namespaces=ns).strip().lower()
                municipi = cam.findtext("cite:municipi", default="", namespaces=ns).strip().lower()
                link = cam.findtext("cite:link", default="", namespaces=ns).strip()
                
                try:
                    img_data = requests.get(link, timeout=15).content
                    ext = os.path.splitext(urlparse(link).path)[1] or ".gif"
                    object_name = f"traffic_cameras/{timestamp}/{carretera}_{municipi}_{i}{ext}"

                    s3_client.put_object(Bucket=bucket, Key=object_name, Body=img_data)

                    kafka_event = {
                        "carretera": carretera,
                        "municipi": municipi,
                        "link": link,
                        "timestamp": timestamp,
                        "url": f"s3://{bucket}/{object_name}"
                    }
                    producer.send("traffic_cameras", value=kafka_event)

                except Exception as e:
                    print(f"Error with camera {link}: {e}")

        s3_client.put_object(Bucket=bucket, Key=f"traffic_cameras/{timestamp}/metadata.xml", Body=xml)
        print(f"Uploaded 1 metadata file and {i+1} images to {bucket}.")

    trigger_temporal_to_persistent_task = TriggerDagRunOperator(
        task_id='sct_cameras_trigger_temporal_to_persistent',
        trigger_dag_id='sct_cameras_temporal_to_persistent',
        wait_for_completion=False
    )

    metadata = get_camera_metadata()
    ingestion_task = ingest_images(xml=metadata, bucket=BUCKET)
    ingestion_task >> trigger_temporal_to_persistent_task

traffic_cameras_dag()