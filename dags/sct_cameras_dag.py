from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import pandas as pd
import xml.etree.ElementTree as ET
import boto3
import os
from urllib.parse import urlparse

# Argumentos base
default_args = {
    'owner': 'bdm_expert',
    'start_date': datetime(2026, 3, 31),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

ROADS = ["B-10", "B-20", "Ronda del Mig", "B-23", "C-31", "C-32", "C-58"]   
CITIES = ["BARCELONA", "L'HOSPITALET DE LLOBREGAT", "BADALONA", "SANT ADRIÀ DE BESÒS", "MONTGAT", "SANT BOI"]

BUCKET = "landing-zone"

@dag(
    dag_id='traffic_cameras_full_ingestion',
    default_args=default_args,
    schedule='*/10 * * * *', # Frecuencia de 10 min
    catchup=False,
    tags=['sct', 'traffic', 'ingestion']
)
def traffic_cameras_dag():

    @task
    def get_camera_metadata():
        url = 'https://www.gencat.cat/transit/opendata/cameres.xml'
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        
        root = ET.fromstring(response.text)
        ns = {"gml": "http://www.opengis.net/gml", "cite": "http://www.opengeospatial.net/cite"}
        
        rows = []
        for cam in root.findall(".//gml:featureMember/cite:cameres", ns):
            rows.append({
                "carretera": cam.findtext("cite:carretera", default="", namespaces=ns).strip(),
                "municipi": cam.findtext("cite:municipi", default="", namespaces=ns).strip(),
                "link": cam.findtext("cite:link", default="", namespaces=ns).strip()
            })
        
        df = pd.DataFrame(rows)
        df_filtered = df
        if CITIES:
            df_filtered = df[df['municipi'].isin(CITIES)]
        if ROADS:
            df_filtered = df_filtered[df_filtered['carretera'].isin(ROADS)]
        
        return df_filtered.to_dict(orient='records')

    @task
    def ingest_images_to_minio(camera_list, bucket: str):
        import boto3
        
        aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID", default_var=os.getenv("AWS_ACCESS_KEY_ID"))
        aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY", default_var=os.getenv("AWS_SECRET_ACCESS_KEY"))
        endpoint_url = Variable.get("S3_ENDPOINT", default_var=os.getenv("S3_ENDPOINT"))
        s3_client = boto3.client(
            "s3",
            aws_access_key_id= aws_access_key_id,
            aws_secret_access_key= aws_secret_access_key,
            endpoint_url= endpoint_url
        )
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for i, cam in enumerate(camera_list):
            try:
                img_data = requests.get(cam['link'], timeout=15).content
                ext = os.path.splitext(urlparse(cam['link']).path)[1] or ".gif"
                object_name = f"traffic_cameras/{timestamp}/{cam['carretera']}_{cam['municipi']}_{i}{ext}"

                s3_client.put_object(Bucket=bucket, Key=object_name, Body=img_data)
                print(f"Uploaded {len(camera_list)} images to {bucket}.")
            except Exception as e:
                print(f"Error con {cam['link']}: {e}")

    
    metadata = get_camera_metadata()
    ingest_images_to_minio(camera_list=metadata, bucket=BUCKET)

traffic_cameras_dag()