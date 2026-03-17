from datetime import datetime
import json
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.Extractors import csv
from src.Extractors import json
from src.Extractors import realTime
from src.Extractors import xml
from src.Clients.client import *
from src.Clients.seaweedfs import *


BASE_DIR = os.path.dirname(__file__)
RUTA_JSON = os.path.join(BASE_DIR, "apis.json")

s3_client: Client = None

def initilaize_client():
    s3_client = Seaweedfs
    s3_client.connect()


def load_api_config():
    with open(RUTA_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["apis"]


def filter_apis(apis, *, data_type=None):
    result = []
    for entry in apis:
        if data_type is not None and entry.get("data_type") != data_type:
            continue
        result.append(entry)
    return result


def task_csv(**context):
    apis = load_api_config()
    kaggle_csv = filter_apis(apis, data_type="csv")

    for cfg in kaggle_csv:
        api_id = cfg["api"]
        extractor = csv.csv()
        doc, key = extractor.extract(api_id)
        s3_client.load("temporal-landing-zone", doc, key)
        

def task_json(**context):
    apis = load_api_config()
    opendata_xml = filter_apis(apis, data_type="json")

    for cfg in opendata_xml:
        url = cfg["api"]
        extractor = json.JSON()  # substitueix per XmlExtractor si en tens
        document, key = extractor.extract(url)
        s3_client.load("temporal-landing-zone", document, key)


def task_xml(**context):
    apis = load_api_config()
    opendata_xml = filter_apis(apis, data_type="xml")

    for cfg in opendata_xml:
        url = cfg["api"]
        extractor = json.JSON()  # substitueix per XmlExtractor si en tens
        document, key = extractor.extract(url)
        s3_client.load("temporal-landing-zone", document, key)


def task_real_time(**context):
    pass
    #apis = load_api_config()
    #rt_apis = filter_apis(apis, data_type="real_time")

    #for cfg in rt_apis:
        #url = cfg["api"]
        #extractor = realTime.RealTime()
        #document, key = extractor.extract(url)
        # Diferent, perque aqui el que hauras de fer es segurament aplicar un acces per descarregar imatges.
        # i inclus també fotreles amb el kafka.
        #s3_client.load("temporal-landing-zone", document, key)


with DAG(
    dag_id="bdm_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    _csv_task = PythonOperator(
        task_id="kaggle_csv_task",
        python_callable=task_csv,
    )
    
    _json_task = PythonOperator(
        task_id="opendata_json_task",
        python_callable=task_json,
    )

    _xml_task = PythonOperator(
        task_id="opendata_xml_task",
        python_callable=task_xml,
    )

    real_time_task = PythonOperator(
        task_id="real_time_task",
        python_callable=task_real_time,
    )

    # aquí pots decidir l'ordre; ara les deixo en paral·lel
    [_csv_task, _json_task, _xml_task, real_time_task]
