from datetime import datetime
import json
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

from Extractors.csv import ExtractorCSV
from Extractors.json import ExtractorJSON
from Extractors.xml import ExtractorXML
from Clients.client import Client
from Clients.seaweedfs import Seaweedfs


BASE_DIR = os.path.dirname(__file__)
RUTA_JSON = os.path.join(BASE_DIR, "apis.json")


def load_api_config():
    print("load_api_config")
    with open(RUTA_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["apis"]

def filter_apis(apis, *, data_type=None):
    result = []
    print("filter apis")
    for entry in apis:
        if data_type is not None and entry.get("data_type") != data_type:
            continue
        result.append(entry)
    return result


def task_csv(**context):
    s3_client =  Seaweedfs()
    s3_client.connect()
    apis = load_api_config()
    kaggle_csv = filter_apis(apis, data_type="csv")
    for cfg in kaggle_csv:
        api_id = cfg["api"]
        extractor = ExtractorCSV()
        doc, key = extractor.extract_data(api_id, cfg["source"])
        s3_client.load_doc("temporal-landing-zone", doc, key)
        

def task_json(**context):
    s3_client =  Seaweedfs()
    s3_client.connect()
    apis = load_api_config()
    print("json_task")
    opendata_xml = filter_apis(apis, data_type="json")
    for cfg in opendata_xml:
        url = cfg["api"]
        extractor = ExtractorJSON()  # substitueix per XmlExtractor si en tens
        document, key = extractor.extract_data(url)
        s3_client.load_json("temporal-landing-zone", document, key)


def task_xml(**context):
    s3_client =  Seaweedfs()
    s3_client.connect()
    apis = load_api_config()
    print("xml task")
    opendata_xml = filter_apis(apis, data_type="xml")
    for cfg in opendata_xml:
        url = cfg["api"]
        extractor = ExtractorXML()  # substitueix per XmlExtractor si en tens
        document, key = extractor.extract_data(url)
        s3_client.load_xml("temporal-landing-zone", document, cfg["source"]+key)

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

    [_csv_task, _json_task, _xml_task, real_time_task]
