from datetime import datetime
import json
import os
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from Extractors.realTime import ExtractorRealTime
from Clients.seaweedfs import Seaweedfs

RUTA_JSON = os.path.join(os.path.dirname(__file__), "..", "JSON", "apis.json")

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

def task_real_time(**context):
    s3_client =  Seaweedfs()
    s3_client.connect()
    apis = load_api_config()
    rt_apis = filter_apis(apis, data_type="real_time")

    for cfg in rt_apis:
        url = cfg["api"]
        type = cfg["type"]
        extractor = ExtractorRealTime()
        document, key = extractor.extract_real_time_data(url, type)
        if document is not None:
            s3_client.load_real_time("temporal-landing-zone", document, key)

with DAG(
    dag_id="landing_zone_dag_real_time",
    schedule="*/10 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag_rt:

    realtime_task = PythonOperator(
        task_id="real_time_task",
        python_callable=task_real_time,
    )