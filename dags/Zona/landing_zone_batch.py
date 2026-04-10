from datetime import datetime
import json
import os

from airflow.decorators import dag, task

from Extractors.csv import ExtractorCSV
from Extractors.json import ExtractorJSON
from Extractors.xml import ExtractorXML
from Extractors.realTime import ExtractorRealTime
from Clients.client import Client
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


@dag(
    dag_id="landing_zone_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["landing", "ingestion"]
)
def landing_zone_dag():

    @task
    def task_csv():
        s3_client = Seaweedfs()
        s3_client.connect()

        apis = load_api_config()
        kaggle_csv = filter_apis(apis, data_type="csv")

        for cfg in kaggle_csv:
            api_id = cfg["api"]
            extractor = ExtractorCSV()
            doc, key = extractor.extract_data(api_id, cfg["source"])
            s3_client.load_doc("temporal-landing-zone", doc, key)


    @task
    def task_json():
        s3_client = Seaweedfs()
        s3_client.connect()

        apis = load_api_config()
        print("json_task")
        opendata_json = filter_apis(apis, data_type="json")

        for cfg in opendata_json:
            url = cfg["api"]
            extractor = ExtractorJSON()
            document, key = extractor.extract_data(url)
            s3_client.load_json("temporal-landing-zone", document, key)


    @task
    def task_xml():
        s3_client = Seaweedfs()
        s3_client.connect()

        apis = load_api_config()
        print("xml task")
        opendata_xml = filter_apis(apis, data_type="xml")

        for cfg in opendata_xml:
            url = cfg["api"]
            extractor = ExtractorXML()
            document, key = extractor.extract_data(url)
            s3_client.load_xml("temporal-landing-zone", document, cfg["source"] + key)


    csv_task = task_csv()
    json_task = task_json()
    xml_task = task_xml()

    [csv_task, json_task, xml_task]


landing_zone_dag()