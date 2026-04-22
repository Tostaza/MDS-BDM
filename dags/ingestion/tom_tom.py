import os
import json
import requests
import boto3
from datetime import datetime, timedelta, timezone
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


RUTA_JSON = os.path.join(os.path.dirname(__file__), "..", "JSON", "points.json")

def load_json_points():
    with open(RUTA_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["points"]

default_args = {
    'owner': 'upc',
    'depends_on_past': False,
    'start_date': datetime.now(tz=timezone.utc),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='tom_tom_trams_information',
    default_args=default_args,
    schedule="*/1 * * * *",
    catchup=False,
    tags=['tom_tom', 'trams', 'transit', 'scheduled', 'ingestion'],
)
def ingest_trams_congestion():

    @task
    def ingest_data():
        from Clients.seaweedfs import Seaweedfs
        from Extractors.json import ExtractorJSON
        import pandas as pd
        import io

        LANDING_BUCKET = "landing-zone"
        client = Seaweedfs()
        client.connect()
        extractor = ExtractorJSON()

        API_KEY = "Ai9JDYMoWhb7kxbTynVYrXRnJxQdXAGa"
        url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
        points = load_json_points()
        all_data = []

        for p in points:
            # Adding mandatory 'point' parameter for TomTom flowSegmentData
            point_str = f"{p['lat']},{p['lon']}"
            res_json, _ = extractor.extract_real_time_data(url, "TomTom", API_KEY, params={"point": point_str})
            
            # Extract the actual flow information and attach point metadata
            flow_data = res_json.get("flowSegmentData", {})
            flow_data["point_id"] = p["id"]
            flow_data["point_name"] = p["name"]
            all_data.append(flow_data)

        df = pd.DataFrame(all_data)
        key = "TOMTOM/tomtom_flow_data.csv"
        client.upload_dataframe(LANDING_BUCKET, key, df)


    trigger_temporal_to_persistent_task = TriggerDagRunOperator(
        task_id='tomtom_trigger_temporal_to_persistent',
        trigger_dag_id='tomtom_temporal_to_persistent',
        wait_for_completion=False
    )

    ingest_data() >> trigger_temporal_to_persistent_task

ingest_trams_congestion()
