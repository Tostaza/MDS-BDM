import os
import json
import requests
import boto3
from datetime import datetime, timedelta
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
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='tom_tom_trams_information',
    default_args=default_args,
    schedule="*/1 * * * *",
    catchup=False,
    tags=['tom_tom', 'trams', 'transit'],
)
def ingest_trams_congestion():

    @task
    def ingest_data():
        from Clients.seaweedfs import Seaweedfs
        import pandas as pd
        import io

        TEMPORAL_LANDING_BUCKET = "temporal-landing-zone"
        client = Seaweedfs()

        API_KEY = os.environ.get('tomtomtoken')
        url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
        points = load_json_points()

        data = []
        for point in points:
            point_to_search = (point["lat"], point["lon"])
            params = {
                "key": API_KEY,
                "point": point_to_search,
                "unit": "KMPH",
                "openLr": "false",
            }
            response = requests.get(url, params=params, timeout=30)
            response = response.json
            print(response)
            data.append({"current_speed" : response.currentSpeed,  "free_flow_speed": response.freeFlowSpeed, "currentTravelTime": response.currentTravelTime, "free_flow_time": response.freeFlowTravelTime, "road_close":response.roadClosure})

        df = pd.Dataframe(data)
        key = "TOMTOM/tomtom_maps_coordinator"
        client.upload_dataframe(TEMPORAL_LANDING_BUCKET, key, df)


    ingest_data()

ingest_trams_congestion()
