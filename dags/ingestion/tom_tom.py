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
        import pandas as pd
        import io

        TEMPORAL_LANDING_BUCKET = "temporal-landing-zone"
        client = Seaweedfs()

        API_KEY = "Ai9JDYMoWhb7kxbTynVYrXRnJxQdXAGa"
        url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
        points = load_json_points()

        data = []
        for point in points:
            
            point_to_search = f"{point['lat']},{point['lon']}"
            print(point_to_search)
            params = {
                "key": API_KEY,
                "point": point_to_search,
                "unit": "KMPH",
                "openLr": "false",
            }
            response = requests.get(url, params=params, timeout=30)
            response = response.json()
            flow = response["flowSegmentData"]

            data.append({
                "currentSpeed": flow["currentSpeed"],
                "free_flow_speed": flow["freeFlowSpeed"],
                "currentTravelTime": flow["currentTravelTime"],
                "free_flow_time": flow["freeFlowTravelTime"],
                "road_close": flow["roadClosure"],
                "latitude": point['lat'],
                "longitude": point['lon']
            })

        df = pd.DataFrame(data)
        key = "TOMTOM/tomtom_maps_coordinator.csv"
        client.upload_dataframe(TEMPORAL_LANDING_BUCKET, key, df)


    trigger_temporal_to_persistent_task = TriggerDagRunOperator(
        task_id='tomtom_trigger_temporal_to_persistent',
        trigger_dag_id='tomtom_temporal_to_persistent',
        wait_for_completion=False
    )

    ingest_data() >> trigger_temporal_to_persistent_task

ingest_trams_congestion()
