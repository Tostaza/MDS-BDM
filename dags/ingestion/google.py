import os
import json
import requests
import boto3
from datetime import datetime, timedelta, timezone
from airflow.decorators import dag, task  # pyright: ignore[reportMissingImports]
from airflow.models import Variable  # pyright: ignore[reportMissingImports]
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator  # pyright: ignore[reportMissingImports]
from dotenv import load_dotenv
RUTA_JSON = os.path.join(os.path.dirname(__file__), "..", "JSON", "points.json")


def seconds_from_duration(dur_str: str) -> float:
    return float(dur_str.rstrip("s"))

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
    dag_id='google_trams_information',
    default_args=default_args,
    schedule="*/5 * * * *",
    catchup=False,
    tags=['google', 'trams', 'transit', 'scheduled', 'ingestion'],
)
def ingest_trams_congestion():

    @task
    def ingest_data():
        from Clients.seaweedfs import Seaweedfs
        import pandas as pd
        import io

        TEMPORAL_LANDING_BUCKET = "temporal-landing-zone"
        client = Seaweedfs()
        load_dotenv()

        data = []
        url = "https://routes.googleapis.com/directions/v2:computeRoutes"
        API_KEY = "AIzaSyCVhaVruGaRYU6jRW1g1wdH-ty4_hZHELU"
        print(API_KEY)
        print("1")
        headers = {
            "X-Goog-Api-Key": API_KEY,
            "X-Goog-FieldMask": "routes.duration,routes.staticDuration,routes.distanceMeters",
            "Content-Type": "application/json"
        }

        points = load_json_points()
        for i in range(len(points) - 1):
            origin = points[i]
            destination = points[i + 1]

            body = {
                "origin": {
                    "location": {
                        "latLng": {
                            "latitude": origin["lat"],
                            "longitude": origin["lon"]
                        }
                    }
                },
                "destination": {
                    "location": {
                        "latLng": {
                            "latitude": destination["lat"],
                            "longitude": destination["lon"]
                        }
                    }
                },
                "travelMode": "DRIVE",
                "routingPreference": "TRAFFIC_AWARE_OPTIMAL"
            }
        
            response = requests.post(url, headers=headers, data=json.dumps(body))
            response_data = response.json()
            print(response_data)
            route = response_data["routes"][0]
            distance_m = route["distanceMeters"]
            distance_km = distance_m / 1000
            dur_s = seconds_from_duration(route["duration"])
            static_dur_s = seconds_from_duration(route["staticDuration"])
            dur_h = dur_s / 3600
            static_dur_h = static_dur_s / 3600
            speed_traffic_kmh = distance_km / dur_h if dur_h > 0 else None
            speed_free_kmh = distance_km / static_dur_h if static_dur_h > 0 else None

            result = {
                "Km": distance_km,
                "duration_seconds": dur_s,
                "static_duration": static_dur_s,
                "duration_hours": dur_h,
                "static_duration_h": static_dur_h,
                "current_speed": speed_traffic_kmh,
                "free_speed": speed_free_kmh
            }
            data.append({
                "origin_lat": origin["lat"],
                "origin_lng": origin["lon"],
                "dest_lat": destination["lat"],
                "dest_lng": destination["lon"],
                "Km": distance_km,
                "duration_seconds": dur_s,
                "static_duration": static_dur_s,
                "duration_hours": dur_h,
                "static_duration_h": static_dur_h,
                "current_speed": speed_traffic_kmh,
                "free_speed": speed_free_kmh
            })

        df = pd.DataFrame(data)
        key = "Google/google_maps_coordinator.csv"
        client.upload_dataframe(TEMPORAL_LANDING_BUCKET, key, df)

    trigger_temporal_to_persistent_task = TriggerDagRunOperator(
        task_id='google_trigger_temporal_to_persistent',
        trigger_dag_id='google_temporal_to_persistent',
        wait_for_completion=False
    )

    ingest_data() >> trigger_temporal_to_persistent_task

ingest_trams_congestion()
