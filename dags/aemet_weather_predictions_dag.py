import os
import json
import requests
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Argumentos por defecto para el DAG
default_args = {
    'owner': 'upc',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 28), # Ajusta a la fecha de inicio deseada
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


CITIES = {
    "Barcelona": "08019",
    "Hospitalet de Llobregat": "08101"
}

BUCKET = "landing-zone"

@dag(
    dag_id='aemet_weather_ingestion',
    default_args=default_args,
    description='Ingestion of AEMET weather predictions for Barcelona',
    schedule='0 */6 * * *', # Execute 4 times a day (every 6 hours)
    catchup=False,
    tags=['aemet', 'weather', 'ingestion'],
)
def aemet_ingestion_workflow():

    @task(task_id='fetch_aemet_data', retry_exponential_backoff=True,max_retry_delay=timedelta(minutes=1))
    def fetch_weather_predictions_hourly(city_code: str, **context):
        
        aemet_api_key = Variable.get("AEMET_API_KEY", default_var=os.getenv("AEMET_API_KEY"))
        if not aemet_api_key:
            raise ValueError("AEMET_API_KEY is missing. Configure it in Airflow Variables or as an environment variable.")

        url = f"https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/horaria/{city_code}"
        querystring = {"api_key": aemet_api_key}
        headers = {'cache-control': "no-cache"}

        print(f"Requesting data for city {city_code}...")
        response = requests.request("GET", url, headers=headers, params=querystring)
        response.raise_for_status()
        
        response_json = response.json()
        if response_json.get("estado") != 200:
            raise Exception(f"Error while retrieving data from api: {response_json.get('descripcion')}")

        
        data_url = response_json.get("datos")
        data_response = requests.request("GET", data_url, headers=headers)
        data_response.raise_for_status()
    
        return data_response.json()

    @task(task_id='store_aemet_data')
    def store_weather_data(weather_data: dict, bucket: str, **context):
        
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

        weather_data = weather_data[0]
        dt = datetime.fromisoformat(weather_data['elaborado'])
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        hour = dt.strftime("%H")

        key = f"aemet/{weather_data['id']}/{year}/{month}/{day}/{hour}_hourly_predictions.json"
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(weather_data).encode("utf-8")
        )

        print(f"Data stored for city {weather_data['id']}")

    trigger_temporal_to_persistent_task = TriggerDagRunOperator(
        task_id='trigger_temporal_to_persistent',
        trigger_dag_id='aemet_temporal_to_persistent',
        wait_for_completion=False
    )

    cities_codes = list(CITIES.values()) # This is important!
    cities_data = fetch_weather_predictions_hourly.expand(city_code=cities_codes)
    stored_data =store_weather_data.partial(bucket=BUCKET).expand(weather_data=cities_data)

    stored_data >> trigger_temporal_to_persistent_task

aemet_ingestion_workflow()