from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import feedparser
import json
from kafka import KafkaProducer
import boto3 # Librería estándar para interactuar con MinIO/S3

# 1. Definir los argumentos por defecto del DAG
default_args = {
    'owner': 'bdm_student',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def fetch(url: str) -> dict:
    feed = feedparser.parse(url)
    
    if feed.bozo:
        raise Exception(f"Error while parsing feed: {feed.bozo_exception}")
    
    return feed.entries


def fetch_and_ingest_rss(**kwargs):
    last_processed_date = Variable.get("transit_last_timestamp", default_var="2000-01-01T00:00:00Z")
    
    rss_url = "https://nitter.net/transit/rss"
    feed = feedparser.parse(rss_url)
    
    nuevos_posts = []
    latest_date_in_batch = last_processed_date
    
    # Filtrar los posts nuevos
    for entry in feed.entries:
        post_date = entry.published
        
        if post_date > last_processed_date:
            nuevos_posts.append({
                "id": entry.id,
                "text": entry.title,
                "link": entry.link,
                "published_at": post_date
            })
            # Actualizar cuál es la fecha más reciente vista en este batch
            if post_date > latest_date_in_batch:
                latest_date_in_batch = post_date
                
    if not nuevos_posts:
        print("No hay nuevos tweets para procesar.")
        return

    print(f"Se han encontrado {len(nuevos_posts)} nuevos tweets.")

    # --- INYECTAR A KAFKA (Hot Path) ---
    # producer = KafkaProducer(bootstrap_servers=['kafka:9092'], 
    #                          value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    # for post in nuevos_posts:
    #     producer.send('transit_tweets_topic', post)
    # producer.flush()

    # --- GUARDAR EN MINIO / LANDING ZONE (Cold Path) ---
    # Recuerda que en la Landing Zone los datos se extraen en su forma cruda y se cargan sin transformaciones significativas[cite: 37].
    # s3_client = boto3.client('s3', endpoint_url='http://minio:9000', 
    #                          aws_access_key_id='TU_USUARIO', 
    #                          aws_secret_access_key='TU_CONTRASEÑA')
    # file_name = f"transit_tweets_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    # s3_client.put_object(Bucket='landing-zone', Key=f'transit/{file_name}', 
    #                      Body=json.dumps(nuevos_posts))

    # Actualizar la variable para la próxima ejecución de Airflow
    Variable.set("transit_last_timestamp", latest_date_in_batch)
    print(f"Variable actualizada a: {latest_date_in_batch}")


with DAG(
    'transit_rss_ingestion',
    default_args=default_args,
    description='Ingesta de micro-batches desde Nitter RSS para Transit',
    schedule=None, # Change it for more frequency
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['bdm', 'ingestion', 'twitter'],
) as dag:

    # 4. Definir la tarea
    ingest_task = PythonOperator(
        task_id='fetch_and_ingest_rss_task',
        python_callable=fetch_and_ingest_rss,
        # provide_context=True
    )

    ingest_task
