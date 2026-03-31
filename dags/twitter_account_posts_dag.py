from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import feedparser
import json
# from kafka import KafkaProducer
import boto3
import os


default_args = {
    'owner': 'upc',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

# def fetch(url: str) -> dict:
#     feed = feedparser.parse(url)
    
#     if feed.bozo:
#         raise Exception(f"Error while parsing feed: {feed.bozo_exception}")
    
#     return feed.entries


def fetch_and_ingest_rss(source_id, variable_name, rss_url, s3_client, bucket):
    from datetime import datetime
    from email.utils import parsedate_to_datetime

    latest_post_timestamp = datetime.fromisoformat(Variable.get(f"{source_id}_{variable_name}", default_var="2026-03-21T00:00:00+00:00"))
    
    print(f"latest_post_timestamp: {latest_post_timestamp}")
    
    feed = feedparser.parse(rss_url)

    new_posts = []
    latest_post_timestamp_in_batch = latest_post_timestamp
    
    for entry in feed.entries:
        post_date = parsedate_to_datetime(entry.published)
        
        if post_date > latest_post_timestamp:
            new_posts.append(entry)

            if post_date > latest_post_timestamp_in_batch:
                latest_post_timestamp_in_batch = post_date
                
    if not new_posts:
        print("New posts not found")
        return
    else:
        print(f"{len(new_posts)} new posts found")

    # --- INYECTAR A KAFKA (Hot Path) ---
    # producer = KafkaProducer(bootstrap_servers=['kafka:9092'], 
    #                          value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    # for post in nuevos_posts:
    #     producer.send('transit_tweets_topic', post)
    # producer.flush()

    for p in new_posts:
        dt = parsedate_to_datetime(p.published)
        timestamp_suffix = dt.isoformat()

        key = f"{source_id}/post_{timestamp_suffix}.json"
        s3_client.put_object(Bucket=bucket, Key=key, Body=json.dumps(p))

    Variable.set(f"{source_id}_{variable_name}", latest_post_timestamp_in_batch.isoformat())


with DAG(
    'transit_rss_ingestion',
    default_args=default_args,
    description='Micro-batching ingestion of RSS feed posts from Transit Twitter account',
    schedule="*/5 * * * *", # Frequency: Every 5 minutes
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['twitter', "social_media", 'ingestion'],
) as dag:

    # TODO: USE ENV VARIABLES TO PASS AWS CREDENTIALS

    s3_client = boto3.client(
        "s3",
        aws_access_key_id= "admin", # os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key= "admin", # os.getenv("AWS_SECRET_ACCESS_KEY"),
        endpoint_url= "http://seaweedfs:8333" # f"http://{os.getenv("S3_ENDPOINT")}"
    )

    ingest_task = PythonOperator(
        task_id='fetch_and_ingest_rss_task',
        python_callable=fetch_and_ingest_rss,
        op_kwargs={
            'source_id': "transit_posts",
            'variable_name': "last_post_timestamp",
            'rss_url': "https://nitter.net/transit/rss",
            #'kafka_topic': "transit_tweets_topic",
            's3_client': s3_client,
            'bucket': "landing-zone"
        }
    )

    ingest_task
