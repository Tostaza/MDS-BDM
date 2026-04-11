from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timezone
from airflow.models import DagModel
from airflow.utils.session import provide_session

dag_ids = [
    "bcn_relacio_trams_ingestion",
    "ingest_hf_rain_prediction",
    "bcn_historic_trams_congestion_ingestion",
    "bcn_historic_accidents_gu_ingest"
]

@dag(
    dag_id="manual_ingestion",
    schedule=None,
    start_date=datetime.now(tz=timezone.utc),
    catchup=False,
    tags=["manual", "ingestion"],
    description="Trigger all manual ingestion dags"
)
def manual_ingestion():

    trigger_ingestion_dags = TriggerDagRunOperator.partial(
        task_id="trigger_ingestion_dags",
        wait_for_completion=False, 
    ).expand(
        trigger_dag_id=dag_ids
    )

manual_ingestion()