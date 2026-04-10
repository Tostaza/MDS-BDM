from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from airflow.decorators import dag


@dag(
    dag_id="pipeline_general",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pipeline"]
)
def pipeline_general_dag():

    ejecutar_landing = TriggerDagRunOperator(
        task_id="landing_zone_dag",
        trigger_dag_id="landing_zone_dag",
        wait_for_completion=True
    )

    ejecutar_landing


pipeline_general_dag()