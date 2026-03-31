from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="pipeline_general",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    ejecutar_landing = TriggerDagRunOperator(
        task_id="landing_zone_dag",
        trigger_dag_id="landing_zone_dag",
        wait_for_completion=True
    )
    
    ejecutar_landing_real_time = TriggerDagRunOperator(
        task_id = "landing_zone_dag_real_time",
        trigger_dag_id = "landing_zone_dag_real_time",
        wait_for_completion = True
    )
    
    [ejecutar_landing, ejecutar_landing_real_time]