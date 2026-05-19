

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


default_args = {
    "owner": "upc",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


# Resolved on the driver host (= spark-worker in cluster mode), where
# docker-compose mounts ./trusted-zone -> /opt/spark/work-dir/trusted-zone.
TRUSTED_APPLICATION = "/opt/spark/work-dir/trusted-zone/trusted_zone.py"


@dag(
    dag_id="trusted_zone_pipeline",
    default_args=default_args,
    description="Silver layer: persistent-landing-zone -> trusted-zone (Spark)",
    schedule="@hourly",
    start_date=datetime(2026, 5, 17),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "trusted", "spark", "medallion"],
)
def trusted_zone_pipeline():
    run_trusted_zone = SparkSubmitOperator(
        task_id="spark_submit_trusted_zone",
        conn_id="spark_bdm",
        application=TRUSTED_APPLICATION,
        name="trusted-zone-airflow",
        deploy_mode="client",
        driver_memory="1g",
        execution_timeout=timedelta(minutes=60),
        verbose=False,
    )

    trigger_exploitation = TriggerDagRunOperator(
        task_id="trigger_exploitation_zone",
        trigger_dag_id="exploitation_zone_pipeline",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    run_trusted_zone >> trigger_exploitation


trusted_zone_pipeline()
