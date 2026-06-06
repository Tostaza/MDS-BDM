from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
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
CAMERAS_SCRIPT = "/opt/spark/work-dir/trusted-zone/run_cameras_images.py"


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
    """
    DAG que permet fer el traspas de Persistent-landing-zone cap a Trusted Zone
    """

    # Copia imatges GIF de cameras (boto3, sense Spark), es a dir, traspas de les imatges.
    run_cameras = BashOperator(
        task_id="traffic_cameras_images_to_trusted",
        bash_command=f"python {CAMERAS_SCRIPT}",
        append_env=True,
    )

    # Execucio de trapsas de tota la informacio i dades de Persitent-lading-Zone cap a la Trusted Zone.
    # Aplicant cert cleaning i certa standarització, farem que totes les dades segueixin un esquema tabular.
    # Sera mes facil despres per poder treballar amb les dades si aqui elaborem per a totes un tabular shcema.
    run_trusted_zone = SparkSubmitOperator(
        task_id="spark_submit_trusted_zone",
        conn_id="spark_bdm",
        application=TRUSTED_APPLICATION,
        name="trusted-zone-airflow",
        deploy_mode="client",
        driver_memory="1g",
        packages="io.delta:delta-core_2.12:2.4.0",
        execution_timeout=timedelta(minutes=60),
        verbose=False,
    )

    # TRIGGER de airflow, un cop s'acaba de executar la Trusted-Zone, anem a executar la Exploitation, així generem tota la pipeline.
    trigger_exploitation = TriggerDagRunOperator(
        task_id="trigger_exploitation_zone",
        trigger_dag_id="exploitation_zone_pipeline",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    run_cameras >> run_trusted_zone >> trigger_exploitation


trusted_zone_pipeline()
