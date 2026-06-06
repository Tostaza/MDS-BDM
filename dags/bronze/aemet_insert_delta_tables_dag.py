from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models import Variable
import os

default_args = {
    'owner': 'upc',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='aemet_temporal_to_persistent',
    default_args=default_args,
    description='Batch transformation from raw AEMET JSON predictions to a SINGLE Delta table (history per municipality per forecast) using delta-rs',
    schedule=None, # Triggered after the ingestion DAG
    start_date=datetime(2026, 3, 28),
    catchup=False,
    tags=['aemet', 'weather', 'bronze'],
)
def aemet_temporal_to_persistent_pipeline():

    @task(task_id='process_aemet_delta_table_task')
    def process_temporal_to_persistent():
        import pandas as pd
        from Clients.seaweedfs import Seaweedfs
        from DELTA_LAKE.delta_lake import DeltaLake

        AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID", default_var=os.getenv("AWS_ACCESS_KEY_ID"))
        AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY", default_var=os.getenv("AWS_SECRET_ACCESS_KEY"))
        S3_ENDPOINT = Variable.get("S3_ENDPOINT", default_var=os.getenv("S3_ENDPOINT"))
        LANDING_BUCKET = "landing-zone"
        LANDING_PREFIX = "aemet/"
        TRUSTED_BUCKET = "persistent-landing-zone"
        # Una sola taula Delta amb totes les prediccions: cada nou `elaborado` afegeix files.
        TRUSTED_PREFIX = "aemet/"

        storage_options = {
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "AWS_ENDPOINT_URL": S3_ENDPOINT,
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "AWS_ALLOW_HTTP": "true"
        }

        # (id, elaborado) = (id municipi, timestamp de generació de la predicció).
        # Cada generació nova és una fila nova → preservem totes les prediccions emeses, no només
        # l'última. El predicat ja era correcte; només calia centralitzar la taula.
        merge_key_cols = ["id", "elaborado"]
        predicate = "target.id = source.id AND target.elaborado = source.elaborado"

        s3_client = Seaweedfs()
        s3_client.connect()
        delta_lake = DeltaLake()
        json_keys = s3_client.list_object_keys(
            Bucket=LANDING_BUCKET,
            Prefix=LANDING_PREFIX,
            suffix=".json",
        )
        if not json_keys:
            print("No s'han trobat fitxers JSON AEMET a landing.")
            return

        frames = []
        for key in json_keys:
            records = s3_client.read_json_records(LANDING_BUCKET, key)
            df_k = pd.DataFrame(records)
            for col in df_k.columns:
                if df_k[col].dtype == 'object':
                    df_k[col] = df_k[col].astype(str)
            df_k["source_file"] = key
            frames.append(df_k)

        df_new = pd.concat(frames, ignore_index=True)

        missing = [c for c in merge_key_cols if c not in df_new.columns]
        if missing:
            raise ValueError(
                f"aemet: falten columnes obligatòries per al MERGE {missing!r} "
                f"al batch de {len(json_keys)} JSON(s)."
            )

        # Dedup intra-batch: si el mateix municipi té dues respostes amb el mateix `elaborado`
        # (poc probable: AEMET sol incrementar el timestamp a cada crida), ens quedem amb l'última.
        df_new = df_new.drop_duplicates(subset=merge_key_cols, keep="last")

        s3_client.create_bucket(TRUSTED_BUCKET)
        persistent_path = f"s3://{TRUSTED_BUCKET}/{TRUSTED_PREFIX}"
        delta_lake.connect_and_load(df_new, persistent_path, storage_options, predicate)
        print(
            f"aemet: MERGE únic de {len(df_new)} prediccions (de {len(json_keys)} JSONs) "
            f"→ {persistent_path}"
        )

    process_temporal_to_persistent()

aemet_temporal_to_persistent_pipeline()
