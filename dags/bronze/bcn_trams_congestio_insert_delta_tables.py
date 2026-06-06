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
    dag_id='bcn_trams_congestion_temporal_to_persistent',
    default_args=default_args,
    description='Batch transformation from raw BCN Trams Congestion CSVs to a SINGLE Delta table (history per tram per timestamp) using delta-rs',
    schedule=None, # Triggered after the ingestion DAG
    start_date=datetime(2026, 4, 10),
    catchup=False,
    tags=['ajuntament_bcn', 'trams_congestion', 'bronze'],
)
def bcn_trams_congestion_temporal_to_persistent_pipeline():

    @task(task_id='process_bcn_trams_congestion_delta_table_task')
    def process_temporal_to_persistent():
        import pandas as pd
        from Clients.seaweedfs import Seaweedfs
        from DELTA_LAKE.delta_lake import DeltaLake

        S3_ENDPOINT = Variable.get("S3_ENDPOINT", default_var=os.getenv("S3_ENDPOINT"))
        AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID", default_var=os.getenv("AWS_ACCESS_KEY_ID"))
        AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY", default_var=os.getenv("AWS_SECRET_ACCESS_KEY"))
        LANDING_BUCKET = "landing-zone"
        LANDING_PREFIX = "ajuntament_bcn/trams_congestion/"
        TRUSTED_BUCKET = "persistent-landing-zone"
        # Una sola taula Delta amb totes les lectures històriques per tram i timestamp.
        TRUSTED_PREFIX = "ajuntament_bcn/trams_congestion/"

        storage_options = {
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "AWS_ENDPOINT_URL": S3_ENDPOINT,
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "AWS_ALLOW_HTTP": "true"
        }

        # MERGE per (idTram, data): cada lectura de congestió d'un tram en un instant és una fila
        # diferent — preservem història. El predicat antic `idTram` sol hauria *sobreescrit* l'última
        # lectura de cada tram a cada batch, destruint l'històric. El disseny per-fitxer ho ocultava
        # perquè cada CSV anava a la seva pròpia taula.
        merge_key_cols = ["idTram", "data"]
        predicate = "target.idTram = source.idTram AND target.data = source.data"

        s3_client = Seaweedfs()
        s3_client.connect()
        delta_lake = DeltaLake()

        csv_keys = s3_client.list_object_keys(
            Bucket=LANDING_BUCKET,
            Prefix=LANDING_PREFIX,
            suffix=".csv",
        )

        if not csv_keys:
            print("No s'han trobat dades noves per processar.")
            return

        frames = []
        for key in csv_keys:
            df_k = s3_client.read_csv_object(Bucket=LANDING_BUCKET, Key=key)
            df_k = df_k.dropna(axis=1, how='all')
            df_k["source_file"] = key
            frames.append(df_k)

        df_new = pd.concat(frames, ignore_index=True)

        missing = [c for c in merge_key_cols if c not in df_new.columns]
        if missing:
            raise ValueError(
                f"trams_congestion: falten columnes obligatòries per al MERGE {missing!r} "
                f"al batch de {len(csv_keys)} CSV(s). Si el camp temporal no és 'data', "
                f"actualitza `merge_key_cols`/`predicate` segons l'esquema real."
            )

        df_new = df_new.drop_duplicates(subset=merge_key_cols, keep="last")

        s3_client.create_bucket(TRUSTED_BUCKET)
        persistent_path = f"s3://{TRUSTED_BUCKET}/{TRUSTED_PREFIX}"
        delta_lake.connect_and_load(df_new, persistent_path, storage_options, predicate)
        print(
            f"trams_congestion: MERGE únic de {len(df_new)} lectures (de {len(csv_keys)} CSVs) "
            f"→ {persistent_path}"
        )

    process_temporal_to_persistent()

bcn_trams_congestion_temporal_to_persistent_pipeline()
