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
    dag_id='bcn_relacio_trams_historic_temporal_to_persistent',
    default_args=default_args,
    description='Batch transformation from raw BCN Relacio Trams CSV(s) to a SINGLE Delta table (static dimension) using delta-rs',
    schedule=None, # Triggered after the ingestion DAG
    start_date=datetime(2026, 4, 10),
    catchup=False,
    tags=['ajuntament_bcn', 'relacio_trams', 'bronze'],
)
def bcn_relacio_trams_temporal_to_persistent_pipeline():

    @task(task_id='process_bcn_relacio_trams_delta_table_task')
    def process_temporal_to_persistent():
        import pandas as pd
        from Clients.seaweedfs import Seaweedfs
        from DELTA_LAKE.delta_lake import DeltaLake
        AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID", default_var=os.getenv("AWS_ACCESS_KEY_ID"))
        AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY", default_var=os.getenv("AWS_SECRET_ACCESS_KEY"))
        S3_ENDPOINT = Variable.get("S3_ENDPOINT", default_var=os.getenv("S3_ENDPOINT"))

        LANDING_BUCKET = "landing-zone"
        LANDING_PREFIX = "ajuntament_bcn/relacio_trams/"
        TRUSTED_BUCKET = "persistent-landing-zone"
        # Una sola taula Delta (dimensió estàtica de geometria de trams).
        TRUSTED_PREFIX = "ajuntament_bcn/relacio_trams/"

        storage_options = {
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "AWS_ENDPOINT_URL": S3_ENDPOINT,
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "AWS_ALLOW_HTTP": "true"
        }

        # `idTram` és la clau natural del catàleg de trams i el join key amb trams_congestion.
        # El predicat anterior amb (latitud, longitud) era fràgil (floats com a clau) i s'allunyava
        # del model conceptual: un tram es referencia per id, no per coordenades.
        merge_key_cols = ["idTram"]
        predicate = "target.idTram = source.idTram"

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
            df_k["source_file"] = key
            frames.append(df_k)

        df_new = pd.concat(frames, ignore_index=True)

        missing = [c for c in merge_key_cols if c not in df_new.columns]
        if missing:
            raise ValueError(
                f"relacio_trams: falta la columna identificadora {missing!r} (esperat 'idTram')."
            )

        # Dataset estàtic: si es reingesta o la font canvia, ens quedem amb la última versió per tram.
        df_new = df_new.drop_duplicates(subset=merge_key_cols, keep="last")

        s3_client.create_bucket(TRUSTED_BUCKET)
        persistent_path = f"s3://{TRUSTED_BUCKET}/{TRUSTED_PREFIX}"
        delta_lake.connect_and_load(df_new, persistent_path, storage_options, predicate)
        print(
            f"relacio_trams: MERGE únic de {len(df_new)} trams (de {len(csv_keys)} CSVs) "
            f"→ {persistent_path}"
        )

    process_temporal_to_persistent()

bcn_relacio_trams_temporal_to_persistent_pipeline()
