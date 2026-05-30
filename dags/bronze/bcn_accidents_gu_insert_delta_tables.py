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
    dag_id='bcn_accidents_gu_temporal_to_persistent',
    default_args=default_args,
    description='Batch transformation from raw BCN Accidents GU CSVs to a SINGLE Delta table using delta-rs',
    schedule=None, # Triggered after the ingestion DAG
    start_date=datetime(2026, 4, 10),
    catchup=False,
    tags=['ajuntament_bcn', 'accidents', 'gu', 'bronze'],
)
def bcn_accidents_gu_temporal_to_persistent_pipeline():

    @task(task_id='process_bcn_accidents_gu_delta_table_task')
    def process_temporal_to_persistent():
        import pandas as pd
        from DELTA_LAKE.delta_lake import DeltaLake
        from Clients.seaweedfs import Seaweedfs

        AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID", default_var=os.getenv("AWS_ACCESS_KEY_ID"))
        AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY", default_var=os.getenv("AWS_SECRET_ACCESS_KEY"))
        S3_ENDPOINT = Variable.get("S3_ENDPOINT", default_var=os.getenv("S3_ENDPOINT"))
        LANDING_BUCKET = "landing-zone"
        LANDING_PREFIX = "ajuntament_bcn/accidents_gu/"
        TRUSTED_BUCKET = "persistent-landing-zone"
        # Una sola taula Delta amb l'històric de tots els anys.
        TRUSTED_PREFIX = "ajuntament_bcn/accidents_gu/"

        storage_options = {
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "AWS_ENDPOINT_URL": S3_ENDPOINT,
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "AWS_ALLOW_HTTP": "true"
        }

        # Numero_expedient és la clau natural de l'Ajuntament per a un accident concret;
        # garanteix idempotència si es reingesta el mateix CSV.
        merge_key_cols = ["Numero_expedient"]
        predicate = "target.Numero_expedient = source.Numero_expedient"

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

        # Coordenades UTM com a float per evitar mismatch de tipus al MERGE (CSV les guarda com a string).
        cols_float = [
            "Coordenada_UTM_X_ED50",
            "Coordenada_UTM_Y_ED50"
        ]

        frames = []
        for key in csv_keys:
            df_k = s3_client.read_csv_object(Bucket=LANDING_BUCKET, Key=key)
            for col in cols_float:
                if col in df_k.columns:
                    df_k[col] = pd.to_numeric(df_k[col], errors="coerce")
            df_k["source_file"] = key
            frames.append(df_k)

        df_new = pd.concat(frames, ignore_index=True)

        missing = [c for c in merge_key_cols if c not in df_new.columns]
        if missing:
            raise ValueError(
                f"accidents_gu: falten columnes obligatòries per al MERGE {missing!r} "
                f"al batch de {len(csv_keys)} CSV(s)."
            )

        # Dedup intra-batch sobre la clau MERGE: si dos CSVs anuals reportessin el mateix
        # Numero_expedient (poc probable però possible en talls de fiscal year), keep="last".
        df_new = df_new.drop_duplicates(subset=merge_key_cols, keep="last")

        s3_client.create_bucket(TRUSTED_BUCKET)
        persistent_path = f"s3://{TRUSTED_BUCKET}/{TRUSTED_PREFIX}"
        delta_lake.connect_and_load(df_new, persistent_path, storage_options, predicate)
        print(
            f"accidents_gu: MERGE únic de {len(df_new)} accidents (de {len(csv_keys)} CSVs) "
            f"→ {persistent_path}"
        )

    process_temporal_to_persistent()

bcn_accidents_gu_temporal_to_persistent_pipeline()
