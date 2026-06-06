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
    dag_id='twitter_transit_temporal_to_persistent',
    default_args=default_args,
    description='Batch transformation from raw RSS posts JSON to a SINGLE Delta table (one row per tweet) using delta-rs',
    schedule=None, # Triggered by the ingestion DAG (every 5 min)
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['twitter', "social_media", "bronze"],
)
def transit_temporal_to_persistent_pipeline():

    @task(task_id='process_delta_table_task')
    def process_temporal_to_persistent():
        import pandas as pd
        from DELTA_LAKE.delta_lake import DeltaLake
        from Clients.seaweedfs import Seaweedfs

        AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID", default_var=os.getenv("AWS_ACCESS_KEY_ID"))
        AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY", default_var=os.getenv("AWS_SECRET_ACCESS_KEY"))
        S3_ENDPOINT = Variable.get("S3_ENDPOINT", default_var=os.getenv("S3_ENDPOINT"))

        LANDING_BUCKET = "landing-zone"
        LANDING_PREFIX = "transit_posts/"
        TRUSTED_BUCKET = "persistent-landing-zone"
        # Una sola taula Delta per font (un row per tweet), no una taula per fitxer.
        TRUSTED_PREFIX = "transit_posts/"

        storage_options = {
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "AWS_ENDPOINT_URL": S3_ENDPOINT,
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "AWS_ALLOW_HTTP": "true",
        }

        # `link` és l'únic identificador estable d'un tweet a Nitter (id RSS pot canviar de format).
        merge_key_cols = ["link"]
        predicate = "target.link = source.link"

        s3_client = Seaweedfs()
        s3_client.connect()
        delta_lake = DeltaLake()

        json_keys = s3_client.list_object_keys(
            Bucket=LANDING_BUCKET,
            Prefix=LANDING_PREFIX,
            suffix=".json",
        )
        if not json_keys:
            print("No new files found in the Landing Zone.")
            return

        # Acumulem TOTS els fitxers del batch en un sol DataFrame i fem UNA sola MERGE.
        # Així el log transaccional Delta creix d'un commit per batch en comptes d'un per fitxer.
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

        # Validació: si la clau natural del MERGE no apareix, parar amb error clar (millor que
        # crear una taula corrupta o silenciar duplicats).
        missing = [c for c in merge_key_cols if c not in df_new.columns]
        if missing:
            raise ValueError(
                f"transit_posts: falten columnes obligatòries per al MERGE {missing!r} "
                f"al batch de {len(json_keys)} fitxers."
            )

        # delta-rs rebutja el MERGE si la `source` té múltiples files que casen amb la mateixa
        # `target`. Si dins del mateix batch hi ha dos fitxers amb el mateix tweet (Nitter reenvia
        # entrades), ens quedem amb l'últim observat per tenir ingest determinista.
        df_new = df_new.drop_duplicates(subset=merge_key_cols, keep="last")

        s3_client.create_bucket(TRUSTED_BUCKET)
        persistent_path = f"s3://{TRUSTED_BUCKET}/{TRUSTED_PREFIX}"
        delta_lake.connect_and_load(df_new, persistent_path, storage_options, predicate)
        print(
            f"transit_posts: MERGE únic de {len(df_new)} files (de {len(json_keys)} fitxers landing) "
            f"→ {persistent_path}"
        )

    process_temporal_to_persistent()

transit_temporal_to_persistent_pipeline()
