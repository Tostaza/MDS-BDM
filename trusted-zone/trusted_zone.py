"""
Punt d'entrada: llegeix Delta de persistent-landing-zone i escriu a trusted-zone.
Pipelines especials: transit_posts, sct/traffic_incidents, aemet, accidents GU.
Env vars: S3A_ENDPOINT, S3A_ACCESS_KEY, S3A_SECRET_KEY, SPARK_MASTER, PERSISTENT_BUCKET, TRUSTED_BUCKET, DATA_PREFIX, SCT_FILTER_BARCELONA_ONLY
"""
from __future__ import annotations
import os
import sys
import traceback
from pathlib import Path
from pyspark.sql import SparkSession

# Per poder trobar els imports dels diferents fitxers que faré servir.
_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_WORK = _SCRIPT_DIR.parent
for _p in (_REPO_WORK, _SCRIPT_DIR):
    _s = str(_p)
    if _s not in sys.path:
        sys.path.insert(0, _s)

from cleaner import CleanOptions, Cleaner  # noqa: E402
from spark_client import SparkClient  # noqa: E402
from trusted_paths import relative_path_under_base, trusted_target_uri  # noqa: E402
from trusted_pipelines import (clean_options_for_accidents_gu, partition_roots_by_pipeline, process_aemet_predictions_parquet, process_sct_traffic_incidents_parquet, process_transit_posts) # noqa: E402
from traffic_cameras_media import process_traffic_cameras_persistent_to_trusted  # noqa: E402
import boto3
from botocore.exceptions import ClientError


def _env(name: str, default: str) -> str:
    """Per carregar les variables del .env"""
    value = os.environ.get(name)
    return value if value is not None and value != "" else default

def ensure_bucket_exists(bucket: str) -> None:
    """Crea el bucket amb name (bucket:str) que es passa per parametre a SeaweedFS si no existeix."""

    endpoint = _env("S3A_ENDPOINT", _env("S3_ENDPOINT", "http://seaweedfs:8333"))
    access_key = _env("S3A_ACCESS_KEY", _env("AWS_ACCESS_KEY_ID", "admin"))
    secret_key = _env("S3A_SECRET_KEY", _env("AWS_SECRET_ACCESS_KEY", "admin"))
    s3 = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' exists.")
    except ClientError:
        s3.create_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' created.")

def _register_local_pyfiles_for_udf_workers(spark: SparkSession) -> None:
    """Distribueix cleaner.py i trusted_pipelines.py als workers per als UDFs. Passar als workers tots els udf i així trobin les funcions i ho puguin executar."""
    zone = Path(__file__).resolve().parent
    sc = spark.sparkContext
    for filename in ("cleaner.py", "trusted_pipelines.py"):
        path = zone / filename
        if path.is_file():
            sc.addPyFile(str(path))

def build_spark_session(app_name: str = "trusted-zone-landing-to-trusted") -> SparkClient:
    """Crea un SparkClient configurat per S3A/SeaweedFS."""
    return SparkClient(
        app_name=app_name,
        master=_env("SPARK_MASTER", "local[*]"),
        endpoint=_env("S3A_ENDPOINT", "http://seaweedfs:8333"),
        access_key=_env("S3A_ACCESS_KEY", "admin"),
        secret_key=_env("S3A_SECRET_KEY", "admin"),
    )

def landing_base_uri() -> str:
    """Genera la URL del origin on anirem a buscar-ne les diferents dades i els diferents parquet files. EN AQUEST CAS PERSITENT-LANDING-ZONE."""
    bucket = _env("PERSISTENT_BUCKET", "persistent-landing-zone")
    prefix = _env("DATA_PREFIX", "").strip("/")
    base = f"s3a://{bucket}"
    return f"{base}/{prefix}/" if prefix else f"{base}/"

def trusted_base_uri() -> str:
    """Generem la URL de output, del bucket Destí. En aquest cas on guardarem les dades i la informació. EN AQUES CAS TRUSTED ZONE"""
    bucket = _env("TRUSTED_BUCKET", "trusted-zone")
    prefix = _env("DATA_PREFIX", "").strip("/")
    base = f"s3a://{bucket}"
    return f"{base}/{prefix}/" if prefix else f"{base}/"

def _java_s3a_path(hpath_ctor, uri: str):
    """ Construeix un Path Hadoop S3A garantint barra final a l'arrel del bucket.
        Per paths a buckets, hadoop necessiten que siguin absoluts, per tant amb un slice al final /. """
    s = (uri or "").strip()
    if not s:
        raise ValueError("URI buida per a Path de Hadoop")
    if s.startswith("s3a://"):
        after_scheme = s[6:]
        if "/" not in after_scheme:
            s = f"s3a://{after_scheme}/"
    return hpath_ctor(s)

def _is_hive_partition_dir(name: str) -> bool:
    """ Retorna True si el nom de carpeta segueix el patro clau=valor de Hive.
        Em permetrà poder detectar diferencies entre diferents carpetes, és a dir dins de folders si tinc subfolders com any = 2024
        em detecta aquestes subfolders filtrades per any.
    """
    return "=" in name and not name.lower().endswith(".parquet")

def discover_parquet_roots(spark: SparkSession, base_uri: str) -> list[str]:
    """
    Recorre base_uri i retorna les URIs que Spark pot llegir com a dataset Parquet/Delta.
    Regles: si hi ha .parquet -> arrel; si tots els fills son particions Hive -> arrel; si no, baixa.
    """
    jvm = spark.sparkContext._jvm
    hpath = jvm.org.apache.hadoop.fs.Path
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    # AQUI ESTAREM DIENT-LI QUE FACI LA CONNEXIÓ HADOOP AMB EL NOSTRE s3.
    # Fixa't que estem passant-li les credencials per a que es pugui connectar.
    hadoop_conf.set("fs.s3a.endpoint", _env("S3A_ENDPOINT", "http://seaweedfs:8333"))
    hadoop_conf.set("fs.s3a.access.key", _env("S3A_ACCESS_KEY", "admin"))
    hadoop_conf.set("fs.s3a.secret.key", _env("S3A_SECRET_KEY", "admin"))
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    base_path = _java_s3a_path(hpath, base_uri) # Aqui sera doncs la url incial a persistent-landing-zone.
    fs = base_path.getFileSystem(hadoop_conf)   # Aqui tindrem el flat name de S3 per poder accedir-ne a carpetes i els seus continguts.

    if not fs.exists(base_path):
        print(f"Aviso: la ruta de escaneo no existe: {base_uri}")
        return []

    def walk(path_str: str) -> list[str]:
        """
        Serveix per trobar de tots els directoris, totes les carpetes i de aquestes tots els fitxers.
        """
        p = _java_s3a_path(hpath, path_str)
        if not fs.exists(p):
            return []
        statuses = fs.listStatus(p)
        # ignorem _delta_log: es el log transaccional de Delta, no conte dades
        dirs = [s for s in statuses if s.isDirectory() and s.getPath().getName() != "_delta_log"]
        files = [ s for s in statuses if s.isFile() and s.getPath().getName().lower().endswith(".parquet")]
        
        if files:
            return [p.toString() + "/"]
        if not dirs:
            return []
        names = [s.getPath().getName() for s in dirs]
        if dirs and all(_is_hive_partition_dir(n) for n in names):
            return [p.toString() + "/"]
        roots: list[str] = []
        for s in dirs:
            roots.extend(walk(s.getPath().toString()))
        return roots
    return walk(base_path.toString())


def main() -> None:
    """Orquestra persistent-landing -> trusted-zone: primer imatges (boto3), despres Spark."""
    landing_base = landing_base_uri()
    trusted_base = trusted_base_uri()
    ensure_bucket_exists(_env("TRUSTED_BUCKET", "trusted-zone"))

    try:
        n_img = process_traffic_cameras_persistent_to_trusted()
        print(f"=== Fase 0 terminada: {n_img} imagen(es) procesada(s) ===")
    except Exception as ex:
        print(f"=== Fase 0 ERROR (imagens): {ex} ===")
        traceback.print_exc()

    spark_client = build_spark_session()
    spark = spark_client.connect()
    _register_local_pyfiles_for_udf_workers(spark)
    try:
        cleaner = Cleaner(spark)
        options = CleanOptions()

        roots = discover_parquet_roots(spark, landing_base)
        print(roots)
        if not roots:
            print(f"Avis: no se han trobat carpetas Parquet sota: {landing_base}")
        else:
            lb = landing_base.rstrip("/")
            transit_roots, sct_roots, aemet_roots, default_roots = partition_roots_by_pipeline(landing_base, roots)

            if transit_roots:
                try:
                    out_transit = trusted_target_uri(trusted_base, "transit_posts")
                    process_transit_posts(spark, cleaner, transit_roots[0], out_transit)
                except Exception as ex:
                    print(f"ERROR pipeline transit_posts: {ex}")
                    traceback.print_exc()

            for root in sct_roots:
                try:
                    rel = relative_path_under_base(lb, root.rstrip("/"))
                    out = trusted_target_uri(trusted_base, rel)
                    process_sct_traffic_incidents_parquet(cleaner, root, out)
                except Exception as ex:
                    print(f"ERROR pipeline sct {root}: {ex}")
                    traceback.print_exc()

            for root in aemet_roots:
                try:
                    rel = relative_path_under_base(lb, root.rstrip("/"))
                    out = trusted_target_uri(trusted_base, rel)
                    process_aemet_predictions_parquet(cleaner, root, out)
                except Exception as ex:
                    print(f"ERROR pipeline aemet {root}: {ex}")
                    traceback.print_exc()

            for root in default_roots:
                try:
                    rel = relative_path_under_base(lb, root.rstrip("/"))
                    out = trusted_target_uri(trusted_base, rel)
                    rel_lower = rel.lower()
                    if rel_lower.startswith("ajuntament_bcn/accidents_gu"):
                        # Si es accidents_gu tenim numero_expeident, element clau, ID per el cual mirarem no duplicates mes rapid.
                        df_ag = cleaner.read_delta(root.rstrip("/") + "/")
                        opts_ag = clean_options_for_accidents_gu(df_ag)
                        cleaned_ag = cleaner.clean(df_ag, options=opts_ag)
                        cleaner.write_delta(cleaned_ag, out.rstrip("/") + "/", mode="overwrite")
                    else:
                        cleaner.clean_delta(
                            input_path=root,
                            output_path=out,
                            options=options,
                            write_mode="overwrite",
                        )
                    print(f"  OK: {root} -> {out}")
                except Exception as ex:
                    print(f"ERROR pipeline {root}: {ex}")
                    traceback.print_exc()

            print("Escriptura Delta a trusted-zone completada.")
    except Exception as ex:
        print(f"ERROR en pipeline Delta (trusted): {ex}")
        traceback.print_exc()
    finally:
        spark_client.close()


if __name__ == "__main__":
    main()
