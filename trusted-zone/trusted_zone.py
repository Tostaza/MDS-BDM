"""
Punto de entrada: Spark + S3A (SeaweedFS) + Cleaner.

Descubre de forma recursiva carpetas que son “raíces” de un dataset Parquet bajo
persistent-landing-zone, limpia cada una y escribe en trusted-zone.

Casos especials (veure ``trusted_pipelines.py``):
- ``transit_posts/`` (Nitter / Delta per post): es fusionen totes les arrels en un sol
  Parquet a ``trusted-zone/.../transit_posts/`` amb camps plans per retrieval.
- ``sct/traffic_incidents/``: enriqueix ``xml_content`` i dedup per ``file_name``.
- ``aemet/``: predicció horària → una fila per hora (columnes planes).
- La resta: mateixa jerarquia relativa Parquet → Parquet amb ``Cleaner``.

Ejecución típica (desde la raíz del repo):

    python trusted-zone/trusted_zone.py

    spark-submit (Docker): cal que els workers puguin importar ``trusted_pipelines`` (UDF XML SCT).
    Opció A — afegir la carpeta del job al PYTHONPATH:

    export PYTHONPATH=/opt/spark/work-dir/trusted-zone:/opt/spark/work-dir:/opt/spark/work-dir/dags
    spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/trusted-zone/trusted_zone.py

    Opció B — ``trusted_zone.main`` ja crida ``addPyFile`` per a ``cleaner.py`` i ``trusted_pipelines.py``
    després de crear la sessió (no cal PYTHONPATH extra per a aquest UDF).

Variables de entorno opcionales:
    S3A_ENDPOINT          URL del API S3 de SeaweedFS (ej. http://seaweedfs:8333 en Docker)
    S3A_ACCESS_KEY        clave S3 (por defecto admin)
    S3A_SECRET_KEY        secreto S3 (por defecto admin)
    SPARK_MASTER          ej. local[*], spark://spark-master:7077
    PERSISTENT_BUCKET     bucket origen (por defecto persistent-landing-zone)
    TRUSTED_BUCKET        bucket destino (por defecto trusted-zone)
    DATA_PREFIX           prefijo común dentro de ambos buckets, ej. mi-proyecto/
    TRUSTED_TRANSIT_REL_PREFIX       prefijo relatiu landing per Twitter (default transit_posts)
    TRUSTED_SCT_INCIDENTS_REL_PREFIX prefijo SCT XML (default sct/traffic_incidents)
    TRUSTED_AEMET_REL_PREFIX         prefijo AEMET (default aemet)
    SCT_FILTER_BARCELONA_ONLY        true per filtrar files XML que continguin "Barcelona"
"""

from __future__ import annotations

import os
import sys
import traceback
from pathlib import Path

from pyspark.sql import SparkSession

# `trusted-zone/` (cleaner) + arrel del repo (`spark_client/`, un sol lloc reutilitzable).
_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_WORK = _SCRIPT_DIR.parent

for _p in (_REPO_WORK, _SCRIPT_DIR):
    _s = str(_p)
    if _s not in sys.path:
        sys.path.insert(0, _s)

from cleaner import CleanOptions, Cleaner  # noqa: E402
from spark_client import SparkClient  # noqa: E402
from trusted_paths import relative_path_under_base, trusted_target_uri  # noqa: E402
from trusted_pipelines import (  # noqa: E402
    clean_options_for_accidents_gu,
    partition_roots_by_pipeline,
    process_aemet_predictions_parquet,
    process_sct_traffic_incidents_parquet,
    process_transit_posts_union,
)
from traffic_cameras_media import process_traffic_cameras_persistent_to_trusted  # noqa: E402


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value is not None and value != "" else default

def ensure_bucket_exists(bucket: str) -> None:
    """
    Crea el bucket en SeaweedFS (API S3) si no existe: ``head_bucket`` y si falla ``create_bucket``.

    Usa ``S3A_ENDPOINT`` o ``S3_ENDPOINT``, y ``S3A_ACCESS_KEY`` / ``S3A_SECRET_KEY`` o bien
    ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` (para alinear con ``docker exec`` y Airflow).
    """
    import boto3
    from botocore.exceptions import ClientError

    endpoint = _env("S3A_ENDPOINT", _env("S3_ENDPOINT", "http://seaweedfs:8333"))
    access_key = _env("S3A_ACCESS_KEY", _env("AWS_ACCESS_KEY_ID", "admin"))
    secret_key = _env("S3A_SECRET_KEY", _env("AWS_SECRET_ACCESS_KEY", "admin"))

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' exists.")
    except ClientError:
        s3.create_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' created.")


def _register_local_pyfiles_for_udf_workers(spark: SparkSession) -> None:
    """
    Els UDF de PySpark es deserialitzen als workers amb cloudpickle i exigeixen poder fer
    ``import trusted_pipelines`` (i ``cleaner``, importat per aquest mòdul). El driver ja té
    ``trusted-zone/`` a ``sys.path``; els workers no, llevat que es distribueixin els ``.py``.
    """
    zone = Path(__file__).resolve().parent
    sc = spark.sparkContext
    for filename in ("cleaner.py", "trusted_pipelines.py"):
        path = zone / filename
        if path.is_file():
            sc.addPyFile(str(path))


def build_spark_session(app_name: str = "trusted-zone-landing-to-trusted") -> SparkClient:
    """
    Crea un SparkClient configurado para S3A/SeaweedFS.

    El endpoint por defecto usa el hostname Docker `seaweedfs`. Si ejecutas Spark
    en tu máquina contra SeaweedFS expuesto en el puerto 8333, define S3A_ENDPOINT
    con http://127.0.0.1:8333 (o el host que uses).
    """
    return SparkClient(
        app_name=app_name,
        master=_env("SPARK_MASTER", "local[*]"),
        endpoint=_env("S3A_ENDPOINT", "http://seaweedfs:8333"),
        access_key=_env("S3A_ACCESS_KEY", "admin"),
        secret_key=_env("S3A_SECRET_KEY", "admin"),
    )


def landing_base_uri() -> str:
    """URI base de lectura (todo lo que cuelga de aquí se escanea)."""
    bucket = _env("PERSISTENT_BUCKET", "persistent-landing-zone")
    prefix = _env("DATA_PREFIX", "").strip("/")
    base = f"s3a://{bucket}"
    return f"{base}/{prefix}/" if prefix else f"{base}/"


def trusted_base_uri() -> str:
    """URI base de escritura: misma estructura de prefijos que landing, otro bucket."""
    bucket = _env("TRUSTED_BUCKET", "trusted-zone")
    prefix = _env("DATA_PREFIX", "").strip("/")
    base = f"s3a://{bucket}"
    return f"{base}/{prefix}/" if prefix else f"{base}/"


def _java_s3a_path(hpath_ctor, uri: str):
    """
    Construye un org.apache.hadoop.fs.Path válido para S3A.

    Si haces rstrip('/') sobre `s3a://bucket/` obtienes `s3a://bucket` (sin path) y
    Hadoop/S3A lanza IllegalArgumentException: path must be absolute al usar exists/listStatus.
    Por eso, en la raíz del bucket forzamos siempre la barra final.
    """
    s = (uri or "").strip()
    if not s:
        raise ValueError("URI vacía para Path de Hadoop")
    if s.startswith("s3a://"):
        after_scheme = s[6:]
        if "/" not in after_scheme:
            s = f"s3a://{after_scheme}/"
    return hpath_ctor(s)


def _is_hive_partition_dir(name: str) -> bool:
    """
    Heurística tipo Hive: carpeta `clave=valor`.
    Si todas las hijas directas cumplen esto, tratamos la carpeta padre como una sola tabla particionada.
    """
    return "=" in name and not name.lower().endswith(".parquet")


def discover_parquet_roots(spark: SparkSession, base_uri: str) -> list[str]:
    """
    Recorre `base_uri` y devuelve URIs de carpetas que Spark puede leer con read.parquet(uri).

    Después de la migració a Delta, les carpetes detectades són *taules Delta*: el reader
    Delta llegirà ``_delta_log`` per resoldre la versió activa, però estructuralment una
    taula Delta no particionada té els ``.parquet`` actius a l'arrel (al costat de
    ``_delta_log/``), per la qual cosa l'heurística "hi ha parquet aquí → arrel del dataset"
    segueix sent vàlida sense canvis.

    Reglas:
    - Si en esta carpeta hay ficheros *.parquet, esa carpeta es una raíz (dataset).
    - Si solo hay subcarpetas y todas parecen particiones Hive (nombre con '=') ignorant
      ``_delta_log``, la raíz es esta carpeta (Delta particionat).
    - Si no, se sigue bajando en cada subcarpeta (saltant ``_delta_log``) y se concatenan los resultados.

    Limitación: si mezclas carpetas `año=2024` con otra carpeta que no es partición al mismo nivel,
    se intentará tratar cada rama por separado (puede no coincidir con un único read.parquet del padre).
    """
    jvm = spark.sparkContext._jvm
    hpath = jvm.org.apache.hadoop.fs.Path
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", _env("S3A_ENDPOINT", "http://seaweedfs:8333"))
    hadoop_conf.set("fs.s3a.access.key", _env("S3A_ACCESS_KEY", "admin"))
    hadoop_conf.set("fs.s3a.secret.key", _env("S3A_SECRET_KEY", "admin"))
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    base_path = _java_s3a_path(hpath, base_uri)
    fs = base_path.getFileSystem(hadoop_conf)

    if not fs.exists(base_path):
        print(f"Aviso: la ruta de escaneo no existe: {base_uri}")
        return []

    def walk(path_str: str) -> list[str]:
        p = _java_s3a_path(hpath, path_str)
        if not fs.exists(p):
            return []
        statuses = fs.listStatus(p)
        # ``_delta_log`` és el log transaccional Delta. No conté dades i no s'ha de tractar
        # com a partició ni recórrer-hi: el reader Delta ja el llegeix per la seva banda.
        dirs = [
            s for s in statuses
            if s.isDirectory() and s.getPath().getName() != "_delta_log"
        ]
        files = [
            s
            for s in statuses
            if s.isFile() and s.getPath().getName().lower().endswith(".parquet")
        ]

        # Hay Parquet en este nivel → todo el árbol bajo esta carpeta lo lee Spark como un dataset.
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
    """
    Orquestación **persistent-landing → trusted-zone**.

    Orden de ejecución:
        1. **Fase 0 (boto3, sin Spark):** imágenes ``traffic_cameras`` de persistent a trusted
           (ver ``traffic_cameras_media``). Así no depende de que Spark/Parquet fallen después.
        2. **Spark:** descubrimiento de carpetas Parquet, pipelines Twitter/SCT/AEMET/accidents GU y
           el resto con ``Cleaner.clean_delta``.

    Variables principales: ver docstring del módulo al inicio de este archivo.
    """
    landing_base = landing_base_uri()
    trusted_base = trusted_base_uri()

    # Fase 0 (boto3, sense Spark): imatges persistent → trusted. S'executa sempre primer.
    ensure_bucket_exists(_env("TRUSTED_BUCKET", "trusted-zone"))
    print("=== Fase 0: traffic_cameras (persistent-landing → trusted-zone, imágenes) ===")
    try:
        n_img = process_traffic_cameras_persistent_to_trusted()
        print(f"=== Fase 0 terminada: {n_img} imagen(es) procesada(s) ===")
    except Exception as ex:
        print(f"=== Fase 0 ERROR (imágenes): {ex} ===")
        traceback.print_exc()

    spark_client = build_spark_session()
    spark = spark_client.connect()
    _register_local_pyfiles_for_udf_workers(spark)
    try:
        cleaner = Cleaner(spark)
        options = CleanOptions()

        roots = discover_parquet_roots(spark, landing_base)
        if not roots:
            print(f"Aviso: no se encontraron carpetas Parquet bajo: {landing_base}")
        else:
            lb = landing_base.rstrip("/")
            transit_roots, sct_roots, aemet_roots, default_roots = partition_roots_by_pipeline(
                landing_base, roots
            )

            print(f"Base landing:  {landing_base}")
            print(f"Base trusted: {trusted_base}")
            print(
                f"Datasets Parquet detectados: {len(roots)} "
                f"(transit_posts={len(transit_roots)}, sct={len(sct_roots)}, "
                f"aemet={len(aemet_roots)}, otros={len(default_roots)})"
            )

            if transit_roots:
                out_transit = trusted_target_uri(trusted_base, "transit_posts")
                print(f"  → [pipeline transit_posts] {len(transit_roots)} carpetas  ==>  {out_transit}")
                process_transit_posts_union(spark, cleaner, transit_roots, out_transit)

            for root in sct_roots:
                rel = relative_path_under_base(lb, root.rstrip("/"))
                out = trusted_target_uri(trusted_base, rel)
                print(f"  → [pipeline sct/xml] {root}  ==>  {out}")
                process_sct_traffic_incidents_parquet(cleaner, root, out)

            for root in aemet_roots:
                rel = relative_path_under_base(lb, root.rstrip("/"))
                out = trusted_target_uri(trusted_base, rel)
                print(f"  → [pipeline aemet horari] {root}  ==>  {out}")
                process_aemet_predictions_parquet(cleaner, root, out)

            for root in default_roots:
                rel = relative_path_under_base(lb, root.rstrip("/"))
                out = trusted_target_uri(trusted_base, rel)
                print(f"  → {root}  ==>  {out}")
                rel_lower = rel.lower()
                if rel_lower.startswith("ajuntament_bcn/accidents_gu"):
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

            print("Limpieza y escritura Delta en trusted-zone completadas.")
    except Exception as ex:
        print(f"ERROR en pipeline Delta (trusted): {ex}")
        traceback.print_exc()
    finally:
        spark_client.close()


if __name__ == "__main__":
    main()
