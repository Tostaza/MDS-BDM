"""
Punto de entrada: Spark + S3A (SeaweedFS) + Cleaner.

Descubre de forma recursiva carpetas que son “raíces” de un dataset Parquet bajo
persistent-landing-zone, limpia cada una y escribe el mismo path relativo en
trusted-zone (misma jerarquía de carpetas, datos limpios).

Ejecución típica (desde la raíz del repo):

    python trusted-zone/trusted-zone.py

Variables de entorno opcionales:
    S3A_ENDPOINT          URL del API S3 de SeaweedFS (ej. http://seaweedfs:8333 en Docker)
    S3A_ACCESS_KEY        clave S3 (por defecto admin)
    S3A_SECRET_KEY        secreto S3 (por defecto admin)
    SPARK_MASTER          ej. local[*], spark://spark-master:7077
    PERSISTENT_BUCKET     bucket origen (por defecto persistent-landing-zone)
    TRUSTED_BUCKET        bucket destino (por defecto trusted-zone)
    DATA_PREFIX           prefijo común dentro de ambos buckets, ej. mi-proyecto/
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

# Mismo directorio que cleaner.py (el nombre con guion impide usarlo como paquete estándar).
_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

from cleaner import CleanOptions, Cleaner  # noqa: E402


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value is not None and value != "" else default


def build_spark_session(app_name: str = "trusted-zone-landing-to-trusted") -> SparkSession:
    """
    Crea (o reutiliza) una SparkSession con el filesystem S3A apuntando a SeaweedFS.

    El endpoint por defecto usa el hostname Docker `seaweedfs`. Si ejecutas Spark
    en tu máquina contra SeaweedFS expuesto en el puerto 8333, define S3A_ENDPOINT
    con http://127.0.0.1:8333 (o el host que uses).
    """
    endpoint = _env("S3A_ENDPOINT", "http://seaweedfs:8333")
    access_key = _env("S3A_ACCESS_KEY", "admin")
    secret_key = _env("S3A_SECRET_KEY", "admin")
    master = _env("SPARK_MASTER", "local[*]")

    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
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


def _is_hive_partition_dir(name: str) -> bool:
    """
    Heurística tipo Hive: carpeta `clave=valor`.
    Si todas las hijas directas cumplen esto, tratamos la carpeta padre como una sola tabla particionada.
    """
    return "=" in name and not name.lower().endswith(".parquet")


def discover_parquet_roots(spark: SparkSession, base_uri: str) -> list[str]:
    """
    Recorre `base_uri` y devuelve URIs de carpetas que Spark puede leer con read.parquet(uri).

    Reglas:
    - Si en esta carpeta hay ficheros *.parquet, esa carpeta es una raíz (dataset).
    - Si solo hay subcarpetas y todas parecen particiones Hive (nombre con '='), la raíz es esta carpeta.
    - Si no, se sigue bajando en cada subcarpeta y se concatenan los resultados.

    Limitación: si mezclas carpetas `año=2024` con otra carpeta que no es partición al mismo nivel,
    se intentará tratar cada rama por separado (puede no coincidir con un único read.parquet del padre).
    """
    jvm = spark.sparkContext._jvm
    hpath = jvm.org.apache.hadoop.fs.Path
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    base_path = hpath(base_uri.rstrip("/"))
    fs = base_path.getFileSystem(hadoop_conf)

    if not fs.exists(base_path):
        print(f"Aviso: la ruta de escaneo no existe: {base_uri}")
        return []

    def walk(path_str: str) -> list[str]:
        p = hpath(path_str.rstrip("/"))
        if not fs.exists(p):
            return []
        statuses = fs.listStatus(p)
        dirs = [s for s in statuses if s.isDirectory()]
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


def relative_path_under_base(base_uri: str, root_uri: str) -> str:
    """Parte relativa de `root_uri` respecto a `base_uri` (sin barra inicial/final)."""
    b = base_uri.rstrip("/")
    r = root_uri.rstrip("/")
    if not r.startswith(b):
        raise ValueError(f"La raíz {root_uri!r} no cuelga de la base {base_uri!r}")
    if r == b:
        return ""
    # Tras el prefijo común debe venir una barra
    return r[len(b) + 1 :]


def trusted_target_uri(trusted_base_uri: str, relative: str) -> str:
    """Construye s3a://trusted-zone/.../relativo/ a partir de la base trusted y el sufijo."""
    t = trusted_base_uri.rstrip("/")
    if relative:
        return f"{t}/{relative}/"
    return f"{t}/"


def main() -> None:
    landing_base = landing_base_uri()
    trusted_base = trusted_base_uri()

    spark = build_spark_session()
    try:
        cleaner = Cleaner(spark)
        options = CleanOptions()

        roots = discover_parquet_roots(spark, landing_base)
        if not roots:
            print(f"No se encontraron carpetas Parquet bajo: {landing_base}")
            return

        print(f"Base landing:  {landing_base}")
        print(f"Base trusted: {trusted_base}")
        print(f"Datasets detectados: {len(roots)}")

        for root in roots:
            rel = relative_path_under_base(landing_base.rstrip("/"), root.rstrip("/"))
            out = trusted_target_uri(trusted_base, rel)
            print(f"  → {root}  ==>  {out}")
            cleaner.clean_parquet(
                input_path=root,
                output_path=out,
                options=options,
                write_mode="overwrite",
            )

        print("Limpieza y escritura en trusted-zone completadas.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
