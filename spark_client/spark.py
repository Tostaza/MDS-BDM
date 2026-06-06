"""
Cliente Spark reutilizable: sesión con S3A (SeaweedFS) y cierre ordenado.

Usado por ``trusted_zone``, ``exploitation_zone`` y otros jobs que comparten la misma
configuración de endpoint y credenciales vía variables de entorno.
"""
from dataclasses import dataclass, field
import os
from typing import Optional

from pyspark.sql import SparkSession

from .client_motor_processamiento import ClientMotorProcessamiento


@dataclass
class SparkClient(ClientMotorProcessamiento):
    app_name: str = "BDM"
    master: str = os.getenv("SPARK_MASTER", "local[*]")
    endpoint: str = os.getenv("S3A_ENDPOINT", "http://seaweedfs:8333")
    access_key: str = os.getenv("S3A_ACCESS_KEY", "admin")
    secret_key: str = os.getenv("S3A_SECRET_KEY", "admin")
    spark: Optional[SparkSession] = field(default=None)

    def connect(self) -> SparkSession:
        """
        Crea o reutiliza una ``SparkSession`` con S3A (SeaweedFS) y política de fechas **LEGACY**.

        ``LEGACY`` es necesario para que ``to_timestamp`` con patrones tipo RSS
        (``EEE, dd MMM yyyy HH:mm:ss z``) en el pipeline de ``transit_posts`` no falle en Spark 3+.

        También activa Delta Lake (extensión SQL + catálogo) y pide los jars
        ``io.delta:delta-core_2.12:2.4.0`` por Ivy. Sin esto, ``read/write.format("delta")``
        fallaría con ``ClassNotFoundException``. La versión está alineada con Spark 3.4.x
        (Delta 2.4.x es la rama soportada).
        """
        self.spark = (
            SparkSession.builder.appName(self.app_name)
            .master(self.master)
            .config("spark.hadoop.fs.s3a.endpoint", self.endpoint)
            .config("spark.hadoop.fs.s3a.access.key", self.access_key)
            .config("spark.hadoop.fs.s3a.secret.key", self.secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            # RSS/Nitter: to_timestamp("EEE, dd MMM yyyy HH:mm:ss z") requereix parser pre-Spark 3.
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            # Delta Lake — duplicado vs spark-defaults.conf intencionadamente para que
            # SparkSession.builder funcione cuando se llama fuera de spark-submit
            # (jupyter, tests locales con master=local[*]).
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .getOrCreate()
        )
        return self.spark

    def close(self) -> None:
        if self.spark is not None:
            self.spark.stop()
            self.spark = None

    def stop(self) -> None:
        """Detiene la sesión; equivalente a :meth:`close` (como ``SparkSession.stop()``)."""
        self.close()
