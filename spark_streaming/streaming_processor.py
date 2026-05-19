
from __future__ import annotations

import os
import sys
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

# Allow ``python streaming_processor.py`` from the repo root as well as
# ``spark-submit /opt/spark/work-dir/spark_streaming/streaming_processor.py``
# inside the spark-worker container where ``spark_streaming`` is bind-mounted
# under ``/opt/spark/work-dir/``.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PARENT_DIR = os.path.dirname(_THIS_DIR)
for _candidate in (_THIS_DIR, _PARENT_DIR):
    if _candidate not in sys.path:
        sys.path.insert(0, _candidate)

try:
    from spark_streaming.schemas import TOPIC_SCHEMAS  # type: ignore
except ImportError:
    from schemas import TOPIC_SCHEMAS  # type: ignore  # noqa: F401


# ---------------------------------------------------------------------------
# Configuration via environment variables (with sensible defaults for the
# docker-compose stack).
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
LANDING_BUCKET = os.getenv("LANDING_BUCKET", "landing-zone")
STREAMING_PREFIX = os.getenv("STREAMING_PREFIX", "streaming")
CHECKPOINT_PREFIX = os.getenv("STREAMING_CHECKPOINT_PREFIX", "_streaming_checkpoints")
TRIGGER_INTERVAL = os.getenv("STREAMING_TRIGGER_INTERVAL", "30 seconds")
STARTING_OFFSETS = os.getenv("STREAMING_STARTING_OFFSETS", "earliest")

# S3A / SeaweedFS — same as the batch SparkClient.
S3A_ENDPOINT = os.getenv("S3A_ENDPOINT", "http://seaweedfs:8333")
S3A_ACCESS_KEY = os.getenv("S3A_ACCESS_KEY", "admin")
S3A_SECRET_KEY = os.getenv("S3A_SECRET_KEY", "admin")


def build_spark_session() -> SparkSession:
    """Create a SparkSession wired up for SeaweedFS + Structured Streaming.

    The Kafka source jar is expected to be supplied at spark-submit time via
    ``--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1`` (see
    README). It is NOT requested here because Ivy resolution from inside the
    SparkSession builder is brittle in cluster mode.
    """
    return (
        SparkSession.builder.appName("bdm-streaming-processor")
        .config("spark.hadoop.fs.s3a.endpoint", S3A_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3A_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3A_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Avoid the FileOutputCommitter v1 rename-storm on object stores.
        .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        # Keep streaming output small and snappy-compressed.
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def _read_topic(spark: SparkSession, topic: str) -> DataFrame:
    """Open a Kafka source DataFrame for a single topic."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", STARTING_OFFSETS)
        # Hard cap per micro-batch to avoid blowing the executor heap on
        # cold start when ``earliest`` replays days of events.
        .option("maxOffsetsPerTrigger", 50000)
        .option("failOnDataLoss", "false")
        .load()
    )


def _shape_dataframe(raw: DataFrame, topic: str, schema: Optional[StructType]) -> DataFrame:
    """
    Convert a Kafka source DataFrame into the shape we want on disk.

    Always-present columns
    ----------------------
    - ``kafka_topic``, ``kafka_partition``, ``kafka_offset``, ``kafka_timestamp``,
      ``kafka_key`` -- raw Kafka metadata, useful for replay / debugging.
    - ``ingestion_ts`` -- the Kafka record timestamp expressed as a Spark
      timestamp. Used for the date-hour partitioning.
    - ``year``, ``month``, ``day``, ``hour`` -- partition columns derived from
      ``ingestion_ts``.

    Topic-specific columns
    ----------------------
    - If a typed schema is registered, the JSON value is parsed and its
      fields are flattened next to the metadata.
    - Otherwise the raw value is stored as ``value_raw`` (string) and the
      silver layer parses it on read.
    """
    base = raw.select(
        F.lit(topic).alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("value_str"),
    )

    if schema is not None:
        parsed = base.withColumn("payload", F.from_json(F.col("value_str"), schema))
        shaped = parsed.select(
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "kafka_key",
            F.col("payload.*"),
        )
    else:
        shaped = base.withColumnRenamed("value_str", "value_raw")

    shaped = shaped.withColumn("ingestion_ts", F.col("kafka_timestamp"))
    shaped = (
        shaped.withColumn("year", F.year("ingestion_ts"))
        .withColumn("month", F.month("ingestion_ts"))
        .withColumn("day", F.dayofmonth("ingestion_ts"))
        .withColumn("hour", F.hour("ingestion_ts"))
    )
    return shaped


def _start_topic_query(spark: SparkSession, topic: str, schema: Optional[StructType]):
    """Start a streaming query for one topic. Returns the StreamingQuery."""
    raw = _read_topic(spark, topic)
    shaped = _shape_dataframe(raw, topic, schema)

    output_path = f"s3a://{LANDING_BUCKET}/{STREAMING_PREFIX}/{topic}/"
    checkpoint_path = f"s3a://{LANDING_BUCKET}/{CHECKPOINT_PREFIX}/{topic}/"

    print(f"[streaming] starting query topic={topic} output={output_path}")

    return (
        shaped.writeStream.format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("year", "month", "day", "hour")
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName(f"bdm-streaming-{topic}")
        .start()
    )


def main() -> int:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    queries = []
    for topic, schema in TOPIC_SCHEMAS.items():
        try:
            queries.append(_start_topic_query(spark, topic, schema))
        except Exception as exc:  # pragma: no cover - logged at runtime
            # One bad topic must not take down the whole job. The others
            # keep running; the broken one will be re-attempted on the next
            # job restart.
            print(f"[streaming] FAILED to start query for topic={topic}: {exc!r}")

    if not queries:
        print("[streaming] no queries started, aborting")
        return 1

    print(f"[streaming] {len(queries)} queries running, awaiting termination")
    # Block on any one query terminating. ``awaitAnyTermination`` returns when
    # the first query stops (normally or with an error). The wrapping
    # ``while True`` reaps the failed query and keeps the surviving ones
    # alive — useful in cluster mode where we want partial-progress.
    while True:
        spark.streams.awaitAnyTermination()
        alive = [q for q in spark.streams.active]
        if not alive:
            print("[streaming] all queries terminated, exiting")
            return 0
        print(f"[streaming] one query terminated; {len(alive)} still active")


if __name__ == "__main__":
    raise SystemExit(main())
