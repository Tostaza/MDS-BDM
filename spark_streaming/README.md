# BDM Streaming Processor

Spark Structured Streaming job that consumes every Kafka topic produced by
the BDM ingestion DAGs and IoT simulators, and writes append-mode Parquet to
SeaweedFS under `s3a://landing-zone/streaming/{topic}/`.

## What it consumes

| Kafka topic | Producer | Schema parsed? |
|---|---|---|
| `Sensors-IoT` | `src/synthetic_data/sensor_bcn.py` | Yes (`schemas.SENSORS_IOT_SCHEMA`) |
| `parking-lot-events` | `src/synthetic_data/parking_iot_sensor.py` | Yes (`schemas.PARKING_LOT_EVENTS_SCHEMA`) |
| `trams_congestion` | `dags/ingestion/bcn_rt_trams_congestion_dag.py` | No (raw `value_raw` string) |
| `traffic_incidents` | `dags/ingestion/sct_traffic_incidents_dag.py` | No |
| `traffic_cameras` | `dags/ingestion/sct_cameras_dag.py` | No |
| `transit_tweets_topic` | `dags/ingestion/twitter_account_posts_dag.py` | No |

For topics without a typed schema, the Kafka value is stored as a `value_raw`
string column so the silver layer can parse it later. This makes the
streaming job resilient to producer-side schema drift.

## Output layout

```
s3a://landing-zone/streaming/{topic}/year=YYYY/month=MM/day=DD/hour=HH/part-*.snappy.parquet
s3a://landing-zone/_streaming_checkpoints/{topic}/
```

Common columns (every row):

- `kafka_topic`, `kafka_partition`, `kafka_offset`
- `kafka_timestamp` (Kafka record timestamp)
- `kafka_key` (cast to string)
- `ingestion_ts` (alias of `kafka_timestamp`, used for partitioning)
- `year`, `month`, `day`, `hour` (partition columns derived from `ingestion_ts`)

Plus either:
- The parsed JSON fields (for topics with a known schema), or
- `value_raw` (string) for the rest.

## How to run

### Option A — compose service (recommended)

The `spark-streaming` service is opt-in behind a profile so it does NOT start
with a plain `docker compose up`:

```bash
docker compose --profile streaming up -d spark-streaming
docker compose logs -f spark-streaming
```

To stop only the streaming job:

```bash
docker compose --profile streaming stop spark-streaming
docker compose --profile streaming rm -f spark-streaming
```

### Option B — manual `spark-submit` from inside spark-master

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/spark/work-dir/spark_streaming/streaming_processor.py
```

(`spark_streaming/` is bind-mounted into `spark-master` at
`/opt/spark/work-dir/spark_streaming` — see `docker-compose.yaml`.)

The `--packages` flag tells Spark to fetch the Kafka source jar from Maven
Central at submit time. First submission downloads ~10 MB; subsequent
submissions reuse the Ivy cache under `/tmp/.ivy2`.

## Configuration

All overridable via environment variables (defaults in parentheses match the
docker-compose stack):

| Env var | Default | Notes |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Comma-separated broker list |
| `LANDING_BUCKET` | `landing-zone` | S3 bucket for output |
| `STREAMING_PREFIX` | `streaming` | Prefix under the bucket |
| `STREAMING_CHECKPOINT_PREFIX` | `_streaming_checkpoints` | Where Spark stores commit logs |
| `STREAMING_TRIGGER_INTERVAL` | `30 seconds` | Micro-batch interval |
| `STREAMING_STARTING_OFFSETS` | `earliest` | Only used on first run for a checkpoint |
| `S3A_ENDPOINT` | `http://seaweedfs:8333` | SeaweedFS S3 endpoint |
| `S3A_ACCESS_KEY` | `admin` | |
| `S3A_SECRET_KEY` | `admin` | |

## How to verify

After starting the service and waiting ~60 seconds:

1. **Open the Spark UI** at <http://localhost:8082> — should show
   `bdm-streaming-processor` as a running application with N active
   StreamingQuery jobs (one per topic).

2. **Inspect SeaweedFS** at <http://localhost:8888> — under
   `landing-zone/streaming/` you should see one prefix per topic, with
   `year=YYYY/month=MM/day=DD/hour=HH/` partitions appearing as records
   accumulate.

3. **Read a sample with Jupyter or Spark** (from the spark-master container):

   ```python
   from pyspark.sql import SparkSession
   spark = SparkSession.builder.appName("verify").getOrCreate()
   df = spark.read.parquet("s3a://landing-zone/streaming/Sensors-IoT/")
   df.show(5, truncate=False)
   df.printSchema()
   ```

## Design notes

- **One `StreamingQuery` per topic, single SparkSession.** A failure in one
  query (bad schema, broken offset, …) does not prevent the others from
  making progress. Each query owns its own checkpoint location.
- **`processingTime="30 seconds"` trigger.** Tuned for low S3 small-file
  count while keeping end-to-end latency manageable. The job emits a handful
  of Parquet files per topic per minute, which is well within the silver
  layer's downstream tolerance for fan-out.
- **`--packages` over baking the Kafka jar into `bdm-spark:3.3.4`.** Avoids
  a Dockerfile change. If the Maven fetch becomes a problem in offline /
  air-gapped settings, copy `spark-sql-kafka-0-10_2.12-3.4.1.jar` and its
  transitive deps (`kafka-clients`, `commons-pool2`, `spark-token-provider-kafka-0-10`)
  into `jars/` and add `COPY ./jars/...` lines to the root `Dockerfile`.
- **No Delta sink (yet).** The bronze Delta layer in this project uses
  `deltalake` (delta-rs) from Airflow, not Spark — and the spark image does
  not yet bundle delta-core. Writing append-mode Parquet to the same
  `landing-zone` bucket keeps the architecture consistent (raw landing
  zone → batch promotes to persistent landing zone → trusted → exploitation)
  and avoids introducing a second Delta toolchain in Spark.

## What this does NOT do

- It does not promote streaming Parquet into `persistent-landing-zone`
  (Delta). That promotion can be added as a new bronze-style DAG that reads
  the `landing-zone/streaming/{topic}/` partitions on a schedule and merges
  them into Delta tables, mirroring the pattern used by the existing
  ingestion → bronze pairs.
- It does not back-pressure or rate-limit beyond the
  `maxOffsetsPerTrigger=50000` cap. On cold start with a long backlog the
  first micro-batches may take longer than the 30 s trigger; Spark will
  simply queue them and catch up.
- It does not deduplicate. The bronze Delta layer (or the silver Spark job)
  is the right place for that, using the per-topic natural keys.
