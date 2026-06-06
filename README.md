# BDM — Barcelona Urban Mobility Data Platform

## 📌 Overview

This project is developed by Carles and Joel.
It focuses on **urban mobility in Barcelona** — traffic congestion, weather, accidents, parking and traffic-camera data — and integrates these sources into a single analytics platform.

The platform follows a **medallion architecture** (landing → trusted → exploitation) and powers a real-time dashboard for monitoring the city's mobility.

---

## 🎯 Objective

The main objective of this project is to **build an end-to-end big data pipeline that ingests, cleans and combines heterogeneous Barcelona mobility datasources, and exposes them through a live dashboard and predictive models**.

We aim to:

- Develop a clear and efficient solution
- Apply best practices and modern technologies
- Deliver measurable and meaningful results

---
## Datasources
This project prioritizes the use of open data platforms and public APIs. Selected datasources are the following:

**Real Datasources**
- **AEMET (*Agencia Estatal de Meteorología*, Open Data)**: Hourly and daily weather predictions data for Barcelona and its surroundings.
- **Nitter (Twitter/X)**: Social media data for Barcelona and alerts of incidents posted by @transit account.
- **TomTom**: Real-time flow speed, distance and other traffic data for Barcelona.
- **Google Maps**: Real-time flow speed, distance and other traffic data for Barcelona.
- **Ajuntament de Barcelona (Open Data)**: Historical and current state of traffic in roads of Barcelona, their prediction and their coordinates. 
- **Gencat (Open Data)**:  Real time images of traffic cameras in Catalonia.

**Synthethic Datasources**
- **parking-iot-sensor**: Script that simulates multiple parking sensors distributed across Barcelona that generate real-time data about parking availability.
- **KaggleHub**: Datasets for car accidents produced in Barcelona and its surroundings.


| Datasource | Streaming | Semi Streaming | Batches |
| :--- | :---: | :---: | :---: |
| **AEMET** | | | ✅ |
| **Nitter (Twitter/X)** | | ✅ | |
| **TomTom** | |✅ |✅ |
| **Google Maps** | |✅ | ✅|
| **Ajuntament de Barcelona** | | ✅ | ✅ |
| **Gencat (Traffic Cameras)** | | ✅ | |
| **parking-iot-sensor** | ✅ | | |
| **KaggleHub** | | | ✅ |

The above table classifies the datasources according to the type of data flow:
- **Streaming**: Data is generated continuously and in real-time. Data is transmitted via persistent connections such as websockets or actively pushed by the source.
- **Semi Streaming**: Data is generated in real-time but in micro-batches. In this case, data is polled from the source at very short intervals (~5min), behaving as a streaming source.
- **Batches**: Data is generated in batches that are obtained in longer intervals (hours, days, etc).


## 🛠️ Technologies Used

- Airflow: Workflow orchestrator
- SeaweedFS: S3-compatible object storage
- Kafka: Message broker
- Spark: Distributed data processing
- Delta Lake: Open Table Format

---

## 🚀 Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/carles-aguilera-pilo/BDM.git
   ```

2. Build airflow image:
   ```bash
   docker build . -f docker/airflow/Dockerfile -t bdm-airflow:latest
   ```

3. Setup env variables:
   ```bash
   cp env.example .env
   ```

4. Run stack:
   ```bash
   docker compose up -d
   ```

---

## Spark in Docker: trusted-zone and exploitation-zone

Both jobs run with **`spark-submit`** inside the **`jupyter`** container, using the cluster master (`spark://spark-master:7077`). The code imports `SparkClient` from the **`spark_client/`** package (`connect()` to open a session, `close()` or `stop()` to close).

### Common Prerequisites

1. **Stack running**: `docker compose up -d` (at least `jupyter`, `spark-master`, `spark-worker`, `seaweedfs`).
2. **Volumes in `jupyter` and `spark-worker`**: in `docker-compose.yaml` must be mounted under `/opt/spark/work-dir/` at least `trusted-zone/`, `exploitation-zone/` and **`spark_client/`** (otherwise you will see `ModuleNotFoundError: No module named 'spark_client'`). After changing volumes: `docker compose up -d --force-recreate jupyter`.
3. **`PYTHONPATH`**: include the effective root of the repo in the container (`/opt/spark/work-dir` in the default layout) and, if you need imports from Airflow, `/opt/spark/work-dir/dags` (only if you mount `dags/` in that path).
4. **Variables** (optional; have defaults in the scripts): `S3A_ENDPOINT`, `S3A_ACCESS_KEY`, `S3A_SECRET_KEY`, `SPARK_MASTER`, `PERSISTENT_BUCKET`, `TRUSTED_BUCKET`, `EXPLOITATION_BUCKET`, `DATA_PREFIX`, etc. You can prefix them in the same `bash -lc "..."` with `export VAR=value && ...`.

Quick path verification in the container:

```powershell
docker exec -it jupyter ls /opt/spark/work-dir
docker exec -it jupyter ls /opt/spark/work-dir/spark_client
```

---

### Trusted-zone: `persistent-landing-zone` → `trusted-zone`

**What it does:** runs `trusted-zone/trusted_zone.py`: discovers Parquet datasets under the **landing** bucket, applies the *cleaner* and writes to **trusted-zone** maintaining the same folder hierarchy.

**Command (PowerShell on the host):**

```powershell
docker exec -it jupyter bash -lc "export PYTHONPATH=/opt/spark/work-dir:/opt/spark/work-dir/dags && /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/trusted-zone/trusted_zone.py"
```

- **`PYTHONPATH`**: first entry = root where the `spark_client` package is (next to `trusted-zone/`). The second is optional if you need code under `dags/`.
- If you use another master (e.g. local): change `--master spark://spark-master:7077` to `--master 'local[*]'` and adjust `SPARK_MASTER` if your `SparkClient` reads it from the environment.

---

### Exploitation-zone: `trusted-zone` → `exploitation-zone`

**What it does:** runs `exploitation-zone/exploitation_zone.py`: copies the Parquet detected in **trusted** to **exploitation** with the same structure; can apply transformations defined in the script's catalog. Depends on data already existing in trusted (usually after running the **trusted-zone** step).

**Command (PowerShell on the host):**

```powershell
docker exec -it jupyter bash -lc "export PYTHONPATH=/opt/spark/work-dir:/opt/spark/work-dir/dags && /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/exploitation-zone/exploitation_zone.py"
```

- Same `PYTHONPATH` criteria as above. The script already adds `trusted-zone/` and the exploitation folder to `sys.path` for local imports (`trusted_zone`, `dataset_operations`, etc.).

**Typical pipeline order:** first **trusted-zone**, then **exploitation-zone**.

### Local Windows Execution (without Docker)

Only if you have PySpark and network connectivity to SeaweedFS configured as in the script:

```powershell
cd C:\path\to\repo\BDM
$env:PYTHONPATH = "$PWD;$PWD\dags"
# Trusted (landing → trusted):
python trusted-zone\trusted_zone.py
# Exploitation (trusted → exploitation), when data already exists in trusted:
python exploitation-zone\exploitation_zone.py
```

In PowerShell the `PYTHONPATH` separator is **`;`**. Inside Linux containers it remains **`:`**.

### Common Issues

| Issue | Typical Cause |
| :--- | :--- |
| `ModuleNotFoundError: spark_client` | In Docker, `./spark_client` is not mounted at `/opt/spark/work-dir/spark_client` (see `docker-compose`) or you did not recreate `jupyter` after the change. Could also be missing the repo root in `PYTHONPATH`. |
| Exploitation with no data / empty paths | Trusted-zone has not yet written to the expected bucket/prefix, or bucket/prefix variables do not match what is in SeaweedFS. |
| Incorrect paths | Replace `/opt/spark/work-dir` with the real path where you see `trusted-zone/`, `exploitation-zone/` and `spark_client/` (`docker exec -it jupyter ls /opt/spark/work-dir`). |
| Imports from `dags/` | If you remove `.../dags` from `PYTHONPATH`, you will not be able to import modules only present there; mount `./dags` in the container if you need them. |