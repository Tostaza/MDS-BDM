# Project Name

## 📌 Overview

This project is developed by Carles and Joel.
It focuses on **[Topic X]** and aims to deliver an innovative and practical solution within this field.

The project is designed to be engaging, impactful, and scalable.

---

## 🎯 Objective

The main objective of this project is to **[Objective X prima]**.

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
   git clone https://github.com/your-username/project-name.git
   ```

2. Build airflow image:
   ```bash
   docker build . -f docker/airflow/Dockerfile -t bdm-airflow:latest
   ```

3. Setup env variables:
   ```bash
   cp .env.example .env
   ```

4. Run stack:
   ```bash
   docker compose up -d
   ```

---

## Spark en Docker: trusted-zone y exploitation-zone

Ambos jobs se ejecutan con **`spark-submit`** dentro del contenedor **`jupyter`**, usando el master del cluster (`spark://spark-master:7077`). El código importa `SparkClient` desde el paquete **`spark_client/`** (`connect()` para abrir sesión, `close()` o `stop()` para cerrarla).

### Prerrequisitos comunes

1. **Stack levantado**: `docker compose up -d` (al menos `jupyter`, `spark-master`, `spark-worker`, `seaweedfs`).
2. **Volúmenes en `jupyter` y `spark-worker`**: en `docker-compose.yaml` deben montarse bajo `/opt/spark/work-dir/` al menos `trusted-zone/`, `exploitation-zone/` y **`spark_client/`** (sin esto verás `ModuleNotFoundError: No module named 'spark_client'`). Tras cambiar volúmenes: `docker compose up -d --force-recreate jupyter`.
3. **`PYTHONPATH`**: incluir la raíz efectiva del repo en el contenedor (`/opt/spark/work-dir` en el layout por defecto) y, si hace falta imports desde Airflow, `/opt/spark/work-dir/dags` (solo si montas `dags/` en ese path).
4. **Variables** (opcionales; tienen defaults en los scripts): `S3A_ENDPOINT`, `S3A_ACCESS_KEY`, `S3A_SECRET_KEY`, `SPARK_MASTER`, `PERSISTENT_BUCKET`, `TRUSTED_BUCKET`, `EXPLOITATION_BUCKET`, `DATA_PREFIX`, etc. Puedes prefijarlas en el mismo `bash -lc "..."` con `export VAR=valor && ...`.

Comprobación rápida de rutas en el contenedor:

```powershell
docker exec -it jupyter ls /opt/spark/work-dir
docker exec -it jupyter ls /opt/spark/work-dir/spark_client
```

---

### Trusted-zone: `persistent-landing-zone` → `trusted-zone`

**Qué hace:** ejecuta `trusted-zone/trusted_zone.py`: descubre datasets Parquet bajo el bucket de **landing**, aplica el *cleaner* y escribe en **trusted-zone** manteniendo la misma jerarquía de carpetas.

**Comando (PowerShell en el host):**

```powershell
docker exec -it jupyter bash -lc "export PYTHONPATH=/opt/spark/work-dir:/opt/spark/work-dir/dags && /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/trusted-zone/trusted_zone.py"
```

- **`PYTHONPATH`**: primera entrada = raíz donde está el paquete `spark_client` (junto a `trusted-zone/`). La segunda es opcional si necesitas código bajo `dags/`.
- Si usas otro master (por ejemplo local): cambia `--master spark://spark-master:7077` por `--master 'local[*]'` y ajusta `SPARK_MASTER` si tu `SparkClient` lo lee del entorno.

---

### Exploitation-zone: `trusted-zone` → `exploitation-zone`

**Qué hace:** ejecuta `exploitation-zone/exploitation_zone.py`: copia los Parquet detectados en **trusted** hacia **exploitation** con la misma estructura; puede aplicar transformaciones definidas en el catálogo del script. Depende de que los datos ya existan en trusted (habitualmente después de ejecutar el paso **trusted-zone**).

**Comando (PowerShell en el host):**

```powershell
docker exec -it jupyter bash -lc "export PYTHONPATH=/opt/spark/work-dir:/opt/spark/work-dir/dags && /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/exploitation-zone/exploitation_zone.py"
```

- Mismo criterio de `PYTHONPATH` que arriba. El script ya añade `trusted-zone/` y la carpeta de exploitation al `sys.path` para imports locales (`trusted_zone`, `dataset_operations`, etc.).

**Orden típico en pipeline:** primero **trusted-zone**, luego **exploitation-zone**.

### Ejecución local en Windows (sin Docker)

Solo si tienes PySpark y red hasta SeaweedFS configurada como en el script:

```powershell
cd C:\ruta\al\repo\BDM
$env:PYTHONPATH = "$PWD;$PWD\dags"
# Trusted (landing → trusted):
python trusted-zone\trusted_zone.py
# Exploitation (trusted → exploitation), cuando ya haya datos en trusted:
python exploitation-zone\exploitation_zone.py
```

En PowerShell el separador de `PYTHONPATH` es **`;`**. Dentro de contenedores Linux sigue siendo **`:`**.

### Fallos frecuentes

| Problema | Causa habitual |
| :--- | :--- |
| `ModuleNotFoundError: spark_client` | En Docker falta montar `./spark_client` en `/opt/spark/work-dir/spark_client` (ver `docker-compose`) o no recreaste `jupyter` tras el cambio. También puede faltar incluir la raíz del repo en `PYTHONPATH`. |
| Exploitation sin datos / rutas vacías | Trusted-zone aún no ha escrito en el bucket/prefijo esperado, o variables de bucket/prefijo no coinciden con lo que hay en SeaweedFS. |
| Rutas incorrectas | Sustituye `/opt/spark/work-dir` por la ruta real donde ves `trusted-zone/`, `exploitation-zone/` y `spark_client/` (`docker exec -it jupyter ls /opt/spark/work-dir`). |
| Imports desde `dags/` | Si quitas `.../dags` del `PYTHONPATH`, no podrás importar módulos solo presentes ahí; monta `./dags` en el contenedor si lo necesitas. |