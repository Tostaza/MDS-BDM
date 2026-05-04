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

## Zona trusted (Spark): de `persistent-landing-zone` a `trusted-zone`

Este paso ejecuta `trusted-zone/trusted_zone.py`: lee Parquet bajo el bucket de **persistent-landing**, aplica el *cleaner* y escribe en **trusted-zone** (misma jerarquía de carpetas). Requiere **PySpark**, acceso **S3A** a SeaweedFS y el paquete **`spark_client/`** en la **raíz del repositorio** (única fuente; se importa con `from spark_client import SparkClient`).

### Prerrequisitos

- Contenedor con Spark (en los ejemplos se usa el servicio `jupyter`).
- El código del repo montado en el contenedor (rutas típicas bajo `/opt/spark/work-dir/`).
- Variables de entorno alineadas con `trusted_zone.py` (`S3A_ENDPOINT`, `S3A_ACCESS_KEY`, `S3A_SECRET_KEY`, `SPARK_MASTER`, buckets, etc.), según tu `.env` o `docker compose`.

### Opción recomendada: script Bash (dentro del contenedor)

Los scripts `trusted-zone/spark-submit-trusted.sh` y `exploitation-zone/spark-submit-exploitation.sh` definen `PYTHONPATH` (raíz del repo + `dags/`) y llaman a `spark-submit`. Deben tener **fines de línea LF** (en Windows, Git respeta `*.sh text eol=lf` del `.gitattributes`).

Desde **PowerShell en Windows** (se ejecuta Bash *dentro* del contenedor):

```powershell
docker exec -it jupyter bash /opt/spark/work-dir/trusted-zone/spark-submit-trusted.sh
```

Si tu repo **no** está en `/opt/spark/work-dir`, indica la raíz con `BDM_REPO_ROOT` (directorio que contiene `spark_client/`, `trusted-zone/` y `dags/`):

```powershell
docker exec -it jupyter env BDM_REPO_ROOT=/ruta/en/contenedor/al/repo bash /opt/spark/work-dir/trusted-zone/spark-submit-trusted.sh
```

Otro master de Spark (ejemplo `local[*]`):

```powershell
docker exec -it jupyter env SPARK_MASTER="local[*]" bash /opt/spark/work-dir/trusted-zone/spark-submit-trusted.sh
```

### Opción alternativa: una sola línea con `spark-submit`

Equivalente manual (ajusta rutas si tu montaje es distinto):

```powershell
docker exec -it jupyter bash -lc "export PYTHONPATH=/opt/spark/work-dir:/opt/spark/work-dir/dags && /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/trusted-zone/trusted_zone.py"
```

- **`PYTHONPATH`**: la **raíz del repositorio** (carpeta padre de `spark_client/`) y opcionalmente **`.../dags`** para `Clients.*`.
- `trusted_zone.py` añade la raíz del repo al `sys.path` para poder hacer `from spark_client import SparkClient` sin duplicar código.

### Trusted → Exploitation (mismo criterio)

```powershell
docker exec -it jupyter bash /opt/spark/work-dir/exploitation-zone/spark-submit-exploitation.sh
```

### Ejecución local en Windows (sin Docker)

Solo si tienes PySpark y red hasta SeaweedFS configurada como en el script:

```powershell
cd C:\ruta\al\repo\BDM
$env:PYTHONPATH = "$PWD;$PWD\dags"
python trusted-zone\trusted_zone.py
```

En PowerShell el separador de `PYTHONPATH` es **`;`**. Dentro de contenedores Linux sigue siendo **`:`**.

### Fallos frecuentes

| Problema | Causa habitual |
| :--- | :--- |
| `ModuleNotFoundError: spark_client` | Falta la carpeta `spark_client/` en la raíz del repo montada, o `PYTHONPATH` no incluye esa raíz. |
| `set: pipefail: invalid option` en `.sh` | Fichero `.sh` guardado con **CRLF**; conviértelo a LF o clona tras aplicar `.gitattributes`. |
| Rutas incorrectas | Sustituye `/opt/spark/work-dir` por la ruta real donde ves `trusted-zone/` y `dags/` en el contenedor (`docker exec -it jupyter ls /opt/spark/work-dir`). |