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

- Airflow
- SeaweedFS (S3-compatible object storage)
- Kafka

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