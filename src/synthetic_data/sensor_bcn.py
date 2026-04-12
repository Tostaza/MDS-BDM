import time
import random
import json
from datetime import datetime

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic


KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "Sensors-IoT"
INTERVAL = 5  # segundos

random.seed(42)

LOCATIONS = [
    {"sensor_id": "BCN_001", "ubicacion": "Ronda Litoral", "lat": 41.380, "lon": 2.180, "base_flow": 50, "base_speed": 45},
    {"sensor_id": "BCN_002", "ubicacion": "Ronda de Dalt", "lat": 41.441, "lon": 2.134, "base_flow": 48, "base_speed": 50},
    {"sensor_id": "BCN_003", "ubicacion": "Diagonal", "lat": 41.393, "lon": 2.162, "base_flow": 42, "base_speed": 28},
    {"sensor_id": "BCN_004", "ubicacion": "Passeig de Gracia", "lat": 41.392, "lon": 2.165, "base_flow": 30, "base_speed": 22},
]


def hour_multiplier(ts: datetime) -> float:
    hour = ts.hour
    weekday = ts.weekday()

    if weekday < 5:
        if 7 <= hour <= 9:
            return 1.7
        elif 17 <= hour <= 20:
            return 1.8
        elif 0 <= hour <= 5:
            return 0.35
        return 1.0
    else:
        return 0.8


def congestion_label(occupancy_pct: float) -> str:
    if occupancy_pct < 30:
        return "low"
    elif occupancy_pct < 55:
        return "medium"
    elif occupancy_pct < 80:
        return "high"
    return "very_high"


def simulate_sensor(location: dict, ts: datetime) -> dict:
    hm = hour_multiplier(ts)

    vehicles = location["base_flow"] * hm * random.uniform(0.85, 1.15)
    vehicles = round(vehicles)

    speed = location["base_speed"] - max(0, vehicles - location["base_flow"]) * 0.5
    speed = max(5, round(speed, 1))

    occupancy = min(100, max(5, 20 + vehicles * 1.3 - speed * 0.4))

    return {
        "sensor_id": location["sensor_id"],
        "timestamp": ts.isoformat(),
        "ubicacion": location["ubicacion"],
        "lat": location["lat"],
        "lon": location["lon"],
        "vehicles_per_minute": vehicles,
        "avg_speed_kmh": speed,
        "occupancy_pct": round(occupancy, 1),
        "congestion_level": congestion_label(occupancy),
    }


def generate_batch():
    ts = datetime.now()
    return [simulate_sensor(loc, ts) for loc in LOCATIONS]


def create_topic():
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        client_id="admin"
    )

    topic = NewTopic(
        name=TOPIC,
        num_partitions=1,
        replication_factor=1
    )

    try:
        admin.create_topics([topic])
        print(f" Topic '{TOPIC}' creado")
    except Exception:
        print(f" Topic '{TOPIC}' ya existe")


def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )


def run():
    create_topic()  # se intenta crear (no rompe si existe)
    producer = get_producer()
    print(" Iniciando producer...")
    while True:
        batch = generate_batch()
        for event in batch:
            producer.send(TOPIC, value=event)
        producer.flush()
        print(f" Enviados {len(batch)} eventos")
        print("-" * 60)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    run()