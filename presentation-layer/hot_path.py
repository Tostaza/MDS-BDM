from __future__ import annotations
import json
import os
import threading
import time
import redis
import streamlit as st
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
PARKING_TOPIC = os.environ.get("PARKING_TOPIC", "parking-lot-events")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_DB = int(os.environ.get("REDIS_DB", "1"))
KEY_FREE_COUNT = "parking:free_count"
KEY_TOTAL_LOTS = "parking:total_lots"
KEY_LAST_UPDATE = "parking:last_update_ts"

def get_redis_client() -> redis.Redis:
    """Crea i retorna un client Redis apuntant al servidor configurat."""
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True, socket_connect_timeout=2)

def _consume_loop() -> None:
    """Bucle Kafka que llegeix events de parking i actualitza els comptadors a Redis."""
    while True:
        try:
            r = get_redis_client()
            consumer = KafkaConsumer(PARKING_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, auto_offset_reset="latest", enable_auto_commit=False, value_deserializer=lambda b: json.loads(b.decode("utf-8")))

            state: dict[str, bool] = {}
            for msg in consumer:
                event = msg.value
                if not isinstance(event, dict):
                    continue
                device_id = event.get("device_id")
                if not device_id:
                    continue

                state[device_id] = event.get("state") == "empty"
                free = sum(1 for v in state.values() if v)
                total = len(state)

                pipe = r.pipeline()
                pipe.set(KEY_FREE_COUNT, free)
                pipe.set(KEY_TOTAL_LOTS, total)
                pipe.set(KEY_LAST_UPDATE, int(time.time()))
                pipe.execute()
        except Exception as exc:
            print(f"[hot_path] consumer crashed: {exc!r}; reconnecting in 5 s")
            time.sleep(5)


@st.cache_resource(show_spinner=False)
def start_parking_consumer() -> threading.Thread:
    """Arrenca el thread Kafka->Redis una sola vegada per proces Streamlit."""
    thread = threading.Thread(target=_consume_loop, name="parking-hot-path", daemon=True)
    thread.start()
    return thread


def read_parking_counters() -> tuple[int, int, int | None]:
    """Llegeix (places_lliures, total_places, timestamp) des de Redis."""
    try:
        r = get_redis_client()
        free = int(r.get(KEY_FREE_COUNT) or 0)
        total = int(r.get(KEY_TOTAL_LOTS) or 0)
        ts_raw = r.get(KEY_LAST_UPDATE)
        ts = int(ts_raw) if ts_raw else None
        return free, total, ts
    except Exception as exc:
        print(f"[hot_path] redis read error: {exc!r}")
        return 0, 0, None
