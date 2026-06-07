"""Warm path: consumer Kafka per cameres de transit (index en memoria)."""
from __future__ import annotations
import json
import os
import re
import threading
import time
import streamlit as st
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
CAMERAS_TOPIC = "traffic_cameras"

_cameras_lock = threading.Lock()
_cameras_state: dict[str, list[dict]] = {}


def parse_s3_url(url: str) -> tuple[str, str]:
    """s3://bucket/key -> (bucket, key)"""
    s = url.strip()
    if not s.startswith("s3://"):
        raise ValueError(f"Not an S3 URL: {url!r}")
    rest = s[5:]
    if "/" not in rest:
        return rest, ""
    bucket, key = rest.split("/", 1)
    return bucket, key


def site_key_from_url(url: str) -> str:
    """Deriva site_key del filename: .../b-20_080_0.gif -> b-20_080"""
    filename = url.rstrip("/").split("/")[-1]
    stem, _ = os.path.splitext(filename)
    m = re.match(r"^(.*)_(\d+)$", stem)
    return m.group(1) if m else stem


def _consume_cameras_loop():
    while True:
        try:
            consumer = KafkaConsumer(
                CAMERAS_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset="latest",
                enable_auto_commit=False,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            )
            for msg in consumer:
                event = msg.value
                if not isinstance(event, dict) or "url" not in event:
                    continue
                site = site_key_from_url(event["url"])
                entry = {
                    "url": event["url"],
                    "timestamp": event.get("timestamp", ""),
                    "filename": event["url"].rstrip("/").split("/")[-1],
                    "carretera": event.get("carretera", ""),
                    "municipi": event.get("municipi", ""),
                }
                with _cameras_lock:
                    _cameras_state.setdefault(site, []).append(entry)
        except Exception as exc:
            print(f"[warm_path/cameras] consumer error: {exc!r}; reconnecting in 10s")
            time.sleep(10)


@st.cache_resource(show_spinner=False)
def start_camera_consumer() -> threading.Thread:
    thread = threading.Thread(target=_consume_cameras_loop, name="cameras-warm-path", daemon=True)
    thread.start()
    return thread


def get_cameras_snapshot() -> dict[str, list[dict]]:
    with _cameras_lock:
        return {k: list(v) for k, v in _cameras_state.items()}
