"""
Warm path — live Kafka consumers for the dashboard.

Two independent background threads:
  1. trams_congestion  → latest congestion level per tram (get_trams_snapshot)
  2. Sensors-IoT       → latest reading per IoT sensor   (get_iot_snapshot)

Both follow the same pattern as hot_path.py: a daemon thread started once via
@st.cache_resource, writing into a module-level dict protected by a Lock.
The dashboard reads the dicts every 2 minutes via st.fragment(run_every="2m").
"""

from __future__ import annotations

import json
import os
import threading
import time

import pandas as pd
import streamlit as st
from kafka import KafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TRAMS_TOPIC = os.environ.get("TRAMS_TOPIC", "trams_congestion")
IOT_TOPIC = os.environ.get("IOT_TOPIC", "Sensors-IoT")

# ── Trams state ───────────────────────────────────────────────────────────────

_trams_lock = threading.Lock()
_trams_state: dict[str, dict] = {}  # id_tram (str) → latest record


def _tram_id(record: dict) -> str:
    """Extract id_tram trying multiple column spellings from the Barcelona API."""
    for key in ("id_tram", "ID_TRAM", "idTram", "Id_Tram"):
        v = record.get(key)
        if v is not None:
            return str(v)
    return ""


def _consume_trams_loop() -> None:
    # auto_offset_reset="earliest": replay the full topic on first start so the
    # widget has a complete snapshot immediately, without waiting for the next
    # 5-minute DAG run to produce new messages.
    while True:
        try:
            consumer = KafkaConsumer(
                TRAMS_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            )
            for msg in consumer:
                record = msg.value
                if not isinstance(record, dict):
                    continue
                tram_id = _tram_id(record)
                if not tram_id:
                    continue
                with _trams_lock:
                    _trams_state[tram_id] = record
        except Exception as exc:
            print(f"[warm_path/trams] consumer error: {exc!r}; reconnecting in 10 s")
            time.sleep(10)


@st.cache_resource(show_spinner=False)
def start_trams_consumer() -> threading.Thread:
    """Start the trams Kafka consumer daemon thread exactly once per Streamlit process."""
    thread = threading.Thread(
        target=_consume_trams_loop,
        name="trams-warm-path",
        daemon=True,
    )
    thread.start()
    return thread


def get_trams_snapshot() -> pd.DataFrame:
    """Return a DataFrame with the latest Kafka record per tram."""
    with _trams_lock:
        if not _trams_state:
            return pd.DataFrame()
        rows = list(_trams_state.values())
    df = pd.DataFrame(rows)
    # Normalise estatActual regardless of column casing in the API response.
    for col in ("estatActual", "estat_actual", "estatactual"):
        if col in df.columns:
            df["estatActual"] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
            break
    else:
        df["estatActual"] = 0
    return df


# ── Sensors-IoT state ─────────────────────────────────────────────────────────

_iot_lock = threading.Lock()
_iot_state: dict[str, dict] = {}  # sensor_id (str) → latest record


def _consume_iot_loop() -> None:
    # auto_offset_reset="latest": the IoT generator is continuous; we only care
    # about the current live readings, not replaying historical sensor events.
    while True:
        try:
            consumer = KafkaConsumer(
                IOT_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset="latest",
                enable_auto_commit=False,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            )
            for msg in consumer:
                record = msg.value
                if not isinstance(record, dict):
                    continue
                sid = str(record.get("sensor_id") or "")
                if sid:
                    with _iot_lock:
                        _iot_state[sid] = record
        except Exception as exc:
            print(f"[warm_path/iot] consumer error: {exc!r}; reconnecting in 10 s")
            time.sleep(10)


@st.cache_resource(show_spinner=False)
def start_iot_consumer() -> threading.Thread:
    """Start the Sensors-IoT Kafka consumer daemon thread exactly once per Streamlit process."""
    thread = threading.Thread(
        target=_consume_iot_loop,
        name="iot-warm-path",
        daemon=True,
    )
    thread.start()
    return thread


def get_iot_snapshot() -> pd.DataFrame:
    """Return a DataFrame with the latest Kafka record per IoT sensor."""
    with _iot_lock:
        if not _iot_state:
            return pd.DataFrame()
        return pd.DataFrame(list(_iot_state.values()))
