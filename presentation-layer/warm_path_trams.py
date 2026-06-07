"""Warm path: consumers Kafka en background per al dashboard (trams i sensors IoT)."""
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
_trams_lock = threading.Lock()
_trams_state: dict[str, dict] = {}
_iot_lock = threading.Lock()
_iot_state: dict[str, dict] = {}

def _tram_id(record: dict) -> str:
    """Extreu id_tram provant diverses grafies de la clau."""
    for key in ("id_tram", "ID_TRAM", "idTram", "Id_Tram"):
        v = record.get(key)
        if v is not None:
            return str(v)
    return ""

def _consume_trams_loop() -> None:
    """Bucle Kafka que llegeix el topic de trams i actualitza l'estat en memoria."""
    while True:
        try:
            consumer = KafkaConsumer(TRAMS_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, auto_offset_reset="earliest", enable_auto_commit=False,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")))
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
    """Arrenca el consumer Kafka de trams una sola vegada per proces Streamlit."""
    thread = threading.Thread(target=_consume_trams_loop, name="trams-warm-path", daemon=True)
    thread.start()
    return thread

def get_trams_snapshot() -> pd.DataFrame:
    """Retorna un DataFrame amb l'ultima lectura Kafka per cada tram."""
    with _trams_lock:
        if not _trams_state:
            return pd.DataFrame()
        rows = list(_trams_state.values())
    df = pd.DataFrame(rows)
    for col in ("estatActual", "estat_actual", "estatactual"):
        if col in df.columns:
            df["estatActual"] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
            break
    else:
        df["estatActual"] = 0
    return df

def _consume_iot_loop() -> None:
    """Bucle Kafka que llegeix el topic IoT i actualitza l'estat en memoria."""
    while True:
        try:
            consumer = KafkaConsumer(IOT_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, auto_offset_reset="latest", enable_auto_commit=False,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")))
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
    """Arrenca el consumer Kafka de sensors IoT una sola vegada per proces Streamlit."""
    thread = threading.Thread(target=_consume_iot_loop, name="iot-warm-path", daemon=True)
    thread.start()
    return thread

def get_iot_snapshot() -> pd.DataFrame:
    """Retorna un DataFrame amb l'ultima lectura Kafka per cada sensor IoT."""
    with _iot_lock:
        if not _iot_state:
            return pd.DataFrame()
        return pd.DataFrame(list(_iot_state.values()))
