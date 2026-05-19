
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# Produced by src/synthetic_data/sensor_bcn.py (one event per sensor per tick).
SENSORS_IOT_SCHEMA = StructType(
    [
        StructField("sensor_id", StringType(), nullable=True),
        StructField("timestamp", StringType(), nullable=True),
        StructField("ubicacion", StringType(), nullable=True),
        StructField("lat", DoubleType(), nullable=True),
        StructField("lon", DoubleType(), nullable=True),
        StructField("vehicles_per_minute", LongType(), nullable=True),
        StructField("avg_speed_kmh", DoubleType(), nullable=True),
        StructField("occupancy_pct", DoubleType(), nullable=True),
        StructField("congestion_level", StringType(), nullable=True),
    ]
)


# Produced by src/synthetic_data/parking_iot_sensor.py (state toggle events).
PARKING_LOT_EVENTS_SCHEMA = StructType(
    [
        StructField("device_id", StringType(), nullable=True),
        StructField("timestamp", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),  # "empty" | "full"
    ]
)


# Map: kafka_topic_name -> spark_schema (or None for schema-on-read fallback).
#
# Topics not listed here get the fallback handler: the streaming job keeps the
# raw value as a string column so the silver layer can parse it later. Adding
# a topic to this dict at any time will start producing typed Parquet for it
# from that point onwards (older partitions stay as raw-value Parquet).
TOPIC_SCHEMAS = {
    "Sensors-IoT": SENSORS_IOT_SCHEMA,
    "parking-lot-events": PARKING_LOT_EVENTS_SCHEMA,
    # The producers below send records whose JSON shape varies across
    # ingestion runs (XML-converted incidents, RSS-converted tweets, …).
    # Keeping them as raw values avoids brittle schema bindings.
    "trams_congestion": None,
    "traffic_incidents": None,
    "traffic_cameras": None,
    "transit_tweets_topic": None,
}
