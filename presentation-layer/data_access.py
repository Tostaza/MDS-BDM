"""Lectura Parquet desde S3 (exploitation-zone) vía fsspec/pyarrow."""

from __future__ import annotations

import os

import pandas as pd


def _env(name: str, default: str) -> str:
    v = os.environ.get(name)
    return v if v is not None and v != "" else default


def s3_storage_options() -> dict:
    return {
        "key": _env("AWS_ACCESS_KEY_ID", "admin"),
        "secret": _env("AWS_SECRET_ACCESS_KEY", "admin"),
        "client_kwargs": {"endpoint_url": _env("S3_ENDPOINT", "http://seaweedfs:8333")},
    }


def load_parquet_prefix(bucket: str, prefix: str) -> pd.DataFrame:
    """Lee todos los .parquet bajo s3://bucket/prefix/ (carpeta)."""
    so = s3_storage_options()
    p = prefix.strip("/")
    base = f"s3://{bucket}/{p}/" if p else f"s3://{bucket}/"
    try:
        return pd.read_parquet(base, storage_options=so)
    except Exception:
        try:
            return pd.read_parquet(base.rstrip("/"), storage_options=so)
        except Exception:
            return pd.DataFrame()
