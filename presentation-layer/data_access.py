"""
I/O del dashboard sobre S3/SeaweedFS (bucket de explotación): lectura Parquet e imágenes.

No contiene joins ni transformaciones ETL; esas operaciones viven en ``exploitation-zone/`` (Spark).
"""

from __future__ import annotations

import os

import pandas as pd


def _env(name: str, default: str) -> str:
    """Devuelve ``os.environ[name]`` si está definido y no vacío; si no, ``default``."""
    v = os.environ.get(name)
    return v if v is not None and v != "" else default


def s3_storage_options() -> dict:
    """Opciones de autenticación y endpoint para ``pandas.read_parquet`` sobre ``s3://``."""
    return {
        "key": _env("AWS_ACCESS_KEY_ID", "admin"),
        "secret": _env("AWS_SECRET_ACCESS_KEY", "admin"),
        "client_kwargs": {"endpoint_url": _env("S3_ENDPOINT", "http://seaweedfs:8333")},
    }


def traffic_cameras_gallery_prefix(data_prefix: str) -> str:
    """
    Prefijo S3 dentro del bucket exploitation donde ``exploitation_zone`` escribió la galería.

    Estructura: ``{DATA_PREFIX}/traffic_cameras/gallery/`` (o sin prefijo si ``DATA_PREFIX`` vacío).
    """
    p = (data_prefix or "").strip("/")
    return f"{p}/traffic_cameras/gallery/" if p else "traffic_cameras/gallery/"


def _s3_client():
    """Cliente boto3 S3 para el dashboard (mismas variables ``S3_ENDPOINT`` / ``AWS_*`` que el compose)."""
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=_env("S3_ENDPOINT", "http://seaweedfs:8333"),
        aws_access_key_id=_env("AWS_ACCESS_KEY_ID", "admin"),
        aws_secret_access_key=_env("AWS_SECRET_ACCESS_KEY", "admin"),
    )


def list_traffic_camera_gallery_sites(bucket: str, data_prefix: str = "") -> list[str]:
    """
    Lista «carpetas» de primer nivel bajo la galería usando ``Delimiter='/'`` en ListObjectsV2.

    Cada nombre devuelto es un ``site_key`` (ubicación lógica) para el selectbox de Streamlit.
    """
    s3 = _s3_client()
    prefix = traffic_cameras_gallery_prefix(data_prefix)
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    sites: list[str] = []
    for cp in resp.get("CommonPrefixes", []) or []:
        p = (cp.get("Prefix") or "").rstrip("/")
        if not p:
            continue
        name = p.split("/")[-1]
        if name:
            sites.append(name)
    return sorted(set(sites))


def list_traffic_camera_gallery_objects(bucket: str, data_prefix: str, site_key: str) -> list[tuple[str, str]]:
    """
    Lista objetos S3 de una carpeta de galería para un ``site_key`` concreto.

    Recorre ``ListObjectsV2`` con paginación y devuelve ``(key_s3, nombre_mostrar)``
    ordenado por nombre de fichero (para columnas de miniaturas en Streamlit).
    """
    s3 = _s3_client()
    base = traffic_cameras_gallery_prefix(data_prefix) + site_key.strip("/") + "/"
    keys: list[tuple[str, str]] = []
    token = None
    while True:
        kw: dict = {"Bucket": bucket, "Prefix": base}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for obj in resp.get("Contents", []) or []:
            k = obj.get("Key") or ""
            if k.endswith("/"):
                continue
            name = k.split("/")[-1]
            keys.append((k, name))
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    keys.sort(key=lambda x: x[1])
    return keys


def get_s3_object_bytes(bucket: str, key: str) -> bytes:
    """Descarga el objeto S3 completo en memoria (bytes) para mostrar imagen/GIF en la UI."""
    s3 = _s3_client()
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


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
