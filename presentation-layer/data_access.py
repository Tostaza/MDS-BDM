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


def traffic_cameras_gallery_prefix(data_prefix: str) -> str:
    """Prefix S3 sota exploitation on es grava la galeria (``traffic_cameras_gallery``)."""
    p = (data_prefix or "").strip("/")
    return f"{p}/traffic_cameras/gallery/" if p else "traffic_cameras/gallery/"


def _s3_client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=_env("S3_ENDPOINT", "http://seaweedfs:8333"),
        aws_access_key_id=_env("AWS_ACCESS_KEY_ID", "admin"),
        aws_secret_access_key=_env("AWS_SECRET_ACCESS_KEY", "admin"),
    )


def list_traffic_camera_gallery_sites(bucket: str, data_prefix: str = "") -> list[str]:
    """Retorna els ``site_key`` (carpetes) dins ``traffic_cameras/gallery/``."""
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
    Objectes d'una carpeta de galeria.

    Retorna llista de ``(key_s3, nom_mostrat)`` ordenada per nom de fitxer.
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
