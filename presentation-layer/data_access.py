"""I/O del dashboard sobre S3/SeaweedFS: lectura Parquet i imatges de la zona d'explotacio."""
from __future__ import annotations
import os
import pandas as pd
import boto3
import s3fs
import pyarrow.parquet as pq

def _env(name: str, default: str) -> str:
    """Llegeix variable d'entorn; retorna default si es buida o absent."""
    v = os.environ.get(name)
    return v if v is not None and v != "" else default

def s3_storage_options() -> dict:
    """Opcions d'autenticacio i endpoint per a pandas.read_parquet sobre s3://."""
    return {"key": _env("AWS_ACCESS_KEY_ID", "admin"), "secret": _env("AWS_SECRET_ACCESS_KEY", "admin"), "client_kwargs": {"endpoint_url": _env("S3_ENDPOINT", "http://seaweedfs:8333")}}

def traffic_cameras_gallery_prefix(data_prefix: str) -> str:
    """Retorna el prefix S3 de la galeria de cameres dins del bucket d'explotacio."""
    p = (data_prefix or "").strip("/")
    return f"{p}/traffic_cameras/gallery/" if p else "traffic_cameras/gallery/"

def _s3_client():
    """Client boto3 per al dashboard (mateix endpoint que el compose)."""
    return boto3.client("s3", endpoint_url=_env("S3_ENDPOINT", "http://seaweedfs:8333"), aws_access_key_id=_env("AWS_ACCESS_KEY_ID", "admin"), aws_secret_access_key=_env("AWS_SECRET_ACCESS_KEY", "admin"))


def list_traffic_camera_gallery_sites(bucket: str, data_prefix: str = "") -> list[str]:
    """Llista els site_keys (carpetes de primer nivell) de la galeria de cameres."""
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
    """Llista els objectes S3 d'un site_key concret; retorna [(clau_s3, nom_fitxer)]."""
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
    """Descarrega un objecte S3 en memoria per mostrar-lo com a imatge a la UI."""
    s3 = _s3_client()
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


def load_parquet_prefix(bucket: str, prefix: str) -> pd.DataFrame:
    """Llegeix tots els .parquet sota s3://bucket/prefix/, excloent _delta_log/."""
    so = s3_storage_options()
    p = prefix.strip("/")
    fs = s3fs.S3FileSystem(
        key=so["key"],
        secret=so["secret"],
        client_kwargs=so["client_kwargs"],
    )
    root = f"{bucket}/{p}"
    keys = set(fs.glob(f"{root}/*.parquet") + fs.glob(f"{root}/**/*.parquet"))
    parquet_keys = [k for k in keys if "_delta_log" not in k]
    if not parquet_keys:
        return pd.DataFrame()
    table = pq.read_table(parquet_keys, filesystem=fs)
    return table.to_pandas()


def load_trams_map(bucket: str, prefix: str = "ajuntament_bcn/trams_map/") -> pd.DataFrame:
    """Llegeix la taula map-ready de trams (id_tram, path, estatActual...) de exploitation."""
    return load_parquet_prefix(bucket, prefix)
