#!/usr/bin/env python3
from __future__ import annotations
from traffic_cameras_media import process_traffic_cameras_persistent_to_trusted
import os
import sys
import boto3
from botocore.exceptions import ClientError
from pathlib import Path
_here = Path(__file__).resolve().parent
_root = _here.parent
for _p in (_root, _here):
    _s = str(_p)
    if _s not in sys.path:
        sys.path.insert(0, _s)

def _env(name: str, default: str) -> str:
    v = os.environ.get(name)
    return v if v is not None and v != "" else default

def ensure_bucket(bucket: str) -> None:
    """
    Crea el bucket de destino (trusted) en SeaweedFS si no existe, vía ``head_bucket`` / ``create_bucket``.
    Misma configuración boto que el resto del proyecto (endpoint y claves por entorno).
    """
    endpoint = _env("S3A_ENDPOINT", _env("S3_ENDPOINT", "http://seaweedfs:8333"))
    key = _env("S3A_ACCESS_KEY", _env("AWS_ACCESS_KEY_ID", "admin"))
    secret = _env("S3A_SECRET_KEY", _env("AWS_SECRET_ACCESS_KEY", "admin"))
    s3 = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=key, aws_secret_access_key=secret)
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"Bucket {bucket!r}: existe.")
    except ClientError:
        s3.create_bucket(Bucket=bucket)
        print(f"Bucket {bucket!r}: creat.")

if __name__ == "__main__":
    ensure_bucket(_env("TRUSTED_BUCKET", "trusted-zone"))
    n = process_traffic_cameras_persistent_to_trusted()