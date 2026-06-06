"""
Seeder mínimo del mapa de trams — para probar el widget SIN Airflow ni el clúster Spark.

Reconstruye en **pandas** la misma tabla `ajuntament_bcn/trams_map/` que normalmente
materializa el job Spark `exploitation-zone` (`materialize_trams_map`):
una fila por tram con la polilínea (`path`) y un nivel de congestió (`estatActual`).

Diferencias deliberadas con el pipeline real (es solo para test local):
  - Geometría: se descarga el CSV real *format_long* del portal Open Data de Barcelona,
    así el mapa muestra calles reales.
  - Congestió: se **sintetiza** un nivel 1–6 por tram (distribución plausible) en lugar de
    leer el feed en vivo, para que el mapa siempre tenga colores aunque no haya pipeline.
    Pon `CONGESTION_MODE=live` para intentar el snapshot real (con fallback a sintético).

Escribe un único Parquet bajo el prefijo que lee el dashboard
(`data_access.load_trams_map`). Se ejecuta dentro de la imagen `bdm-dashboard`
(ya trae pandas/pyarrow/s3fs/boto3).
"""

from __future__ import annotations

import io
import os
import random
import sys
from datetime import datetime, timezone

import boto3
import pandas as pd

GEOM_URL = os.environ.get(
    "GEOM_CSV_URL",
    "https://opendata-ajuntament.barcelona.cat/data/dataset/1090983a-1c40-4609-8620-14ad49aae3ab/"
    "resource/c97072a3-3619-4547-84dd-f1999d2a3fec/download/transit_relacio_trams_format_long.csv",
)
CONGESTION_URL = os.environ.get(
    "CONGESTION_CSV_URL",
    "https://opendata-ajuntament.barcelona.cat/data/dataset/transit-relacio-trams/resource/"
    "2d456eb5-4ea6-4f68-9794-2f3f1a58a008/download",
)
CONGESTION_MODE = os.environ.get("CONGESTION_MODE", "synthetic").lower()

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://seaweedfs:8333")
AWS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "admin")
BUCKET = os.environ.get("EXPLOITATION_BUCKET", "exploitation-zone")
PREFIX = os.environ.get("TRAMS_MAP_PREFIX", "ajuntament_bcn/trams_map/")


def _col(df: pd.DataFrame, *candidates: str) -> str | None:
    lower = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower:
            return lower[c.lower()]
    return None


def build_paths(df: pd.DataFrame) -> pd.DataFrame:
    """Una polilínea ordenada [lon, lat] por tram a partir del CSV format_long."""
    tram_c = _col(df, "Tram", "idTram", "id_tram")
    lon_c = _col(df, "Longitud", "lon", "lng")
    lat_c = _col(df, "Latitud", "lat")
    ord_c = _col(df, "Tram_Components", "ordre", "order", "seq")
    desc_c = _col(df, "Descripció", "Descripcio", "descripcio", "nom")
    if not (tram_c and lon_c and lat_c):
        raise SystemExit(f"CSV de geometría sin columnas esperadas. Tiene: {list(df.columns)}")

    df = df.copy()
    df[lon_c] = pd.to_numeric(df[lon_c], errors="coerce")
    df[lat_c] = pd.to_numeric(df[lat_c], errors="coerce")
    df = df.dropna(subset=[lon_c, lat_c])
    if ord_c:
        df = df.sort_values([tram_c, ord_c])

    rows = []
    for tram, g in df.groupby(tram_c, sort=False):
        path = [[float(lo), float(la)] for lo, la in zip(g[lon_c], g[lat_c])]
        rows.append(
            {
                "id_tram": str(tram),
                "descripcio": str(g[desc_c].iloc[0]) if desc_c else None,
                "path": path,
                "n_punts": len(path),
            }
        )
    return pd.DataFrame(rows)


def synth_congestion(ids: list[str]) -> pd.DataFrame:
    """Nivel 1–6 por tram, distribución plausible y reproducible (seed fija)."""
    rnd = random.Random(42)
    levels, weights = [1, 2, 3, 4, 5, 6], [30, 28, 20, 12, 7, 3]
    now = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    out = []
    for i in ids:
        actual = rnd.choices(levels, weights=weights, k=1)[0]
        previst = min(6, max(1, actual + rnd.choice([-1, 0, 0, 1])))
        out.append({"id_tram": i, "estatActual": actual, "estatPrevist": previst, "data": now})
    return pd.DataFrame(out)


def live_congestion() -> pd.DataFrame | None:
    """Intenta el snapshot real (`idTram#estatActual#estatPrevist`); None si falla."""
    try:
        raw = pd.read_csv(CONGESTION_URL, sep="#", header=None, dtype=str)
        raw = raw.iloc[:, :3]
        raw.columns = ["id_tram", "estatActual", "estatPrevist"]
        raw["id_tram"] = raw["id_tram"].astype(str)
        raw["estatActual"] = pd.to_numeric(raw["estatActual"], errors="coerce").fillna(0).astype(int)
        raw["estatPrevist"] = pd.to_numeric(raw["estatPrevist"], errors="coerce").fillna(0).astype(int)
        raw["data"] = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        print(f"[seed] congestió en vivo: {len(raw)} trams")
        return raw
    except Exception as exc:  # noqa: BLE001
        print(f"[seed] no se pudo leer congestió en vivo ({exc!r}); usando sintética")
        return None


def main() -> None:
    print(f"[seed] descargando geometría: {GEOM_URL}")
    geom = pd.read_csv(GEOM_URL)
    paths = build_paths(geom)
    print(f"[seed] {len(paths)} trams con geometría")

    cong = live_congestion() if CONGESTION_MODE == "live" else None
    if cong is None:
        cong = synth_congestion(paths["id_tram"].tolist())

    trams_map = paths.merge(cong, on="id_tram", how="left")
    trams_map["estatActual"] = trams_map["estatActual"].fillna(0).astype(int)
    trams_map["estatPrevist"] = trams_map.get("estatPrevist", 0)
    trams_map["estatPrevist"] = trams_map["estatPrevist"].fillna(0).astype(int)
    print(
        f"[seed] tabla final: {len(trams_map)} filas · "
        f"niveles {sorted(trams_map['estatActual'].unique().tolist())}"
    )

    # Bucket (SeaweedFS no lo autocrea al escribir vía s3fs).
    s3 = boto3.client(
        "s3", endpoint_url=S3_ENDPOINT, aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET
    )
    try:
        s3.create_bucket(Bucket=BUCKET)
        print(f"[seed] bucket '{BUCKET}' creado")
    except Exception as exc:  # noqa: BLE001
        print(f"[seed] bucket '{BUCKET}' ya existe o no se pudo crear: {exc!r}")

    # Escribe un único Parquet bajo el prefijo que lee el dashboard.
    buf = io.BytesIO()
    trams_map.to_parquet(buf, index=False)
    key = PREFIX.strip("/") + "/trams_map.parquet"
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
    print(f"[seed] escrito s3://{BUCKET}/{key}  ✓")
    print("[seed] listo. Abre el dashboard y mira la sección «Mapa de congestió de trams».")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        print(f"[seed] ERROR: {exc!r}", file=sys.stderr)
        raise
