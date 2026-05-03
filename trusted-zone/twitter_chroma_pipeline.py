"""
Embeddings de mensajes Twitter (Parquet en trusted-zone) → ChromaDB.

Requisitos: contenedor `dashboard` (imagen bdm-dashboard) o entorno con
chromadb, sentence-transformers, pandas, pyarrow.

Ejecución recomendada (misma red Docker que Chroma y SeaweedFS):

    docker compose build dashboard
    docker compose up -d chromadb seaweedfs
    docker compose run --rm --entrypoint "" dashboard \\
        python /trusted-zone/twitter_chroma_pipeline.py

Variables de entorno:
    S3_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
    TRUSTED_BUCKET          (default trusted-zone)
    DATA_PREFIX              prefijo opcional dentro del bucket
    TWITTER_PARQUET_PREFIX   carpeta bajo trusted con Parquet (default transit_posts)
    CHROMA_HOST, CHROMA_PORT
    CHROMA_COLLECTION_TWITTER  nombre colección (default twitter_transit_posts)
    EMBEDDING_BATCH_SIZE       (default 64)
"""

from __future__ import annotations

import hashlib
import os
import sys
from typing import Any

import chromadb
import pandas as pd
from sentence_transformers import SentenceTransformer


def _env(name: str, default: str) -> str:
    v = os.environ.get(name)
    return v if v is not None and v != "" else default


def _s3_dir_uri(bucket: str, *parts: str) -> str:
    p = "/".join(x.strip("/") for x in parts if x.strip("/"))
    if p:
        return f"s3://{bucket.strip('/')}/{p}"
    return f"s3://{bucket.strip('/')}/"


def _storage_options() -> dict[str, Any]:
    return {
        "key": _env("AWS_ACCESS_KEY_ID", "admin"),
        "secret": _env("AWS_SECRET_ACCESS_KEY", "admin"),
        "client_kwargs": {"endpoint_url": _env("S3_ENDPOINT", "http://seaweedfs:8333")},
    }


def _trusted_twitter_path() -> str:
    bucket = _env("TRUSTED_BUCKET", "trusted-zone")
    data_prefix = _env("DATA_PREFIX", "").strip("/")
    tw = _env("TWITTER_PARQUET_PREFIX", "transit_posts").strip("/")
    if data_prefix:
        return _s3_dir_uri(bucket, data_prefix, tw)
    return _s3_dir_uri(bucket, tw)


def _message_text(row: pd.Series) -> str:
    """Compone un texto único por fila a partir de campos típicos RSS/Twitter JSON."""
    chunks: list[str] = []
    for col in ("summary", "title", "content", "description", "text", "body"):
        if col in row.index and pd.notna(row[col]):
            s = str(row[col]).strip()
            if s:
                chunks.append(s)
    if not chunks and "link" in row.index and pd.notna(row["link"]):
        chunks.append(str(row["link"]).strip())
    if not chunks:
        return ""
    text = "\n".join(chunks)
    return text[:8000]


def _stable_id(row: pd.Series, text: str) -> str:
    if "link" in row.index and pd.notna(row["link"]) and str(row["link"]).strip():
        return str(row["link"]).strip()[:512]
    h = hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()
    return f"hash:{h}"


def _row_metadata(row: pd.Series) -> dict[str, str]:
    meta: dict[str, str] = {}
    for col in ("link", "title", "published", "id", "author", "source"):
        if col in row.index and pd.notna(row[col]):
            meta[col] = str(row[col])[:1024]
    return meta


def main() -> int:
    path = _trusted_twitter_path()
    so = _storage_options()
    print(f"Leyendo Parquet desde: {path}")

    try:
        df = pd.read_parquet(path, storage_options=so)
    except FileNotFoundError:
        print("No se encontró la ruta Parquet (¿aún no hay datos en trusted?).")
        return 0
    except Exception as ex:
        print(f"Error leyendo Parquet: {ex}")
        return 1

    if df.empty:
        print("DataFrame vacío; nada que indexar.")
        return 0

    texts: list[str] = []
    ids: list[str] = []
    metas: list[dict[str, str]] = []
    for _, row in df.iterrows():
        t = _message_text(row)
        if not t.strip():
            continue
        ids.append(_stable_id(row, t))
        texts.append(t)
        metas.append(_row_metadata(row))

    if not ids:
        print("No hay textos no vacíos para embeddar.")
        return 0

    # Deduplicar por id (p. ej. mismo link en varias filas)
    seen: dict[str, int] = {}
    u_ids, u_texts, u_metas = [], [], []
    for i, sid in enumerate(ids):
        if sid in seen:
            continue
        seen[sid] = i
        u_ids.append(sid)
        u_texts.append(texts[i])
        u_metas.append(metas[i])

    host = _env("CHROMA_HOST", "chromadb")
    port = int(_env("CHROMA_PORT", "8000"))
    collection_name = _env("CHROMA_COLLECTION_TWITTER", "twitter_transit_posts")

    print(f"Conectando a Chroma {host}:{port} …")
    client = chromadb.HttpClient(host=host, port=port)

    coll = client.get_or_create_collection(
        name=collection_name,
        metadata={"hnsw:space": "cosine", "description": "Twitter / RSS transit embeddings"},
    )

    model_name = _env("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
    print(f"Cargando modelo {model_name} …")
    model = SentenceTransformer(model_name)

    batch = int(_env("EMBEDDING_BATCH_SIZE", "64"))
    all_emb: list[list[float]] = []
    for i in range(0, len(u_texts), batch):
        chunk = u_texts[i : i + batch]
        emb = model.encode(chunk, convert_to_numpy=True, show_progress_bar=False)
        all_emb.extend(emb.tolist())

    # Chroma add en trozos
    chunk_size = 256
    for i in range(0, len(u_ids), chunk_size):
        coll.upsert(
            ids=u_ids[i : i + chunk_size],
            documents=u_texts[i : i + chunk_size],
            metadatas=u_metas[i : i + chunk_size],
            embeddings=all_emb[i : i + chunk_size],
        )
        print(f"  Upsert lote {i // chunk_size + 1} ({len(u_ids[i : i + chunk_size])} docs)")

    print(f"Listo: {len(u_ids)} vectores en la colección '{collection_name}'.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
