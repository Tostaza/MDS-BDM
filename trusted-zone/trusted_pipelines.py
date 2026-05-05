"""
Pipelines de trusted per tipus de dades (semi-estructurat / estructurat).

- **transit_posts/** (Nitter / RSS JSON → Delta a persistent-landing): llegeix totes les
  arrels Parquet sota aquest prefix, les fusiona en un sol dataset, aplica camps plans
  útils per retrieval (text sense HTML, URLs de vídeo, timestamps) i escriu un únic
  Parquet a `trusted-zone/.../transit_posts/`.
- **sct/traffic_incidents/**: la bronze ja grava `file_name` + `xml_content`; aquí es
  refinen strings, longitud, dedup per fitxer i filtres opcionals (p. ex. mencions a Barcelona).
"""

from __future__ import annotations

import os
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructType
from pyspark.sql.utils import AnalysisException

from cleaner import CleanOptions, Cleaner


def _col_dtype(df: DataFrame, colname: str):
    for f in df.schema.fields:
        if f.name == colname:
            return f.dataType
    return None


def _env(name: str, default: str) -> str:
    v = os.environ.get(name)
    return v if v is not None and v != "" else default


def _truthy(name: str, default: str = "false") -> bool:
    return _env(name, default).lower() in ("1", "true", "yes", "on")


def partition_roots_by_pipeline(landing_base: str, roots: Sequence[str]) -> tuple[list[str], list[str], list[str]]:
    """
    Separa les arrels retornades per `discover_parquet_roots` en:
    (transit_posts, sct/traffic_incidents, resta).
    `landing_base` ha d'estar sense barra final inconsistent (es fa rstrip internament).
    """
    from trusted_paths import relative_path_under_base

    lb = landing_base.rstrip("/")
    transit: list[str] = []
    sct: list[str] = []
    rest: list[str] = []
    transit_key = _env("TRUSTED_TRANSIT_REL_PREFIX", "transit_posts").strip("/").lower()
    sct_key = _env("TRUSTED_SCT_INCIDENTS_REL_PREFIX", "sct/traffic_incidents").strip("/").lower()

    for r in roots:
        rel = relative_path_under_base(lb, r.rstrip("/")).replace("\\", "/")
        rl = rel.lower()
        if rl == transit_key or rl.startswith(transit_key + "/"):
            transit.append(r)
        elif rl == sct_key or rl.startswith(sct_key + "/"):
            sct.append(r)
        else:
            rest.append(r)
    return transit, sct, rest


def _flatten_transit_rss_df(df: DataFrame) -> DataFrame:
    """Extreu camps útils per RAG / retrieval des del JSON estil Nitter RSS."""
    out = df

    for struct_col, text_alias in (
        ("title_detail", "title_detail_text"),
        ("summary_detail", "summary_detail_text"),
    ):
        if struct_col not in out.columns:
            continue
        dt = _col_dtype(out, struct_col)
        if isinstance(dt, StructType) and any(f.name == "value" for f in dt.fields):
            out = out.withColumn(text_alias, F.col(f"{struct_col}.value"))

    if "authors" in out.columns:
        dt = _col_dtype(out, "authors")
        if isinstance(dt, ArrayType) and isinstance(dt.elementType, StructType):
            names = [f.name for f in dt.elementType.fields]
            if "name" in names:
                out = out.withColumn(
                    "author_from_authors",
                    F.element_at(F.col("authors"), 1).getField("name"),
                )

    if "author" in out.columns:
        if "author_from_authors" in out.columns:
            out = out.withColumn(
                "author_resolved",
                F.coalesce(F.col("author_from_authors"), F.col("author")),
            )
        else:
            out = out.withColumn("author_resolved", F.col("author"))
    elif "author_from_authors" in out.columns:
        out = out.withColumn("author_resolved", F.col("author_from_authors"))
    else:
        out = out.withColumn("author_resolved", F.lit(None).cast(StringType()))

    if "published" in out.columns:
        out = out.withColumn(
            "published_ts",
            F.to_timestamp(F.col("published"), "EEE, dd MMM yyyy HH:mm:ss z"),
        )

    html_col = None
    if "summary" in out.columns:
        html_col = F.col("summary")
    elif "summary_detail_text" in out.columns:
        html_col = F.col("summary_detail_text")

    if html_col is not None:
        no_tags = F.regexp_replace(html_col, "<[^>]+>", " ")
        out = out.withColumn(
            "summary_text_plain",
            F.trim(F.regexp_replace(no_tags, r"\s+", " ")),
        )
    else:
        out = out.withColumn("summary_text_plain", F.lit(None).cast(StringType()))

    src_media = html_col if html_col is not None else F.lit("")
    out = out.withColumn("media_mp4_url", F.regexp_extract(src_media, r'src="([^"]+\.mp4[^"]*)"', 1))

    if "id" in out.columns:
        out = out.withColumn("tweet_id_str", F.col("id").cast("string"))
    else:
        out = out.withColumn("tweet_id_str", F.lit(None).cast(StringType()))

    return out


def process_transit_posts_union(
    spark: SparkSession,
    cleaner: Cleaner,
    twitter_roots: Sequence[str],
    trusted_output_uri: str,
) -> bool:
    """
    Llegeix totes les carpetes Parquet (p. ex. una taula Delta per post), fusiona amb
    mergeSchema, aplica flatten + Cleaner i escriu un sol dataset a ``trusted_output_uri``.
    """
    paths = [r.rstrip("/") for r in twitter_roots if (r or "").strip()]
    if not paths:
        print("  transit_posts: cap arrel a processar.")
        return False

    try:
        df = spark.read.option("mergeSchema", "true").parquet(*paths)
    except AnalysisException as ex:
        print(f"  transit_posts: error llegint Parquet: {ex}")
        return False
    except Exception as ex:
        print(f"  transit_posts: error inesperat: {ex}")
        return False

    df = _flatten_transit_rss_df(df)

    dup_subset: list[str] | None = None
    if "link" in df.columns:
        dup_subset = ["link"]
    elif "id" in df.columns:
        dup_subset = ["id"]

    opts = CleanOptions(
        trim_strings=True,
        empty_string_as_null=True,
        drop_duplicates=True,
        duplicate_subset=dup_subset,
        dropna_subset=None,
    )
    cleaned = cleaner.clean(df, options=opts)
    out = trusted_output_uri.rstrip("/") + "/"
    cleaner.write_parquet(cleaned, out, mode="overwrite")
    try:
        n = cleaned.count()
    except Exception:
        n = -1
    print(f"  transit_posts → dataset unificat a {out!r} (files comptades: {n}).")
    return True


def enrich_sct_xml_content_df(df: DataFrame) -> DataFrame:
    """Transformacions sobre la taula ja llegida (columna ``xml_content``)."""
    out = df
    if "xml_content" not in out.columns:
        return out

    out = out.withColumn("xml_content", F.trim(F.col("xml_content")))
    out = out.withColumn("xml_len", F.length(F.col("xml_content")))

    if _truthy("SCT_FILTER_BARCELONA_ONLY", "false"):
        out = out.filter(F.lower(F.col("xml_content")).contains("barcelona"))

    out = out.withColumn(
        "title_guess",
        F.regexp_extract(F.col("xml_content"), "(?i)<title>([^<]{1,500})</title>", 1),
    )
    return out


def process_sct_traffic_incidents_parquet(
    cleaner: Cleaner,
    input_root_uri: str,
    output_root_uri: str,
) -> None:
    """Llegeix el Delta/Parquet de incidents SCT, enriqueix XML i escriu a trusted."""
    df = cleaner.read_parquet(input_root_uri.rstrip("/") + "/")
    df = enrich_sct_xml_content_df(df)

    dup_subset: tuple[str, ...] | None = ("file_name",) if "file_name" in df.columns else None
    opts = CleanOptions(
        trim_strings=True,
        empty_string_as_null=True,
        drop_duplicates=bool(dup_subset),
        duplicate_subset=dup_subset,
    )
    cleaned = cleaner.clean(df, options=opts)
    cleaner.write_parquet(cleaned, output_root_uri.rstrip("/") + "/", mode="overwrite")
    print(f"  sct/traffic_incidents → {output_root_uri.rstrip('/') + '/'!r}")


def clean_options_for_accidents_gu(df: DataFrame) -> CleanOptions:
    """Opcions de dedup per accidents GU (CSV→Delta) si existeix la clau natural."""
    if "Numero_expedient" in df.columns:
        return CleanOptions(
            trim_strings=True,
            empty_string_as_null=True,
            drop_duplicates=True,
            duplicate_subset=("Numero_expedient",),
        )
    return CleanOptions()
