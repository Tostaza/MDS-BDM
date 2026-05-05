"""
**Pipelines especiales de la capa trusted** (además del Parquet genérico con ``Cleaner``).

Este módulo clasifica carpetas Parquet bajo ``persistent-landing-zone`` y aplica lógica distinta:

1. **Twitter / Nitter (``transit_posts/``)**  
   Muchas carpetas Delta/Parquet (un post cada una) se leen con ``mergeSchema``, se aplana
   el JSON tipo RSS (``title_detail``, ``summary``, HTML → texto plano, URL de vídeo,
   ``published_ts``, etc.), se deduplica por ``link`` o ``id`` y se escribe **un único**
   dataset Parquet en ``trusted-zone/.../transit_posts/`` (para Chroma / dashboard).

2. **Incidents SCT (``sct/traffic_incidents/``)**  
   La bronze ya guardó ``file_name`` + ``xml_content``. Aquí se enriquece (trim, longitud,
   ``title_guess`` por regex, filtro opcional ``SCT_FILTER_BARCELONA_ONLY``), deduplicado por
   ``file_name`` y escritura Parquet en trusted.

3. **Accidents GU**  
   ``clean_options_for_accidents_gu`` devuelve ``CleanOptions`` con deduplicado por
   ``Numero_expedient`` cuando existe la columna.

``partition_roots_by_pipeline`` reparte las URIs devueltas por ``discover_parquet_roots`` en
tres grupos: transit, SCT y resto (Parquet estándar en ``trusted_zone.main``).
"""

from __future__ import annotations

import os
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
import xml.etree.ElementTree as ET
from pyspark.sql.types import ArrayType, MapType, StructType, StructField, StringType

from cleaner import CleanOptions, Cleaner


def _col_dtype(df: DataFrame, colname: str):
    """Devuelve el ``DataType`` de la columna ``colname`` en el schema del DataFrame, o ``None``."""
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


def parse_sct_xml_to_rows(xml_text: str) -> list[dict]:
    """
    Parsea el XML crudo SCT (WFS/GML) y devuelve un dict por cada
    gml:featureMember con los campos planos de cite:mct2_v_afectacions_data.
    """
    if not xml_text:
        return []

    NS = {
        "gml":  "http://www.opengis.net/gml",
        "cite": "http://www.opengeospatial.net/cite",
    }

    FIELDS = [
        "identificador", "tipus", "subtipus", "carretera",
        "pk_inici", "pk_fi", "causa", "cap_a", "data",
        "nivell", "sentit", "descripcio", "descripcio_tipus",
    ]

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    rows = []
    for member in root.findall("gml:featureMember", NS):
        feature = member.find("cite:mct2_v_afectacions_data", NS)
        if feature is None:
            continue
        row = {}
        for field in FIELDS:
            el = feature.find(f"cite:{field}", NS)
            row[field] = el.text.strip() if el is not None and el.text else None
        rows.append(row)

    return rows


def enrich_sct_xml_content_df(df: DataFrame) -> DataFrame:
    """
    Parsea xml_content (WFS/GML de SCT) y explota cada featureMember
    en una fila propia con columnas planas.
    Mantiene file_name y cualquier otra columna que venga del Parquet original.
    """
    if "xml_content" not in df.columns:
        return df

    # Filtrar per la ciutat de bcn, ens centrem aquí, llavors aqui es on fem el fltre i reduim dimensio del size de elements amb els que treballemm.
    out = df
    if _truthy("SCT_FILTER_BARCELONA_ONLY", "false"):
        out = out.filter(F.lower(F.col("xml_content")).contains("barcelona"))

    # UDF: string XML → array de dicts (uno por featureMember)
    parse_udf = F.udf(
        parse_sct_xml_to_rows,
        ArrayType(MapType(StringType(), StringType()))
    )

    # Aplica el UDF → columna "features" con array de dicts
    out = out.withColumn("features", parse_udf(F.col("xml_content")))

    # Explode: cada dict del array → fila propia
    out = out.withColumn("feature", F.explode("features"))

    # Extrae cada campo del map como columna plana
    FIELDS = [
        "identificador", "tipus", "subtipus", "carretera",
        "pk_inici", "pk_fi", "causa", "cap_a", "data",
        "nivell", "sentit", "descripcio", "descripcio_tipus",
    ]
    for field in FIELDS:
        out = out.withColumn(field, F.col("feature")[field])

    # Limpia columnas intermedias
    out = out.drop("features", "feature", "xml_content")

    return out


def process_sct_traffic_incidents_parquet(
    cleaner: Cleaner,
    input_root_uri: str,
    output_root_uri: str,
) -> None:
    """
    Lee Parquet/Delta de incidents SCT desde ``input_root_uri``, aplica ``enrich_sct_xml_content_df``,
    ``Cleaner.clean`` con deduplicado por ``file_name`` y escribe en ``output_root_uri``.
    """
    df = cleaner.read_parquet(input_root_uri.rstrip("/") + "/")
    df = enrich_sct_xml_content_df(df)   # ← ahora parsea y explota el XML

    # Deduplicación por identificador, basicament es eliminar auqells que tinguin identificador igual.
    dup_subset = ("identificador",) if "identificador" in df.columns else None
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
    """
    Construye ``CleanOptions`` para el prefijo ``ajuntament_bcn/accidents_gu`` en trusted.

    Si existe ``Numero_expedient``, deduplica por esa columna (clave natural del expediente).
    """
    if "Numero_expedient" in df.columns:
        return CleanOptions(
            trim_strings=True,
            empty_string_as_null=True,
            drop_duplicates=True,
            duplicate_subset=("Numero_expedient",),
        )
    return CleanOptions()
