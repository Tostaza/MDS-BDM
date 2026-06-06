"""
Pipelines especials de trusted: transit_posts (RSS Nitter), SCT incidents (WFS/GML),
accidents GU i AEMET horari. partition_roots_by_pipeline reparteix les arrels entre ells.
"""

from __future__ import annotations

import ast
import json
import os
from typing import Any, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
import xml.etree.ElementTree as ET
from pyspark.sql.types import ArrayType, MapType, StructType, StructField, StringType

from cleaner import CleanOptions, Cleaner


def _col_dtype(df: DataFrame, colname: str):
    """Retorna el DataType de la columna o None si no existeix."""
    for f in df.schema.fields:
        if f.name == colname:
            return f.dataType
    return None


def _env(name: str, default: str) -> str:
    v = os.environ.get(name)
    return v if v is not None and v != "" else default


def _truthy(name: str, default: str = "false") -> bool:
    return _env(name, default).lower() in ("1", "true", "yes", "on")


"""
Spark accepta implkementació de funcions externes definidies per nosaltres i aqui li especifiquem el output
de la funció nostre UDF, quina format volem que ens retorni en les dades després de fer / aplicar
la nostra funció.
"""
AEMET_HOURLY_STRUCT = StructType(
    [
        StructField("aemet_municipio_id", StringType(), True),
        StructField("nombre_municipio", StringType(), True),
        StructField("elaborado", StringType(), True),
        StructField("provincia", StringType(), True),
        StructField("dia_fecha", StringType(), True),
        StructField("orto", StringType(), True),
        StructField("ocaso", StringType(), True),
        StructField("hora_periodo", StringType(), True),
        StructField("temperatura", StringType(), True),
        StructField("sens_termica", StringType(), True),
        StructField("nieve", StringType(), True),
        StructField("precipitacion", StringType(), True),
        StructField("humedad_relativa", StringType(), True),
        StructField("prob_nieve", StringType(), True),
        StructField("prob_precipitacion", StringType(), True),
        StructField("prob_tormenta", StringType(), True),
        StructField("estado_cielo", StringType(), True),
        StructField("estado_cielo_descripcion", StringType(), True),
        StructField("viento_velocidad", StringType(), True),
        StructField("viento_direccion", StringType(), True),
        StructField("viento_racha_max", StringType(), True),
    ]
)


def _aemet_str(v: Any) -> str | None:
    if v is None:
        return None
    return str(v)

"""
El que farem aquí és per les diferents variables target, les adaptarem a format de dict.
"""
def _normalize_prediccion_cell(prediccion: Any) -> dict | None:
    if prediccion is None:
        return None
    if isinstance(prediccion, dict):
        return prediccion
    if isinstance(prediccion, str):
        s = prediccion.strip()
        if not s:
            return None
        try:
            parsed = json.loads(s)
        except json.JSONDecodeError:
            try:
                parsed = ast.literal_eval(s)
            except (SyntaxError, ValueError):
                return None
        return parsed if isinstance(parsed, dict) else None
    return None

"""
Comprovació que és un format de hora, que realment correspon a una hora concreta.
"""
def _is_aemet_hour_period(p: Any) -> bool:
    try:
        if p is None:
            return False
        v = int(str(p).strip())
        return 0 <= v <= 23
    except ValueError:
        return False


"""
Per cada entrada ens retornara una llista de les diferents hores que estaria guardant.
e. g : retornaria [0, 1, .... , 23] les diferents hores per les que tenim prediccions.
"""
def _hour_periods_for_day(day: dict) -> list[str]:
    skip = frozenset({"orto", "ocaso", "fecha"})
    found: set[int] = set()
    for k, v in day.items():
        if k in skip or not isinstance(v, list):
            continue
        for item in v:
            if not isinstance(item, dict) or "periodo" not in item:
                continue
            if _is_aemet_hour_period(item["periodo"]):
                found.add(int(str(item["periodo"]).strip()))
    return [str(h) for h in sorted(found)]


def _metric_value_for_hour(arr: Any, hora: str) -> str | None:
    if not isinstance(arr, list):
        return None
    h = int(str(hora).strip())
    for item in arr:
        if not isinstance(item, dict) or "periodo" not in item:
            continue
        if int(str(item["periodo"]).strip()) != h:
            continue
        val = item.get("value")
        return None if val is None else str(val)
    return None


def _estado_cielo_for_hour(arr: Any, hora: str) -> tuple[str | None, str | None]:
    if not isinstance(arr, list):
        return None, None
    h = int(str(hora).strip())
    for item in arr:
        if not isinstance(item, dict) or "periodo" not in item:
            continue
        if int(str(item["periodo"]).strip()) != h:
            continue
        cod = item.get("value")
        desc = item.get("descripcion")
        return (None if cod is None else str(cod), None if desc is None else str(desc))
    return None, None


def _wind_for_hour(day: dict, hora: str) -> tuple[str | None, str | None, str | None]:
    arr = day.get("vientoAndRachaMax")
    if not isinstance(arr, list):
        return None, None, None
    h = int(str(hora).strip())
    velocidad, direccion, racha = None, None, None
    for item in arr:
        if not isinstance(item, dict):
            continue
        ip = item.get("periodo")
        if ip is None:
            continue
        if int(str(ip).strip()) != h:
            continue
        vel = item.get("velocidad")
        dire = item.get("direccion")
        if isinstance(vel, list) and vel:
            velocidad = str(vel[0])
        if isinstance(dire, list) and dire:
            direccion = str(dire[0])
        if "value" in item and item["value"] is not None:
            racha = str(item["value"])
    return velocidad, direccion, racha


def aemet_prediccion_to_hourly_rows(
    prediccion: Any,
    fid: Any,
    nombre: Any,
    elaborado: Any,
    provincia: Any,
) -> list:
    """Converteix una entrada JSON AEMET en una fila per cada hora (0-23) i dia."""
    pred = _normalize_prediccion_cell(prediccion)
    if not pred:
        return []

    out: list[tuple] = []
    fid_s = _aemet_str(fid)
    nom_s = _aemet_str(nombre)
    elab_s = _aemet_str(elaborado)
    prov_s = _aemet_str(provincia)

    for day in pred.get("dia") or []:
        if not isinstance(day, dict):
            continue
        dia_fecha = _aemet_str(day.get("fecha"))
        oro = _aemet_str(day.get("orto"))
        ocaso = _aemet_str(day.get("ocaso"))
        for hora in _hour_periods_for_day(day):
            temp = _metric_value_for_hour(day.get("temperatura"), hora)
            sens = _metric_value_for_hour(day.get("sensTermica"), hora)
            nieve = _metric_value_for_hour(day.get("nieve"), hora)
            prec = _metric_value_for_hour(day.get("precipitacion"), hora)
            hum = _metric_value_for_hour(day.get("humedadRelativa"), hora)
            pn = _metric_value_for_hour(day.get("probNieve"), hora)
            pp = _metric_value_for_hour(day.get("probPrecipitacion"), hora)
            pt = _metric_value_for_hour(day.get("probTormenta"), hora)
            ec, ecd = _estado_cielo_for_hour(day.get("estadoCielo"), hora)
            vv, vd, vr = _wind_for_hour(day, hora)
            out.append(
                (
                    fid_s,
                    nom_s,
                    elab_s,
                    prov_s,
                    dia_fecha,
                    oro,
                    ocaso,
                    hora,
                    temp,
                    sens,
                    nieve,
                    prec,
                    hum,
                    pn,
                    pp,
                    pt,
                    ec,
                    ecd,
                    vv,
                    vd,
                    vr,
                )
            )
    return out


def flatten_aemet_hourly_df(df: DataFrame) -> DataFrame:
    """Expandeix prediccions AEMET: una fila d entrada -> N files (dia + hora)."""
    if "prediccion" not in df.columns:
        return df

    id_c = F.col("id") if "id" in df.columns else F.lit(None).cast(StringType())
    nombre_c = F.col("nombre") if "nombre" in df.columns else F.lit(None).cast(StringType())
    elab_c = F.col("elaborado") if "elaborado" in df.columns else F.lit(None).cast(StringType())
    prov_c = F.col("provincia") if "provincia" in df.columns else F.lit(None).cast(StringType())

    to_hours = F.udf(
        aemet_prediccion_to_hourly_rows,
        ArrayType(AEMET_HOURLY_STRUCT),
    )
    tmp = df.withColumn("_hourly", to_hours(F.col("prediccion"), id_c, nombre_c, elab_c, prov_c))
    exploded = tmp.withColumn("_h", F.explode(F.col("_hourly")))
    field_names = [f.name for f in AEMET_HOURLY_STRUCT.fields]
    return exploded.select([F.col(f"_h.{n}").alias(n) for n in field_names])

"""
Processar Aemet predicitons, fent ús de funcions auxiliars que ens peremtra ntenir-ho finalment com strcutured data.
"""
def process_aemet_predictions_parquet(
    cleaner: Cleaner,
    input_root_uri: str,
    output_root_uri: str,
) -> None:
    """Llegeix Parquet AEMET, expandeix a granularitat horaria i escriu Delta a trusted."""
    df = cleaner.read_delta(input_root_uri.rstrip("/") + "/")
    df = flatten_aemet_hourly_df(df)
    dup_subset = (
        "aemet_municipio_id",
        "elaborado",
        "dia_fecha",
        "hora_periodo",
    )
    missing = [c for c in dup_subset if c not in df.columns]
    opts = CleanOptions(
        trim_strings=True,
        empty_string_as_null=True,
        drop_duplicates=not missing,
        duplicate_subset=dup_subset if not missing else None,
    )
    cleaned = cleaner.clean(df, options=opts)
    cleaner.write_delta(cleaned, output_root_uri.rstrip("/") + "/", mode="overwrite")
    print(f"  aemet (horari) -> {output_root_uri.rstrip('/') + '/'!r}")


"""
Per classificar els diferents datasets segons d'on provenen, SCT | Transit | AEMET | REST.
"""
def partition_roots_by_pipeline(
    landing_base: str, roots: Sequence[str]
) -> tuple[list[str], list[str], list[str], list[str]]:
    """Reparteix arrels en (transit_posts, sct/traffic_incidents, aemet, resta)."""
    from trusted_paths import relative_path_under_base

    lb = landing_base.rstrip("/")
    transit: list[str] = []
    sct: list[str] = []
    aemet: list[str] = []
    rest: list[str] = []
    transit_key = _env("TRUSTED_TRANSIT_REL_PREFIX", "transit_posts").strip("/").lower()
    sct_key = _env("TRUSTED_SCT_INCIDENTS_REL_PREFIX", "sct/traffic_incidents").strip("/").lower()
    aemet_key = _env("TRUSTED_AEMET_REL_PREFIX", "aemet").strip("/").lower()

    for r in roots:
        rel = relative_path_under_base(lb, r.rstrip("/")).replace("\\", "/")
        rl = rel.lower()
        if rl == transit_key or rl.startswith(transit_key + "/"):
            transit.append(r)
        elif rl == sct_key or rl.startswith(sct_key + "/"):
            sct.append(r)
        elif rl == aemet_key or rl.startswith(aemet_key + "/"):
            aemet.append(r)
        else:
            rest.append(r)
    return transit, sct, aemet, rest

"""
Aqui fem dels tweets de transit, fem que sigui un format tabular per guardar-se'n.
"""
def _flatten_transit_rss_df(df: DataFrame) -> DataFrame:
    """Extreu camps utils per RAG des del JSON estil Nitter RSS."""
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


def process_transit_posts(
    spark: SparkSession,
    cleaner: Cleaner,
    twitter_root: str,
    trusted_output_uri: str,
) -> bool:
    """Llegeix la taula Delta centralitzada de transit_posts, aplana RSS i escriu a trusted."""
    if not (twitter_root or "").strip():
        print("  transit_posts: cap arrel a processar.")
        return False

    try:
        df = cleaner.read_delta(twitter_root.rstrip("/") + "/")
    except AnalysisException as ex:
        print(f"  transit_posts: error llegint Delta: {ex}")
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
        duplicate_subset=dup_subset
    )
    cleaned = cleaner.clean(df, options=opts)
    out = trusted_output_uri.rstrip("/") + "/"
    cleaner.write_delta(cleaned, out, mode="overwrite")
    try:
        n = cleaned.count()
    except Exception:
        n = -1
    print(f"  transit_posts -> {out!r} (files comptades: {n}).")
    return True


"""
Donat un xml el que fara es per cada incident generar una row, així passem el xml a un format strcutured
Per tant, per cada entrada en el xml d'incidencia, extreurem una row amb la seva informació.
"""
def parse_sct_xml_to_rows(xml_text: str) -> list[dict]:
    """Parseja el XML WFS/GML de SCT i retorna un dict pla per cada featureMember."""
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
    """Parseja xml_content (WFS/GML de SCT) i explota cada featureMember en una fila plana."""
    if "xml_content" not in df.columns:
        return df

    out = df
    if _truthy("SCT_FILTER_BARCELONA_ONLY", "false"):
        out = out.filter(F.lower(F.col("xml_content")).contains("barcelona"))

    parse_udf = F.udf(
        parse_sct_xml_to_rows,
        ArrayType(MapType(StringType(), StringType()))
    )

    out = out.withColumn("features", parse_udf(F.col("xml_content")))
    out = out.withColumn("feature", F.explode("features"))

    FIELDS = [
        "identificador", "tipus", "subtipus", "carretera",
        "pk_inici", "pk_fi", "causa", "cap_a", "data",
        "nivell", "sentit", "descripcio", "descripcio_tipus",
    ]
    for field in FIELDS:
        out = out.withColumn(field, F.col("feature")[field])

    out = out.drop("features", "feature", "xml_content")

    return out

"""

"""
def process_sct_traffic_incidents_parquet(
    cleaner: Cleaner,
    input_root_uri: str,
    output_root_uri: str,
) -> None:
    """Llegeix Delta SCT, parseja el XML, deduplicat per identificador i escriu a trusted."""
    df = cleaner.read_delta(input_root_uri.rstrip("/") + "/")
    df = enrich_sct_xml_content_df(df)

    dup_subset = ("identificador",) if "identificador" in df.columns else None
    opts = CleanOptions(
        trim_strings=True,
        empty_string_as_null=True,
        drop_duplicates=bool(dup_subset),
        duplicate_subset=dup_subset,
    )
    cleaned = cleaner.clean(df, options=opts)
    cleaner.write_delta(cleaned, output_root_uri.rstrip("/") + "/", mode="overwrite")

"""
Per al dataset de la Guardia urbana el que farem és mirar si hi ha numero expedient, per així vigilar que
no hi hagin duplicats per aquell atribut. Segons si esta o no, doncs aplicarem un tipus de cleaner o un altre.
"""
def clean_options_for_accidents_gu(df: DataFrame) -> CleanOptions:
    """Retorna CleanOptions per accidents GU, deduplicant per Numero_expedient si existeix."""
    if "Numero_expedient" in df.columns:
        return CleanOptions(
            trim_strings=True,
            empty_string_as_null=True,
            drop_duplicates=True,
            duplicate_subset=("Numero_expedient",),
        )
    return CleanOptions()
