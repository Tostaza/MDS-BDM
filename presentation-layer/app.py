"""
Dashboard (capa presentación): lectura de Parquet e imágenes ya materializados en el bucket
de explotación, modelos sklearn en memoria para la UI y componentes Streamlit.

Los joins y el ETL pesado están en ``exploitation-zone/`` (Spark), no en esta carpeta.

Uso (Docker): http://localhost:8501 tras `docker compose up dashboard`.
"""

from __future__ import annotations

import os
import time

import chromadb
import pandas as pd
import streamlit as st

from analytics import render_parquet_analysis, render_tweet_feed
from dashboard_views import render_unified_dashboard
from data_access import (
    get_s3_object_bytes,
    list_traffic_camera_gallery_objects,
    list_traffic_camera_gallery_sites,
    load_parquet_prefix,
    load_trams_map,
    traffic_cameras_gallery_prefix,
)
from hot_path import read_parking_counters, start_parking_consumer
from map_view import render_trams_map


def _env(name: str, default: str) -> str:
    v = os.environ.get(name)
    return v if v is not None and v != "" else default


def _inject_dashboard_css() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,400;0,9..40,500;0,9..40,600;0,9..40,700;1,9..40,400&family=JetBrains+Mono:wght@400;500&display=swap');
        html, body, [class*="stApp"] {
            font-family: 'DM Sans', 'Outfit', system-ui, sans-serif;
        }
        code, pre, .stMarkdown code {
            font-family: 'JetBrains Mono', monospace !important;
        }
        .stApp {
            background: radial-gradient(ellipse 120% 80% at 50% -20%, rgba(45, 212, 191, 0.12), transparent 50%),
                radial-gradient(ellipse 80% 50% at 100% 50%, rgba(99, 102, 241, 0.08), transparent 45%),
                linear-gradient(168deg, #070b14 0%, #0f172a 38%, #0c1a2e 72%, #082f49 100%);
            background-attachment: fixed;
        }
        [data-testid="stAppViewContainer"] > .main {
            background: transparent;
        }
        .block-container {
            padding-top: 1.5rem;
            padding-bottom: 3.5rem;
            max-width: 1180px;
        }
        [data-testid="stHeader"] {
            background: rgba(7, 11, 20, 0.72) !important;
            backdrop-filter: blur(12px);
            border-bottom: 1px solid rgba(94, 234, 212, 0.08);
        }
        h1, h2, h3 { color: #f8fafc !important; letter-spacing: -0.03em; font-weight: 700 !important; }
        h3 { font-size: 1.15rem !important; margin-top: 0.35rem !important; }
        .stMarkdown p, .stMarkdown li, label, span[data-testid="stMarkdownContainer"] p {
            color: #cbd5e1;
        }
        [data-baseweb="select"] > div, [data-baseweb="input"] > div {
            background-color: rgba(15, 23, 42, 0.92) !important;
            border: 1px solid rgba(94, 234, 212, 0.22) !important;
            border-radius: 12px !important;
        }
        [data-testid="stSelectbox"] label, [data-testid="stSlider"] label, [data-testid="stNumberInput"] label {
            font-weight: 600 !important;
            color: #e2e8f0 !important;
            font-size: 0.88rem !important;
        }
        .stTextInput input, .stNumberInput input {
            background-color: rgba(15, 23, 42, 0.95) !important;
            color: #f1f5f9 !important;
            border: 1px solid rgba(94, 234, 212, 0.25) !important;
            border-radius: 12px !important;
        }
        div[data-testid="stExpander"] {
            background: linear-gradient(145deg, rgba(15, 23, 42, 0.65), rgba(30, 41, 59, 0.35));
            border: 1px solid rgba(94, 234, 212, 0.14);
            border-radius: 16px;
        }
        .stDataFrame { border-radius: 14px; overflow: hidden; border: 1px solid rgba(148, 163, 184, 0.12); }
        [data-testid="stVerticalBlockBorderWrapper"] {
            background: linear-gradient(155deg, rgba(15, 23, 42, 0.55), rgba(30, 41, 59, 0.25)) !important;
            border-radius: 20px !important;
            border: 1px solid rgba(94, 234, 212, 0.2) !important;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.35), inset 0 1px 0 rgba(255,255,255,0.04) !important;
            padding: 0.65rem 0.85rem 1rem !important;
        }
        div[data-testid="stAlert"] {
            background-color: rgba(15, 23, 42, 0.9) !important;
            border: 1px solid rgba(148, 163, 184, 0.18);
            border-radius: 14px;
        }
        .stButton > button {
            background: linear-gradient(135deg, #0d9488 0%, #0e7490 45%, #6366f1 130%);
            color: white;
            border: none;
            border-radius: 12px;
            font-weight: 600;
            padding: 0.55rem 1.4rem;
            box-shadow: 0 4px 20px rgba(13, 148, 136, 0.4);
            transition: transform 0.15s ease, box-shadow 0.15s ease;
        }
        .stButton > button:hover {
            box-shadow: 0 8px 28px rgba(34, 211, 238, 0.35);
            border: 1px solid rgba(165, 243, 252, 0.45);
        }
        div[data-testid="stSlider"] > div > div > div {
            background: linear-gradient(90deg, #14b8a6, #6366f1) !important;
        }
        .news-scroll-box, .tweet-scroll-box {
            max-height: 380px;
            overflow-y: auto;
            overflow-x: hidden;
            padding: 0.75rem 0.6rem 0.75rem 0.25rem;
            margin-top: 0.5rem;
            border-radius: 16px;
            border: 1px solid rgba(94, 234, 212, 0.18);
            background: rgba(7, 11, 20, 0.45);
            box-shadow: inset 0 2px 12px rgba(0,0,0,0.25);
        }
        .news-scroll-box::-webkit-scrollbar, .tweet-scroll-box::-webkit-scrollbar { width: 8px; }
        .news-scroll-box::-webkit-scrollbar-thumb, .tweet-scroll-box::-webkit-scrollbar-thumb {
            background: rgba(45, 212, 191, 0.35);
            border-radius: 8px;
        }
        .tweet-card-wrap { margin-bottom: 0.35rem; }
        .tweet-card {
            display: flex;
            gap: 0.85rem;
            padding: 0.95rem 1rem;
            background: linear-gradient(135deg, rgba(30, 41, 59, 0.75), rgba(15, 23, 42, 0.85));
            border: 1px solid rgba(148, 163, 184, 0.12);
            border-radius: 14px;
            margin-bottom: 0.5rem;
            box-shadow: 0 4px 16px rgba(0,0,0,0.22);
        }
        .tweet-avatar {
            width: 42px; height: 42px; flex-shrink: 0;
            border-radius: 999px;
            background: linear-gradient(145deg, #2dd4bf, #818cf8);
            box-shadow: 0 0 0 2px rgba(94, 234, 212, 0.2);
        }
        .tweet-body { flex: 1; min-width: 0; }
        .tweet-meta {
            display: flex; align-items: center; flex-wrap: wrap; gap: 0.35rem;
            margin-bottom: 0.35rem;
        }
        .tweet-author { font-weight: 700; color: #f8fafc !important; font-size: 0.92rem; }
        .tweet-date { color: #94a3b8 !important; font-size: 0.8rem; }
        .tweet-dot { color: #475569; }
        .tweet-text {
            color: #e2e8f0 !important; font-size: 0.94rem; line-height: 1.55;
            word-break: break-word;
        }
        .pill-badge {
            display: inline-flex;
            align-items: center;
            padding: 0.2rem 0.65rem;
            border-radius: 999px;
            font-size: 0.72rem;
            font-weight: 600;
            letter-spacing: 0.04em;
            text-transform: uppercase;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _hero() -> None:
    st.markdown(
        """
        <div style="
            padding: 2rem 1.75rem 1.75rem;
            margin-bottom: 1.5rem;
            border-radius: 20px;
            background: linear-gradient(135deg, rgba(13, 148, 136, 0.35) 0%, rgba(8, 145, 178, 0.2) 50%, rgba(99, 102, 241, 0.15) 100%);
            border: 1px solid rgba(94, 234, 212, 0.25);
            box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.45);
        ">
            <p style="margin:0 0 0.35rem 0; font-size:0.75rem; text-transform:uppercase; letter-spacing:0.22em; color:#5eead4; font-weight:600;">BDM · datos en explotación</p>
            <h1 style="margin:0; font-size:clamp(1.85rem, 4.2vw, 2.5rem); font-weight:700; color:#f8fafc; line-height:1.12; text-shadow:0 0 40px rgba(45,212,191,0.25);">Panel de movilidad y clima</h1>
            <p style="margin:0.85rem 0 0 0; font-size:1.05rem; color:#94a3b8; max-width:52rem; line-height:1.6;">
                TomTom, <strong style="color:#e2e8f0;">BCN</strong> (predicción por zona), cámaras, <strong style="color:#e2e8f0;">Kanishka</strong>, AEMET y noticias.
                Más abajo: análisis detallado, explorador Parquet y Chroma. Origen de tablas: job <code style="color:#99f6e4;">exploitation-zone</code>.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _section_title(title: str, subtitle: str | None = None) -> None:
    sub = (
        f'<p style="margin:0.4rem 0 0 0; font-size:0.92rem; color:#94a3b8; line-height:1.45;">{subtitle}</p>'
        if subtitle
        else ""
    )
    st.markdown(
        f"""
        <div style="margin: 1.75rem 0 1rem 0; padding: 0 0 0.65rem 0; border-bottom: 1px solid rgba(94, 234, 212, 0.2);">
            <h2 style="margin:0; font-size:1.28rem; font-weight:700; color:#f1f5f9;">{title}</h2>
            {sub}
        </div>
        """,
        unsafe_allow_html=True,
    )


@st.cache_resource
def chroma_client():
    host = _env("CHROMA_HOST", "chromadb")
    port = int(_env("CHROMA_PORT", "8000"))
    return chromadb.HttpClient(host=host, port=port)


@st.cache_data(ttl=300, show_spinner="Leyendo Parquet desde S3…")
def cached_load_df(bucket: str, prefix: str) -> pd.DataFrame:
    return load_parquet_prefix(bucket, prefix)


@st.cache_data(ttl=300, show_spinner="Cargando mapa de trams desde S3…")
def cached_load_trams_map(bucket: str, prefix: str) -> pd.DataFrame:
    return load_trams_map(bucket, prefix)


def section_transit_twitter(bucket: str, prefix: str) -> None:
    st.caption(
        f"Prefijo: `s3://{bucket}/{prefix.strip('/')}/` · Origen típico: **transit_posts** "
        "(Nitter/RSS aplanado en trusted-zone y copiado a exploitation)."
    )
    df = cached_load_df(bucket, prefix)
    if df.empty:
        st.info(
            "Aún no hay Parquet de tweets en esa ruta. Ejecuta el pipeline **trusted** (transit_posts) "
            "y **exploitation_zone**; revisa `TRANSIT_POSTS_PREFIX` y `DATA_PREFIX`."
        )
        return

    st.markdown("##### Timeline")
    render_tweet_feed(df, "transit")

    with st.expander("Análisis numérico y categórico del mismo dataset", expanded=False):
        render_parquet_analysis(df, "transit_mix")


def section_accidents_gu(bucket: str, prefix: str) -> None:
    st.caption("Accidents GU (Ajuntament) · solo estadísticos y gráficos.")
    df = cached_load_df(bucket, prefix)
    if df.empty:
        st.warning(
            f"Sin datos en `s3://{bucket}/{prefix.strip('/')}/`. Ejecuta trusted → exploitation."
        )
        return
    with st.expander("Muestra de filas", expanded=False):
        st.dataframe(df.head(100), use_container_width=True, height=260)
    render_parquet_analysis(df, "gu")


def section_aemet(bucket: str, prefix: str) -> None:
    st.caption("AEMET en exploitation: tablas / JSON aplanado según el ETL.")
    df = cached_load_df(bucket, prefix)
    if df.empty:
        st.warning(f"Sin Parquet en `s3://{bucket}/{prefix.strip('/')}/`.")
        return
    with st.expander("Muestra de filas", expanded=False):
        st.dataframe(df.head(100), use_container_width=True, height=260)
    render_parquet_analysis(df, "aemet")


def section_kanishka(bucket: str, prefix: str) -> None:
    st.caption("HuggingFace **RainPrediction** (Kanishka): lluvia y variables meteorológicas.")
    df = cached_load_df(bucket, prefix)
    if df.empty:
        st.warning("Sin datos en el prefijo configurado (`HF_RAIN_ML_PREFIX`).")
        return
    with st.expander("Muestra de filas", expanded=False):
        st.dataframe(df.head(100), use_container_width=True, height=260)
    render_parquet_analysis(df, "kanishka")


def section_kaggle(bucket: str, prefix: str) -> None:
    st.caption("Parquet tipo Kaggle (accidentes). Prefijo: `KAGGLE_ML_PREFIX`.")
    if not prefix.strip():
        st.warning("Prefijo vacío: define `KAGGLE_ML_PREFIX` en el contenedor `dashboard`.")
        return
    df = cached_load_df(bucket, prefix)
    if df.empty:
        st.warning("Sin Parquet en esa ruta.")
        return
    with st.expander("Muestra de filas", expanded=False):
        st.dataframe(df.head(100), use_container_width=True, height=260)
    render_parquet_analysis(df, "kaggle")


def section_traffic_cameras(bucket: str, data_prefix: str) -> None:
    st.caption(
        "Origen: persistent → trusted (Pillow) → exploitation por sitio. Requiere `trusted_zone.py` y `exploitation_zone.py`."
    )
    gprefix = traffic_cameras_gallery_prefix(data_prefix)
    st.caption(f"`s3://{bucket}/{gprefix}` · `DATA_PREFIX={data_prefix!r}`")

    sites = list_traffic_camera_gallery_sites(bucket, data_prefix)
    if not sites:
        st.info(
            "Sin galería aún: comprueba prefijos, `EXPLOITATION_BUCKET` / `DATA_PREFIX` y datos en SeaweedFS."
        )
        return

    col_sel, col_img = st.columns([1, 2], gap="medium")
    with col_sel:
        site = st.selectbox("Ubicación (site_key)", sites, key="traffic_cam_site")
        items = list_traffic_camera_gallery_objects(bucket, data_prefix, site)
        if not items:
            st.warning("Carpeta sin imágenes.")
            return
        names = [n for _k, n in items]
        choice = st.selectbox("Captura", names, key="traffic_cam_file")
        key = next(k for k, n in items if n == choice)

    with col_img:
        try:
            body = get_s3_object_bytes(bucket, key)
        except Exception as ex:
            st.error(f"No se pudo leer `s3://{bucket}/{key}`: {ex}")
            return
        st.caption(f"`{key}`")
        try:
            st.image(body, caption=choice, use_container_width=True)
        except Exception:
            import io

            from PIL import Image

            st.image(Image.open(io.BytesIO(body)), caption=choice, use_container_width=True)


def section_explorer(bucket: str) -> None:
    st.caption("Cualquier prefijo Parquet dentro del bucket exploitation.")
    preset = st.selectbox(
        "Elegir dataset (prefijo)",
        (
            "— Personalizado (escribir abajo) —",
            "transit_posts/",
            "ajuntament_bcn/accidents_gu/",
            "ajuntament_bcn/ml_ready_accidents_traffic/",
            "aemet/",
            "sct/traffic_incidents/",
            "TOMTOM/",
        ),
        key="expl_preset",
    )
    if preset.startswith("—"):
        prefix = st.text_input(
            "Prefijo manual (sin s3:// ni bucket)",
            value="",
            placeholder="ej: mi_proyecto/reportes/",
            key="expl_prefix",
        )
    else:
        prefix = preset
        st.caption(f"Prefijo activo: `{prefix}`")
    if st.button("Cargar y analizar", key="expl_load"):
        df = cached_load_df(bucket, prefix)
        if df.empty:
            st.warning("Sin tablas Parquet legibles en esa ruta.")
        else:
            st.write(f"**{len(df):,}** filas.")
            with st.expander("Muestra", expanded=False):
                st.dataframe(df.head(200), use_container_width=True, height=280)
            render_parquet_analysis(df, "explorer")


def section_realtime_parking() -> None:
    """Live parking-lot counter served by the Kafka → Redis hot path."""
    start_parking_consumer()

    @st.fragment(run_every="5s")
    def _live_widget() -> None:
        free, total, ts = read_parking_counters()
        occupied = max(0, total - free)
        cols = st.columns([1, 1, 1, 2])
        with cols[0]:
            st.metric("Plazas libres", f"{free:,}")
        with cols[1]:
            st.metric("Plazas ocupadas", f"{occupied:,}")
        with cols[2]:
            st.metric("Sensores activos", f"{total:,}")
        with cols[3]:
            if ts:
                age = max(0, int(time.time()) - ts)
                st.caption(
                    f"Actualizado hace **{age} s** · fuente: Kafka `parking-lot-events` → "
                    f"Redis `db=1` · refresco cada 5 s."
                )
            else:
                st.caption(
                    "Esperando el primer evento de Kafka… "
                    "(arranca `parking-sensor` si no ves datos en ~30 s)."
                )

    _live_widget()


def section_chroma() -> None:
    st.caption(f"Cliente HTTP → `{_env('CHROMA_HOST', 'chromadb')}:{_env('CHROMA_PORT', '8000')}`")
    try:
        client = chroma_client()
        cols = client.list_collections()
        st.write(f"**{len(cols)}** colecciones")
        for c in cols:
            st.markdown(f"**{c.name}** · `id={c.id}`")
            coll = client.get_collection(c.name)
            st.caption(f"Documentos: {coll.count()}")
    except Exception as ex:
        st.error(f"No se pudo conectar a Chroma: {ex}")


def main() -> None:
    st.set_page_config(
        page_title="BDM — Dashboard",
        layout="wide",
        page_icon="◆",
        initial_sidebar_state="collapsed",
    )
    _inject_dashboard_css()
    _hero()

    exploitation_bucket = _env("EXPLOITATION_BUCKET", "exploitation-zone")
    pfx_acc = _env("ACCIDENTS_GU_ML_PREFIX", "ajuntament_bcn/accidents_gu/")
    pfx_aemet = _env("AEMET_ML_PREFIX", "aemet/")
    pfx_hf = _env("HF_RAIN_ML_PREFIX", "huggingface/kanishka089/")
    pfx_kaggle = _env("KAGGLE_ML_PREFIX", "")
    pfx_transit = _env("TRANSIT_POSTS_PREFIX", "transit_posts/")
    pfx_trams_map = _env("TRAMS_MAP_PREFIX", "ajuntament_bcn/trams_map/")
    data_prefix = _env("DATA_PREFIX", "")

    st.markdown(
        f"""
        <div style="display:flex; flex-wrap:wrap; gap:0.5rem; margin-bottom:1.5rem;">
            <span style="background:rgba(13,148,136,0.25); color:#99f6e4; padding:0.35rem 0.75rem; border-radius:999px; font-size:0.8rem; border:1px solid rgba(45,212,191,0.35);">Bucket <strong>{exploitation_bucket}</strong></span>
            <span style="background:rgba(99,102,241,0.2); color:#c7d2fe; padding:0.35rem 0.75rem; border-radius:999px; font-size:0.8rem; border:1px solid rgba(129,140,248,0.35);">DATA_PREFIX <strong>{data_prefix or "∅"}</strong></span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    pfx_tomtom = _env("TOMTOM_ML_PREFIX", "TOMTOM/")
    pfx_bcn_ml = _env("BCN_ML_READY_PREFIX", "ajuntament_bcn/ml_ready_accidents_traffic/")
    pfx_sct = _env("SCT_TRAFFIC_INCIDENTS_PREFIX", "sct/traffic_incidents/")

    _section_title(
        "Plazas de parking en tiempo real (hot path)",
        "Contador en vivo desde Kafka, agregado en Redis (db=1) y refrescado cada 5 s. "
        "No depende de Spark ni de Parquet: el consumidor corre dentro del propio "
        "contenedor `dashboard` como hilo en background.",
    )
    with st.container(border=True):
        section_realtime_parking()

    _section_title(
        "Mapa de congestió de trams",
        "Cada tram de Barcelona pintado como una línea coloreada por su último nivel de "
        "congestió (verd fluid → vermell tallat). Geometría + snapshot materializados en "
        "`exploitation-zone` (`trams_map`); aquí solo se renderiza con pydeck.",
    )
    with st.container(border=True):
        render_trams_map(cached_load_trams_map(exploitation_bucket, pfx_trams_map))

    _section_title(
        "Panel principal (datos ya servidos desde el bucket)",
        "TomTom, BCN (dataset ML-ready), cámaras, Kanishka, AEMET por hora y noticias SCT+Twitter.",
    )
    render_unified_dashboard(
        exploitation_bucket,
        data_prefix,
        tomtom_prefix=pfx_tomtom,
        bcn_ml_ready_prefix=pfx_bcn_ml,
        aemet_prefix=pfx_aemet,
        hf_prefix=pfx_hf,
        twitter_prefix=pfx_transit,
        sct_prefix=pfx_sct,
    )

    with st.expander("Tweets de tránsito — timeline y análisis Parquet", expanded=False):
        section_transit_twitter(exploitation_bucket, pfx_transit)

    _section_title(
        "Datasets tabulares (detalle)",
        "Histogramas y frecuencias para accidentes, Kaggle, AEMET y Kanishka (sin duplicar la vista unificada de arriba).",
    )
    r1a, r1b = st.columns(2, gap="large")
    with r1a:
        with st.container(border=True):
            st.subheader("Accidents GU")
            section_accidents_gu(exploitation_bucket, pfx_acc)
    with r1b:
        with st.container(border=True):
            st.subheader("Kaggle")
            section_kaggle(exploitation_bucket, pfx_kaggle)

    r2a, r2b = st.columns(2, gap="large")
    with r2a:
        with st.container(border=True):
            st.subheader("AEMET")
            section_aemet(exploitation_bucket, pfx_aemet)
    with r2b:
        with st.container(border=True):
            st.subheader("Kanishka · lluvia")
            section_kanishka(exploitation_bucket, pfx_hf)

    _section_title("Herramientas", "Explorador de Parquet por prefijo y estado de ChromaDB.")
    with st.container(border=True):
        st.subheader("Explorador Parquet")
        section_explorer(exploitation_bucket)

    with st.container(border=True):
        st.subheader("ChromaDB")
        section_chroma()

    st.markdown(
        """
        <div style="text-align:center; margin-top:2.5rem; padding-top:1.5rem; border-top:1px solid rgba(148,163,184,0.15); color:#64748b; font-size:0.85rem;">
            BDM · capa de presentación · explotación unificada + análisis exploratorio
        </div>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
