"""
Dashboard capa presentación: una sola página con datos en exploitation-zone (S3),
análisis exploratorio (estadísticos y gráficos), vista tipo timeline para tweets y ChromaDB.

Uso (Docker): http://localhost:8501 tras `docker compose up dashboard`.
"""

from __future__ import annotations

import os

import chromadb
import pandas as pd
import streamlit as st

from analytics import render_parquet_analysis, render_tweet_feed
from data_access import (
    get_s3_object_bytes,
    list_traffic_camera_gallery_objects,
    list_traffic_camera_gallery_sites,
    load_parquet_prefix,
    traffic_cameras_gallery_prefix,
)


def _env(name: str, default: str) -> str:
    v = os.environ.get(name)
    return v if v is not None and v != "" else default


def _inject_dashboard_css() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
        html, body, [class*="stApp"] {
            font-family: 'Outfit', system-ui, sans-serif;
        }
        code, pre, .stMarkdown code {
            font-family: 'JetBrains Mono', monospace !important;
        }
        .stApp {
            background: linear-gradient(165deg, #0c1222 0%, #121a2e 35%, #0f172a 70%, #0c4a6e 100%);
            background-attachment: fixed;
        }
        [data-testid="stAppViewContainer"] > .main {
            background: transparent;
        }
        .block-container {
            padding-top: 1.25rem;
            padding-bottom: 3rem;
            max-width: 1280px;
        }
        [data-testid="stHeader"] { background: rgba(15, 23, 42, 0.85); backdrop-filter: blur(8px); }
        h1, h2, h3 { color: #f1f5f9 !important; letter-spacing: -0.02em; }
        .stMarkdown p, .stMarkdown li, label, span[data-testid="stMarkdownContainer"] p {
            color: #cbd5e1;
        }
        [data-baseweb="select"] > div, [data-baseweb="input"] > div {
            background-color: rgba(30, 41, 59, 0.85) !important;
            border-color: rgba(148, 163, 184, 0.25) !important;
        }
        .stTextInput input, .stNumberInput input {
            background-color: rgba(30, 41, 59, 0.9) !important;
            color: #e2e8f0 !important;
            border: 1px solid rgba(94, 234, 212, 0.2) !important;
            border-radius: 10px !important;
        }
        div[data-testid="stExpander"] {
            background: rgba(15, 23, 42, 0.5);
            border: 1px solid rgba(94, 234, 212, 0.15);
            border-radius: 12px;
        }
        .stDataFrame { border-radius: 12px; overflow: hidden; }
        [data-testid="stVerticalBlockBorderWrapper"] {
            background: rgba(15, 23, 42, 0.45) !important;
            border-radius: 16px !important;
            border-color: rgba(94, 234, 212, 0.18) !important;
            padding: 0.35rem 0.5rem 0.85rem;
        }
        div[data-testid="stAlert"] {
            background-color: rgba(30, 41, 59, 0.92) !important;
            border: 1px solid rgba(148, 163, 184, 0.2);
        }
        .stButton > button {
            background: linear-gradient(135deg, #0d9488 0%, #0891b2 100%);
            color: white;
            border: none;
            border-radius: 10px;
            font-weight: 600;
            padding: 0.5rem 1.25rem;
            box-shadow: 0 4px 14px rgba(13, 148, 136, 0.35);
        }
        .stButton > button:hover {
            box-shadow: 0 6px 20px rgba(34, 211, 238, 0.4);
            border: 1px solid rgba(165, 243, 252, 0.5);
        }
        .tweet-card-wrap { margin-bottom: 0.15rem; }
        .tweet-card {
            display: flex;
            gap: 0.9rem;
            padding: 1rem 1.15rem;
            background: rgba(15, 23, 42, 0.72);
            border: 1px solid rgba(148, 163, 184, 0.14);
            border-radius: 16px;
            margin-bottom: 0.6rem;
            box-shadow: 0 4px 18px rgba(0,0,0,0.2);
        }
        .tweet-avatar {
            width: 46px; height: 46px; flex-shrink: 0;
            border-radius: 999px;
            background: linear-gradient(145deg, #14b8a6, #6366f1);
            box-shadow: 0 0 0 2px rgba(94, 234, 212, 0.25);
        }
        .tweet-body { flex: 1; min-width: 0; }
        .tweet-meta {
            display: flex; align-items: center; flex-wrap: wrap; gap: 0.35rem;
            margin-bottom: 0.4rem;
        }
        .tweet-author { font-weight: 700; color: #f8fafc !important; font-size: 0.95rem; }
        .tweet-date { color: #94a3b8 !important; font-size: 0.82rem; }
        .tweet-dot { color: #475569; }
        .tweet-text {
            color: #e2e8f0 !important; font-size: 0.96rem; line-height: 1.5;
            word-break: break-word;
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
            <p style="margin:0 0 0.35rem 0; font-size:0.8rem; text-transform:uppercase; letter-spacing:0.2em; color:#5eead4; font-weight:600;">Exploitation · análisis · visión</p>
            <h1 style="margin:0; font-size:clamp(1.75rem, 4vw, 2.35rem); font-weight:700; color:#f8fafc; line-height:1.15;">BDM — panel único</h1>
            <p style="margin:0.75rem 0 0 0; font-size:1.05rem; color:#94a3b8; max-width:52rem; line-height:1.55;">
                <strong style="color:#e2e8f0;">Sin predicciones ML</strong> en pantalla: solo <strong style="color:#e2e8f0;">estadísticos</strong>,
                histogramas, barras de frecuencia y una <strong style="color:#e2e8f0;">vista tipo timeline</strong> para los tweets
                de tránsito, más cámaras, similaridad y Chroma.
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


def section_similarity(bucket: str, default_sim: str) -> None:
    st.caption("Salida de `twitter_similarity.py` (`similarity_edges.parquet`).")
    thr = st.slider("Umbral mínimo", 0.5, 1.0, 0.8, 0.05, key="sim_thr")
    df_sim = cached_load_df(bucket, default_sim)
    if df_sim.empty:
        st.info(
            f"Sin datos en `s3://{bucket}/{default_sim}/`. Ejecuta embeddings + script de similaridad."
        )
    else:
        if "similarity" in df_sim.columns:
            df_sim = df_sim[df_sim["similarity"] >= thr]
        st.dataframe(df_sim, use_container_width=True, height=320)
        with st.expander("Análisis de la tabla de pares", expanded=False):
            render_parquet_analysis(df_sim, "sim_edges")


def section_explorer(bucket: str) -> None:
    st.caption("Cualquier prefijo Parquet dentro del bucket exploitation.")
    prefix = st.text_input(
        "Prefijo (sin s3:// ni bucket)",
        value="",
        placeholder="ej: mi_proyecto/reportes/",
        key="expl_prefix",
    )
    if st.button("Cargar y analizar", key="expl_load"):
        df = cached_load_df(bucket, prefix)
        if df.empty:
            st.warning("Sin tablas Parquet legibles en esa ruta.")
        else:
            st.write(f"**{len(df):,}** filas.")
            with st.expander("Muestra", expanded=False):
                st.dataframe(df.head(200), use_container_width=True, height=280)
            render_parquet_analysis(df, "explorer")


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
    default_sim = _env("SIMILARITY_OUTPUT_PREFIX", "twitter_similarity_pairs")
    pfx_acc = _env("ACCIDENTS_GU_ML_PREFIX", "ajuntament_bcn/accidents_gu/")
    pfx_aemet = _env("AEMET_ML_PREFIX", "aemet/")
    pfx_hf = _env("HF_RAIN_ML_PREFIX", "huggingface/kanishka089/")
    pfx_kaggle = _env("KAGGLE_ML_PREFIX", "")
    pfx_transit = _env("TRANSIT_POSTS_PREFIX", "transit_posts/")
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

    _section_title("Tweets de tránsito (RSS / Nitter)", "Vista tipo timeline; debajo, estadísticos del mismo Parquet.")
    with st.container(border=True):
        section_transit_twitter(exploitation_bucket, pfx_transit)

    _section_title(
        "Datasets tabulares (accidentes, lluvia, AEMET…)",
        "Medias, medianas, mín/máx, histogramas y barras de frecuencia. Sin formularios de predicción.",
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

    _section_title("Cámaras de tráfico", "Galería por ubicación desde exploitation-zone.")
    with st.container(border=True):
        section_traffic_cameras(exploitation_bucket, data_prefix)

    _section_title("Similaridad, explorador y Chroma", "Pares similares, cualquier prefijo Parquet y colecciones vectoriales.")
    r3a, r3b = st.columns(2, gap="large")
    with r3a:
        with st.container(border=True):
            st.subheader("Similaridad Twitter")
            section_similarity(exploitation_bucket, default_sim)
    with r3b:
        with st.container(border=True):
            st.subheader("Explorador Parquet")
            section_explorer(exploitation_bucket)

    with st.container(border=True):
        st.subheader("ChromaDB")
        section_chroma()

    st.markdown(
        """
        <div style="text-align:center; margin-top:2.5rem; padding-top:1.5rem; border-top:1px solid rgba(148,163,184,0.15); color:#64748b; font-size:0.85rem;">
            BDM · capa de presentación · análisis exploratorio (sin modelos predictivos en UI)
        </div>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
