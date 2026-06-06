"""
Dashboard (capa presentació): mapa de congestió de trams + places de parking en temps real.
"""

from __future__ import annotations

import os

import streamlit as st

from data_access import load_trams_map
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
        div[data-testid="stExpander"] {
            background: linear-gradient(145deg, rgba(15, 23, 42, 0.65), rgba(30, 41, 59, 0.35));
            border: 1px solid rgba(94, 234, 212, 0.14);
            border-radius: 16px;
        }
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
            <p style="margin:0 0 0.35rem 0; font-size:0.75rem; text-transform:uppercase; letter-spacing:0.22em; color:#5eead4; font-weight:600;">BDM · dades en explotació</p>
            <h1 style="margin:0; font-size:clamp(1.85rem, 4.2vw, 2.5rem); font-weight:700; color:#f8fafc; line-height:1.12; text-shadow:0 0 40px rgba(45,212,191,0.25);">Panell de mobilitat urbana</h1>
            <p style="margin:0.85rem 0 0 0; font-size:1.05rem; color:#94a3b8; max-width:52rem; line-height:1.6;">
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


@st.cache_data(ttl=300, show_spinner="Carregant mapa de trams des de S3…")
def cached_load_trams_map(bucket: str, prefix: str):
    try:
        return load_trams_map(bucket, prefix)
    except Exception as e:
        st.error(f"Error llegint trams_map de S3: {e}")
        return None


def section_realtime_parking() -> None:
    start_parking_consumer()

    @st.fragment(run_every="5s")
    def _live_widget() -> None:
        free, total, _ = read_parking_counters()
        occupied = max(0, total - free)
        cols = st.columns(3)
        with cols[0]:
            st.metric("Places lliures", f"{free:,}")
        with cols[1]:
            st.metric("Places ocupades", f"{occupied:,}")
        with cols[2]:
            st.metric("Sensors actius", f"{total:,}")

    _live_widget()


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
    pfx_trams_map = _env("TRAMS_MAP_PREFIX", "ajuntament_bcn/trams_map/")

    _section_title(
        "Places de pàrking en temps real",
        "Comptador en viu des de Kafka, agregat a Redis (db=1) i refrescat cada 5 s.",
    )
    with st.container(border=True):
        section_realtime_parking()

    _section_title(
        "Mapa de congestió de trams",
        "Cada tram de Barcelona pintat com una línia colorejada pel seu últim nivell de congestió "
        "(verd fluid → vermell tallat). Geometria + snapshot materialitzats a exploitation-zone.",
    )
    with st.container(border=True):
        render_trams_map(cached_load_trams_map(exploitation_bucket, pfx_trams_map))

    st.markdown(
        """
        <div style="text-align:center; margin-top:2.5rem; padding-top:1.5rem; border-top:1px solid rgba(148,163,184,0.15); color:#64748b; font-size:0.85rem;">
            BDM · capa de presentació
        </div>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
