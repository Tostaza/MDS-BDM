"""
Widget de mapa (tipo Google Maps de tráfico) para la capa de presentación.

Pinta cada **tram** de Barcelona como una polilínea sobre un mapa oscuro, coloreada
según su último nivel de congestió (`estatActual`, 0–6). Usa **pydeck / deck.gl**, que
viene incluido con Streamlit (sin dependencias nuevas ni API key: basemap CARTO oscuro
que encaja con el tema del dashboard).

La tabla de entrada (`ajuntament_bcn/trams_map/`) la materializa ``exploitation-zone``
(`materialize_trams_map`): una fila por tram con la geometría completa (`path`) y el
snapshot de congestió más reciente. Aquí NO hay joins ni ETL, solo render.
"""

from __future__ import annotations

import pandas as pd
import pydeck as pdk
import streamlit as st

# Escala oficial del Ajuntament para `estatActual` → color RGB (verde fluido → rojo tallat).
# 0 se trata como «sense dades» (gris) para no fingir información que no tenemos.
CONGESTION_COLORS: dict[int, list[int]] = {
    0: [120, 120, 120],  # sense dades
    1: [26, 188, 94],    # molt fluid
    2: [120, 200, 60],   # fluid
    3: [245, 200, 40],   # dens
    4: [240, 140, 30],   # molt dens
    5: [230, 60, 45],     # congestió
    6: [150, 20, 20],    # tallat
}
CONGESTION_LABELS: dict[int, str] = {
    0: "Sense dades",
    1: "Molt fluid",
    2: "Fluid",
    3: "Dens",
    4: "Molt dens",
    5: "Congestió",
    6: "Tallat",
}

# Centro aproximado de Barcelona para el encuadre inicial.
_BCN_LAT, _BCN_LON = 41.3900, 2.1700


def _coerce_level(value) -> int:
    """Normaliza `estatActual` a un entero 0–6; cualquier cosa rara → 0 (sense dades)."""
    try:
        lvl = int(round(float(value)))
    except (TypeError, ValueError):
        return 0
    return lvl if lvl in CONGESTION_COLORS else 0


def prepare_map_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deja el DataFrame listo para `PathLayer`: filas con `path` válido (≥2 puntos),
    columna `color` (RGB) y etiquetas legibles para el tooltip.

    Filtra trams sin geometría utilizable porque deck.gl necesita al menos dos vértices
    para trazar una línea (un único punto no se vería).
    """
    if df is None or df.empty or "path" not in df.columns:
        return pd.DataFrame()

    out = df.copy()
    # `path` puede llegar como ndarray/lista anidada (Parquet → pandas); normaliza a lista.
    out["path"] = out["path"].apply(
        lambda p: [list(pt) for pt in p] if p is not None and len(p) >= 2 else None
    )
    out = out[out["path"].notna()].copy()
    if out.empty:
        return out

    level_col = "estatActual" if "estatActual" in out.columns else None
    out["nivell"] = out[level_col].apply(_coerce_level) if level_col else 0
    out["color"] = out["nivell"].map(CONGESTION_COLORS)
    out["nivell_text"] = out["nivell"].map(CONGESTION_LABELS)
    if "descripcio" not in out.columns:
        out["descripcio"] = None
    out["descripcio"] = out["descripcio"].fillna("(sense nom)")
    id_col = "id_tram" if "id_tram" in out.columns else out.columns[0]
    out["tram_id_text"] = out[id_col].astype(str)
    return out


def _legend_html() -> str:
    """Leyenda de colores reutilizando el look de `pill-badge` del CSS del dashboard."""
    items = "".join(
        f'<span style="display:inline-flex;align-items:center;gap:0.4rem;margin:0 0.6rem 0.4rem 0;">'
        f'<span style="width:14px;height:14px;border-radius:3px;background:rgb({r},{g},{b});'
        f'box-shadow:0 0 0 1px rgba(255,255,255,0.15);"></span>'
        f'<span style="color:#cbd5e1;font-size:0.8rem;">{CONGESTION_LABELS[lvl]}</span></span>'
        for lvl, (r, g, b) in CONGESTION_COLORS.items()
    )
    return f'<div style="display:flex;flex-wrap:wrap;margin-top:0.6rem;">{items}</div>'


def render_trams_map(df: pd.DataFrame) -> None:
    """
    Renderiza el mapa de congestió de trams.

    `df` es lo que devuelve ``data_access.load_trams_map`` (una fila por tram). Si está
    vacío, muestra una pista para ejecutar el pipeline en lugar de un mapa en blanco.
    """
    st.caption(
        "Origen: `s3://exploitation-zone/ajuntament_bcn/trams_map/` · una polilínea por "
        "tram coloreada por su **último** `estatActual`. Job: `exploitation-zone` "
        "(`materialize_trams_map`)."
    )

    data = prepare_map_dataframe(df)
    if data.empty:
        st.info(
            "Aún no hay datos de mapa de trams. Ejecuta ingestión (`relacio_trams` + "
            "`trams_congestion`) → trusted → `exploitation_zone.py`, que escribe "
            "`ajuntament_bcn/trams_map/`."
        )
        return

    # Métricas rápidas del snapshot (congestió real = niveles 5 y 6).
    total = len(data)
    en_congestio = int((data["nivell"] >= 5).sum())
    tallats = int((data["nivell"] == 6).sum())
    c1, c2, c3 = st.columns(3)
    c1.metric("Trams al mapa", f"{total:,}")
    c2.metric("En congestió (≥5)", f"{en_congestio:,}")
    c3.metric("Tallats", f"{tallats:,}")

    layer = pdk.Layer(
        "PathLayer",
        data=data,
        get_path="path",
        get_color="color",
        width_min_pixels=3,
        width_scale=1,
        cap_rounded=True,
        joint_rounded=True,
        pickable=True,
        auto_highlight=True,
    )
    view_state = pdk.ViewState(latitude=_BCN_LAT, longitude=_BCN_LON, zoom=12, pitch=0)
    tooltip = {
        "html": (
            "<b>Tram {tram_id_text}</b><br/>"
            "{descripcio}<br/>"
            "Estat: <b>{nivell_text}</b> ({nivell})"
        ),
        "style": {"backgroundColor": "#0f172a", "color": "#e2e8f0", "fontSize": "0.8rem"},
    }
    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        map_style="dark",
        tooltip=tooltip,
    )
    st.pydeck_chart(deck, use_container_width=True)
    st.markdown(_legend_html(), unsafe_allow_html=True)
