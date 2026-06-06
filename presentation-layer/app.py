"""
Dashboard (capa presentació): mapa de congestió de trams + places de parking en temps real.
"""

from __future__ import annotations

import os

import streamlit as st

from analytics import render_parquet_analysis, render_tweet_feed
from dashboard_views import render_aemet_current_forecast, render_unified_dashboard
import altair as alt
import pandas as pd

from data_access import (
    get_s3_object_bytes,
    list_traffic_camera_gallery_objects,
    list_traffic_camera_gallery_sites,
    load_parquet_prefix,
    load_trams_map,
)
from hot_path import read_parking_counters, start_parking_consumer
from warm_path_trams import (
    get_iot_snapshot,
    get_trams_snapshot,
    start_iot_consumer,
    start_trams_consumer,
)
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
        [data-testid="stSlider"] p,
        [data-testid="stSlider"] span,
        [data-testid="stSlider"] div[data-testid="stTickBarMin"],
        [data-testid="stSlider"] div[data-testid="stTickBarMax"] {
            color: #f1f5f9 !important;
        }
        /* Selectbox: valor seleccionat i opcions del desplegable */
        [data-baseweb="select"] *:not(svg):not(path) { color: #f1f5f9 !important; }
        [data-baseweb="menu"]   *:not(svg):not(path) { color: #f1f5f9 !important; }
        [data-baseweb="menu"] { background-color: #0f172a !important; }
        [role="option"]:hover { background-color: rgba(94,234,212,0.12) !important; }
        /* Mètriques st.metric */
        [data-testid="stMetricValue"]  > div { color: #f1f5f9 !important; }
        [data-testid="stMetricLabel"]  > div { color: #94a3b8 !important; }
        [data-testid="stMetricDelta"]  > div { color: #5eead4 !important; }
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


def _fmt_camera_site(site_key: str) -> str:
    import re
    s = site_key.strip()
    s = re.sub(r"_-_", " — ", s)
    s = s.replace("_", " ")
    s = re.sub(r"\b([a-z]{1,3})-(\d+)\b", lambda m: f"{m.group(1).upper()}-{m.group(2)}", s)
    return s.strip().title()


def section_traffic_cameras(bucket: str, data_prefix: str) -> None:
    sites = list_traffic_camera_gallery_sites(bucket, data_prefix)
    if not sites:
        st.info("Sense imatges de càmeres disponibles.")
        return

    labels = {s: _fmt_camera_site(s) for s in sites}

    selected = st.selectbox(
        "Càmera",
        options=sites,
        format_func=lambda s: labels[s],
        label_visibility="collapsed",
    )

    items = list_traffic_camera_gallery_objects(bucket, data_prefix, selected)
    if not items:
        st.warning("Sense imatges per aquesta càmera.")
        return

    if len(items) > 1:
        names = [n for _, n in items]
        choice_name = st.select_slider("Captura", options=names, label_visibility="collapsed")
        key = next(k for k, n in items if n == choice_name)
    else:
        key, _ = items[0]

    try:
        body = get_s3_object_bytes(bucket, key)
        st.image(body, use_container_width=True)
    except Exception as ex:
        st.error(f"No s'ha pogut carregar la imatge: {ex}")


@st.cache_data(ttl=600, show_spinner="Entrenant model AEMET…")
def _aemet_model(bucket: str):
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import accuracy_score, confusion_matrix, f1_score, precision_score, recall_score
    from sklearn.preprocessing import StandardScaler

    df_train = load_parquet_prefix(bucket, "aemet/ml_rain_train/")
    df_test = load_parquet_prefix(bucket, "aemet/ml_rain_test/")
    if df_train.empty or df_test.empty:
        return None

    target = "ha_plogut"
    drop_cols = [target, "nombre_municipio", "provincia", "orto", "ocaso"]

    def prep(df):
        X = df.drop(columns=[c for c in drop_cols if c in df.columns])
        return X.select_dtypes(include="number"), df[target] if target in df.columns else None

    X_train, y_train = prep(df_train)
    X_test, y_test = prep(df_test)
    if y_train is None or y_test is None or X_train.empty:
        return None

    common = [c for c in X_train.columns if c in X_test.columns]
    X_train, X_test = X_train[common].fillna(0), X_test[common].fillna(0)

    if y_train.nunique() < 2:
        return {"error": f"El dataset de train té una sola classe. Torna a executar el pipeline d'exploitation per regenerar les dades amb mostres sintètiques.", "n_train": len(X_train), "n_test": len(X_test)}

    scaler = StandardScaler()
    model = LogisticRegression(max_iter=500, random_state=42)
    model.fit(scaler.fit_transform(X_train), y_train)
    y_pred = model.predict(scaler.transform(X_test))

    return {
        "accuracy":  accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred, zero_division=0),
        "recall":    recall_score(y_test, y_pred, zero_division=0),
        "f1":        f1_score(y_test, y_pred, zero_division=0),
        "cm":        confusion_matrix(y_test, y_pred).tolist(),
        "n_train":   len(X_train),
        "n_test":    len(X_test),
        "model":     model,
        "scaler":    scaler,
        "features":  common,
    }


def _render_ml_panel(bundle: dict, labels: list | None = None) -> None:
    acc, pre, rec, f1 = bundle["accuracy"], bundle["precision"], bundle["recall"], bundle["f1"]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Accuracy",  f"{acc:.3f}")
    c2.metric("Precision", f"{pre:.3f}")
    c3.metric("Recall",    f"{rec:.3f}")
    c4.metric("F1",        f"{f1:.3f}")

    cm_raw = bundle["cm"]
    labels = labels if labels is not None else ["No plou", "Plou"]
    n = len(cm_raw)
    rows = [
        {"Real": labels[i] if i < 2 else str(i), "Predicció": labels[j] if j < 2 else str(j), "n": cm_raw[i][j]}
        for i in range(n) for j in range(len(cm_raw[i]))
    ]
    df_cm = pd.DataFrame(rows)

    base = alt.Chart(df_cm).encode(
        x=alt.X("Predicció:N", title="Predicció"),
        y=alt.Y("Real:N", title="Real"),
    )
    heatmap = base.mark_rect().encode(
        color=alt.Color("n:Q", scale=alt.Scale(scheme="tealblues"), legend=None)
    )
    text = base.mark_text(color="white", fontSize=20, fontWeight="bold").encode(text="n:Q")
    chart = (heatmap + text).properties(
        height=200, background="#0f172a", title="Matriu de confusió"
    ).configure_axis(
        labelColor="#94a3b8", titleColor="#cbd5e1"
    ).configure_title(color="#f1f5f9")
    st.altair_chart(chart, use_container_width=True)
    st.caption(f"Train: {bundle['n_train']:,} mostres · Test: {bundle['n_test']:,} mostres · Logistic Regression")


def _render_climate_chart(bucket: str) -> None:
    df = load_parquet_prefix(bucket, "aemet/climate_by_municipio/")
    if df.empty:
        st.info("Sense dades de climate_by_municipio.")
        return

    municipis = sorted(df["nombre_municipio"].dropna().unique().tolist())
    selected = st.selectbox("Municipi", municipis, key="aemet_municipio")

    sub = df[df["nombre_municipio"] == selected].copy()
    sub["data"] = pd.to_datetime(sub["dia_fecha_parsed"], errors="coerce")
    sub = sub.sort_values("data")

    metric_map = {
        "avg_temperatura":      "Temperatura (°C)",
        "avg_sens_termica":     "Sensació tèrmica (°C)",
        "avg_humedad_relativa": "Humitat relativa (%)",
    }
    cols_ok = [c for c in metric_map if c in sub.columns]
    melt = sub[["data"] + cols_ok].melt(id_vars="data", var_name="clau", value_name="valor")
    melt["Mètrica"] = melt["clau"].map(metric_map)
    melt["valor"] = pd.to_numeric(melt["valor"], errors="coerce")
    melt = melt.dropna(subset=["valor"])

    colors = ["#2dd4bf", "#818cf8", "#fb923c"][: len(cols_ok)]
    dashes = [[1, 0], [6, 3], [4, 2]][: len(cols_ok)]
    domain = [metric_map[c] for c in cols_ok]

    chart = (
        alt.Chart(melt)
        .mark_line(point=True, strokeWidth=2.5)
        .encode(
            x=alt.X("data:T", title="Data"),
            y=alt.Y("valor:Q", title="Valor"),
            color=alt.Color(
                "Mètrica:N",
                scale=alt.Scale(domain=domain, range=colors),
                legend=alt.Legend(
                    labelColor="#cbd5e1",
                    titleColor="#e2e8f0",
                    labelFontSize=12,
                    titleFontSize=12,
                ),
            ),
            strokeDash=alt.StrokeDash(
                "Mètrica:N",
                scale=alt.Scale(domain=domain, range=dashes),
                legend=None,
            ),
            tooltip=[alt.Tooltip("data:T", title="Data"), "Mètrica:N", alt.Tooltip("valor:Q", format=".1f")],
        )
        .properties(height=300, background="#0f172a", title=f"Evolució climàtica — {selected}")
        .configure_axis(labelColor="#94a3b8", titleColor="#cbd5e1", gridColor="#1e293b")
        .configure_title(color="#f1f5f9")
    )
    st.altair_chart(chart, use_container_width=True)


def _render_rain_predictor(bundle: dict) -> None:
    st.markdown("##### Predictor de pluja")
    model = bundle.get("model")
    scaler = bundle.get("scaler")
    features = bundle.get("features", [])
    if not model or not features:
        st.info("Model no disponible.")
        return

    label_map = {
        "mes": ("Mes (1–12)", 1, 12, 6),
        "dia": ("Dia (1–31)", 1, 31, 15),
        "hora_periodo": ("Hora (0–23)", 0, 23, 12),
        "temperatura": ("Temperatura (°C)", -5, 45, 20),
        "sens_termica": ("Sensació tèrmica (°C)", -10, 45, 18),
        "humedad_relativa": ("Humitat relativa (%)", 0, 100, 65),
    }

    vals = {}
    ordered = [f for f in label_map if f in features]
    cols = st.columns(3)
    for i, feat in enumerate(ordered):
        label, mn, mx, default = label_map[feat]
        vals[feat] = cols[i % 3].slider(label, min_value=mn, max_value=mx, value=default, key=f"pred_{feat}")

    if st.button("Predir", key="rain_predict_btn", use_container_width=True):
        import numpy as np
        row = np.array([[vals.get(f, 0) for f in features]])
        row_scaled = scaler.transform(row)
        pred = model.predict(row_scaled)[0]
        proba = model.predict_proba(row_scaled)[0]
        prob_plou = proba[1] if len(proba) > 1 else proba[0]
        if pred == 1:
            st.markdown(
                f"<div style='text-align:center;padding:1rem;border-radius:12px;"
                f"background:rgba(99,102,241,0.2);border:1px solid #818cf8;'>"
                f"<span style='font-size:2rem;'>🌧️</span><br>"
                f"<span style='font-size:1.3rem;font-weight:700;color:#a5b4fc;'>PLOURÀ</span><br>"
                f"<span style='color:#94a3b8;font-size:0.9rem;'>Probabilitat: {prob_plou:.1%}</span></div>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f"<div style='text-align:center;padding:1rem;border-radius:12px;"
                f"background:rgba(13,148,136,0.2);border:1px solid #2dd4bf;'>"
                f"<span style='font-size:2rem;'>☀️</span><br>"
                f"<span style='font-size:1.3rem;font-weight:700;color:#5eead4;'>NO PLOURÀ</span><br>"
                f"<span style='color:#94a3b8;font-size:0.9rem;'>Probabilitat de pluja: {prob_plou:.1%}</span></div>",
                unsafe_allow_html=True,
            )


def section_aemet(bucket: str) -> None:
    bundle = _aemet_model(bucket)

    col_ml, col_chart = st.columns(2, gap="large")
    with col_ml:
        st.markdown("##### Model de predicció de pluja (Logistic Regression)")
        if bundle and "error" in bundle:
            st.warning(bundle["error"])
            st.caption(f"Train: {bundle['n_train']:,} mostres · Test: {bundle['n_test']:,} mostres")
        elif bundle:
            _render_ml_panel(bundle)
        else:
            st.info("Sense dades de train/test per entrenar el model.")
    with col_chart:
        st.markdown("##### Evolució climàtica per municipi")
        _render_climate_chart(bucket)

    if bundle and "error" not in bundle:
        st.markdown("<div style='margin-top:1.25rem;border-top:1px solid rgba(94,234,212,0.15);padding-top:0.5rem;'></div>", unsafe_allow_html=True)
        _render_rain_predictor(bundle)


@st.cache_data(ttl=600, show_spinner="Entrenant model accidents GU…")
def _accidents_model(bucket: str):
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import accuracy_score, confusion_matrix, f1_score, precision_score, recall_score
    from sklearn.preprocessing import StandardScaler

    df_train = load_parquet_prefix(bucket, "ajuntament_bcn/accidents_gu_ml_train/")
    df_test  = load_parquet_prefix(bucket, "ajuntament_bcn/accidents_gu_ml_test/")
    if df_train.empty or df_test.empty:
        return None

    target    = "accident_greu"
    drop_cols = [target, "Nom_carrer"]

    def prep(df):
        X = df.drop(columns=[c for c in drop_cols if c in df.columns])
        return X.select_dtypes(include="number"), df[target] if target in df.columns else None

    X_train, y_train = prep(df_train)
    X_test,  y_test  = prep(df_test)
    if y_train is None or y_test is None or X_train.empty:
        return None
    if y_train.nunique() < 2:
        return {"error": "El dataset de train té una sola classe.", "n_train": len(X_train), "n_test": len(X_test)}

    common  = [c for c in X_train.columns if c in X_test.columns]
    X_train = X_train[common].fillna(0)
    X_test  = X_test[common].fillna(0)

    scaler = StandardScaler()
    # class_weight='balanced' compensa el desequilibri de classes (molts menys accidents greus)
    model  = LogisticRegression(max_iter=1000, random_state=42, class_weight="balanced")
    model.fit(scaler.fit_transform(X_train), y_train)
    y_pred = model.predict(scaler.transform(X_test))

    # Distribució de classes al train per mostrar-la al panel
    class_counts = y_train.value_counts().to_dict()

    return {
        "accuracy":     accuracy_score(y_test, y_pred),
        "precision":    precision_score(y_test, y_pred, zero_division=0),
        "recall":       recall_score(y_test, y_pred, zero_division=0),
        "f1":           f1_score(y_test, y_pred, zero_division=0),
        "cm":           confusion_matrix(y_test, y_pred).tolist(),
        "n_train":      len(X_train),
        "n_test":       len(X_test),
        "class_counts": class_counts,
        "model":        model,
        "scaler":       scaler,
        "features":     common,
    }


def _fix_enc(s: str) -> str:
    try:
        return s.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return s


def _render_accidents_chart(bucket: str) -> None:
    df = load_parquet_prefix(bucket, "ajuntament_bcn/accidents_gu_by_district/")
    if df.empty:
        st.info("Sense dades d'accidents per districte.")
        return

    df["Nom_districte"] = df["Nom_districte"].dropna().apply(_fix_enc)
    df["Nom_barri"]     = df["Nom_barri"].dropna().apply(_fix_enc)

    metric_cols = [c for c in ["total_morts", "total_lesionats_greus", "total_lesionats_lleus",
                                "total_victimes", "total_vehicles_implicats", "total_accidents"]
                   if c in df.columns]

    districtes = sorted(df["Nom_districte"].dropna().unique().tolist())
    sel_dist   = st.selectbox("Districte", districtes, key="acc_dist")
    df_dist    = df[df["Nom_districte"] == sel_dist]
    barris     = sorted(df_dist["Nom_barri"].dropna().unique().tolist())
    sel_barri  = st.selectbox("Barri", barris, key="acc_barri")

    row = df_dist[df_dist["Nom_barri"] == sel_barri]
    if row.empty or not metric_cols:
        st.warning("Sense dades per aquesta combinació.")
        return

    vals   = row[metric_cols].iloc[0]
    df_bar = pd.DataFrame({"Mètrica": metric_cols, "Valor": [int(vals[c]) for c in metric_cols]})

    chart = (
        alt.Chart(df_bar)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("Mètrica:N", title=None, sort=None,
                    axis=alt.Axis(labelAngle=-30, labelColor="#94a3b8")),
            y=alt.Y("Valor:Q", title="Total",
                    axis=alt.Axis(labelColor="#94a3b8", titleColor="#cbd5e1")),
            color=alt.Color("Mètrica:N",
                            scale=alt.Scale(scheme="tealblues"),
                            legend=None),
            tooltip=["Mètrica:N", "Valor:Q"],
        )
        .properties(height=300, background="#0f172a",
                    title=f"{sel_barri}  ·  {sel_dist}")
        .configure_title(color="#f1f5f9")
        .configure_view(strokeOpacity=0)
    )
    st.altair_chart(chart, use_container_width=True)


def _render_accidents_predictor(bundle: dict) -> None:
    st.markdown("##### Predictor d'accident greu")
    model    = bundle.get("model")
    scaler   = bundle.get("scaler")
    features = bundle.get("features", [])
    if not model or not features:
        st.info("Model no disponible.")
        return

    label_map = {
        "NK_Any":  ("Any",        2020, 2025, 2024),
        "Mes_any": ("Mes (1–12)",    1,   12,    6),
        "Dia_mes": ("Dia (1–31)",    1,   31,   15),
        "Hora_dia":("Hora (0–23)",   0,   23,   12),
    }

    vals    = {}
    ordered = [f for f in label_map if f in features]
    cols    = st.columns(len(ordered) if ordered else 1)
    for i, feat in enumerate(ordered):
        label, mn, mx, default = label_map[feat]
        vals[feat] = cols[i].slider(label, min_value=mn, max_value=mx, value=default, key=f"acc_pred_{feat}")

    if st.button("Predir", key="acc_predict_btn", use_container_width=True):
        import numpy as np
        row        = np.array([[vals.get(f, 0) for f in features]])
        y_pred     = model.predict(scaler.transform(row))[0]
        proba      = model.predict_proba(scaler.transform(row))[0]
        prob_greu  = float(proba[1]) if len(proba) > 1 else float(proba[0])
        if y_pred == 1:
            st.markdown(
                f"<div style='text-align:center;padding:1rem;border-radius:12px;"
                f"background:rgba(220,38,38,0.18);border:1px solid #ef4444;'>"
                f"<span style='font-size:2rem;'>🚨</span><br>"
                f"<span style='font-size:1.3rem;font-weight:700;color:#fca5a5;'>ACCIDENT GREU</span><br>"
                f"<span style='color:#94a3b8;font-size:0.9rem;'>Probabilitat: {prob_greu:.1%}</span></div>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f"<div style='text-align:center;padding:1rem;border-radius:12px;"
                f"background:rgba(13,148,136,0.18);border:1px solid #2dd4bf;'>"
                f"<span style='font-size:2rem;'>✅</span><br>"
                f"<span style='font-size:1.3rem;font-weight:700;color:#5eead4;'>ACCIDENT LLEU / NO GREU</span><br>"
                f"<span style='color:#94a3b8;font-size:0.9rem;'>Probabilitat accident greu: {prob_greu:.1%}</span></div>",
                unsafe_allow_html=True,
            )


def section_accidents_gu(bucket: str) -> None:
    bundle = _accidents_model(bucket)

    col_ml, col_chart = st.columns(2, gap="large")
    with col_ml:
        st.markdown("##### Model de predicció d'accident greu (Logistic Regression + balanced)")
        if bundle and "error" in bundle:
            st.warning(bundle["error"])
            st.caption(f"Train: {bundle['n_train']:,} mostres · Test: {bundle['n_test']:,} mostres")
        elif bundle:
            cc = bundle.get("class_counts", {})
            st.caption(f"Distribució train → No greu (0): {cc.get(0, '?'):,} · Greu (1): {cc.get(1, '?'):,}")
            _render_ml_panel(bundle, labels=["No greu", "Greu"])
        else:
            st.info("Sense dades de train/test per entrenar el model.")
    with col_chart:
        st.markdown("##### Accidents per districte i barri")
        _render_accidents_chart(bucket)

    if bundle and "error" not in bundle:
        st.markdown("<div style='margin-top:1.25rem;border-top:1px solid rgba(94,234,212,0.15);padding-top:0.5rem;'></div>", unsafe_allow_html=True)
        _render_accidents_predictor(bundle)


def section_tomtom(bucket: str) -> None:
    df = load_parquet_prefix(bucket, "TOMTOM/")
    if df.empty:
        st.info("Sense dades TomTom. Executa el pipeline d'exploitation.")
        return

    need = ["point_name", "avg_current_speed_kmh", "avg_free_flow_speed_kmh",
            "avg_speed_diff_kmh", "avg_congestion_pct", "n_measurements"]
    df = df[[c for c in need if c in df.columns]].copy()
    df["avg_congestion_pct"]    = pd.to_numeric(df["avg_congestion_pct"],    errors="coerce").fillna(0)
    df["avg_current_speed_kmh"] = pd.to_numeric(df["avg_current_speed_kmh"], errors="coerce").fillna(0)
    df["avg_free_flow_speed_kmh"] = pd.to_numeric(df["avg_free_flow_speed_kmh"], errors="coerce").fillna(0)
    df = df.sort_values("avg_congestion_pct", ascending=False).reset_index(drop=True)

    # KPIs ràpids
    n_points    = len(df)
    avg_cong    = df["avg_congestion_pct"].mean()
    worst       = df.iloc[0]["point_name"] if n_points > 0 else "—"
    worst_pct   = df.iloc[0]["avg_congestion_pct"] if n_points > 0 else 0
    congested   = int((df["avg_congestion_pct"] > 20).sum())

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Punts monitoritzats", f"{n_points}")
    k2.metric("Congestió mitja",     f"{avg_cong:.1f} %")
    k3.metric("Punt més congestió",  worst, delta=f"{worst_pct:.0f} %", delta_color="inverse")
    k4.metric("Punts > 20 % cong.",  f"{congested}")

    st.markdown("<div style='margin-top:0.75rem;'></div>", unsafe_allow_html=True)

    # Capa 1: barra grisa de referència (velocitat lliure)
    base = alt.Chart(df).encode(
        y=alt.Y("point_name:N",
                sort=alt.EncodingSortField(field="avg_congestion_pct", order="descending"),
                title=None,
                axis=alt.Axis(labelColor="#e2e8f0", labelFontSize=12, ticks=False)),
    )

    bar_free = base.mark_bar(color="#1e293b", height=18).encode(
        x=alt.X("avg_free_flow_speed_kmh:Q", title="Velocitat (km/h)",
                axis=alt.Axis(labelColor="#64748b", titleColor="#94a3b8", gridColor="#1e293b")),
    )

    bar_curr = base.mark_bar(height=11, cornerRadiusTopRight=4, cornerRadiusBottomRight=4).encode(
        x=alt.X("avg_current_speed_kmh:Q", title=""),
        color=alt.Color("avg_congestion_pct:Q",
            scale=alt.Scale(domain=[0, 15, 30, 45],
                            range=["#2dd4bf", "#a3e635", "#fb923c", "#ef4444"]),
            title="Congestió (%)",
            legend=alt.Legend(
                labelColor="#cbd5e1", titleColor="#e2e8f0",
                orient="bottom", direction="horizontal",
                gradientLength=200,
            ),
        ),
        tooltip=[
            alt.Tooltip("point_name:N",              title="Punt"),
            alt.Tooltip("avg_current_speed_kmh:Q",   title="Velocitat actual (km/h)",  format=".0f"),
            alt.Tooltip("avg_free_flow_speed_kmh:Q", title="Velocitat lliure (km/h)",  format=".0f"),
            alt.Tooltip("avg_speed_diff_kmh:Q",      title="Diferència (km/h)",        format=".0f"),
            alt.Tooltip("avg_congestion_pct:Q",      title="Congestió (%)",            format=".1f"),
        ],
    )

    text_pct = base.mark_text(align="left", dx=6, color="#94a3b8", fontSize=11).encode(
        x=alt.X("avg_free_flow_speed_kmh:Q"),
        text=alt.condition(
            alt.datum.avg_congestion_pct > 0,
            alt.Text("avg_congestion_pct:Q", format=".0f"),
            alt.value(""),
        ),
    )

    chart = (
        alt.layer(bar_free, bar_curr, text_pct)
        .properties(
            height=max(320, n_points * 28),
            background="#0f172a",
            title=alt.TitleParams(
                "Velocitat actual vs. velocitat lliure per punt TomTom",
                color="#f1f5f9", fontSize=14,
                subtitle="Barra fosca = velocitat lliure (referència)  ·  Barra acolorida = velocitat actual  ·  % = congestió",
                subtitleColor="#64748b", subtitleFontSize=11,
            ),
        )
        .configure_view(strokeOpacity=0)
    )
    st.altair_chart(chart, use_container_width=True)


def section_daily_incidents(bucket: str) -> None:
    import html as htmllib
    df = load_parquet_prefix(bucket, "incidents/daily_incidents/")
    if df.empty:
        st.info("Sense incidents disponibles. Executa el pipeline d'exploitation.")
        return

    df = df.copy()
    if "data_ts" in df.columns:
        df["data_ts"] = pd.to_datetime(df["data_ts"], errors="coerce")
        df = df.sort_values("data_ts", ascending=False)

    total = len(df)
    st.caption(f"{total} incident{'s' if total != 1 else ''} carregat{'s' if total != 1 else ''}")

    for _, row in df.iterrows():
        titol     = str(row.get("titol", "") or "").strip()
        desc      = str(row.get("descripcio", "") or "").strip()
        autor     = str(row.get("autor", "@transit") or "@transit").strip()
        link      = str(row.get("link", "") or "").strip()
        data_ts   = row.get("data_ts")
        carretera = str(row.get("carretera", "") or "").strip()

        text = titol if titol else desc
        if not text:
            continue

        data_str   = data_ts.strftime("%d/%m/%Y") if pd.notna(data_ts) else "—"
        inicial    = htmllib.escape((autor[1] if autor.startswith("@") else autor[0]).upper())
        autor_esc  = htmllib.escape(autor)
        text_esc   = htmllib.escape(text[:400]) + ("…" if len(text) > 400 else "")

        road_badge = ""
        if carretera and carretera not in ("nan", "None", ""):
            road_badge = (
                f'<span style="background:rgba(99,102,241,0.25);color:#a5b4fc;padding:0.15rem 0.55rem;'
                f'border-radius:999px;font-size:0.72rem;font-weight:600;">{htmllib.escape(carretera)}</span>'
            )

        link_btn = ""
        if link and link.startswith("http"):
            link_btn = (
                f'<a href="{htmllib.escape(link)}" target="_blank" rel="noopener" '
                f'style="color:#5eead4;font-size:0.82rem;text-decoration:none;margin-top:0.5rem;display:inline-block;">'
                f'Veure post ↗</a>'
            )

        st.html(f"""
        <div style="background:linear-gradient(135deg,rgba(30,41,59,0.75),rgba(15,23,42,0.85));
                    border:1px solid rgba(148,163,184,0.12);border-left:3px solid rgba(94,234,212,0.5);
                    border-radius:12px;padding:0.9rem 1.1rem;margin-bottom:0.5rem;">
            <div style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.5rem;flex-wrap:wrap;">
                <span style="width:32px;height:32px;border-radius:50%;flex-shrink:0;
                             background:linear-gradient(145deg,#2dd4bf,#818cf8);
                             display:inline-flex;align-items:center;justify-content:center;
                             font-size:0.75rem;font-weight:700;color:#0f172a;">{inicial}</span>
                <span style="font-weight:700;color:#f1f5f9;font-size:0.92rem;">{autor_esc}</span>
                <span style="color:#475569;">·</span>
                <span style="color:#64748b;font-size:0.8rem;">{data_str}</span>
                {road_badge}
            </div>
            <div style="color:#e2e8f0;font-size:0.93rem;line-height:1.55;">{text_esc}</div>
            {link_btn}
        </div>
        """)


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
        "Tres fonts: **warm path · trams** (Kafka directe, refresc 2 min), "
        "**cold path** (exploitation-zone, refresc horari) i "
        "**warm path · IoT** (sensors sintètics, refresc 2 min). "
        "La geometria dels trams és dades de referència estàtiques llegides una "
        "vegada del cold store.",
    )
    with st.container(border=True):
        tab_warm, tab_cold, tab_iot = st.tabs([
            "Warm path · trams (Kafka)",
            "Cold path · trams (batch)",
            "Warm path · IoT sensors (Kafka)",
        ])

        with tab_warm:
            st.caption(
                "Origen: Kafka `trams_congestion` · congestió en memòria, refresc cada 2 min · "
                "geometria: `exploitation-zone/ajuntament_bcn/trams_map/` (caché 1 h)."
            )
            start_trams_consumer()

            @st.fragment(run_every="2m")
            def _warm_map_widget() -> None:
                geometry = cached_load_trams_map(exploitation_bucket, pfx_trams_map)
                snapshot = get_trams_snapshot()

                if snapshot.empty:
                    st.info(
                        "Esperant dades de Kafka (`trams_congestion`). "
                        "El consumidor fa replay del topic en arrencar; "
                        "triga uns segons la primera vegada."
                    )
                    return

                snap = snapshot.copy()
                id_col = "id_tram" if "id_tram" in snap.columns else snap.columns[0]
                snap["id_tram"] = snap[id_col].astype(str)

                if not geometry.empty:
                    geo = geometry[
                        [c for c in ["id_tram", "path", "descripcio", "n_punts"]
                         if c in geometry.columns]
                    ].copy()
                    geo["id_tram"] = geo["id_tram"].astype(str)
                    extra = [c for c in ["estatActual", "estatPrevist", "data"]
                             if c in snap.columns]
                    merged = geo.merge(snap[["id_tram"] + extra], on="id_tram", how="left")
                else:
                    merged = snap

                render_trams_map(merged)

            _warm_map_widget()

        with tab_cold:
            st.caption(
                "Origen: `s3://exploitation-zone/ajuntament_bcn/trams_map/` · "
                "materialitzat per `exploitation_zone.py` (Spark) · refresc horari."
            )
            render_trams_map(cached_load_trams_map(exploitation_bucket, pfx_trams_map))

        with tab_iot:
            st.caption(
                "Origen: Kafka `Sensors-IoT` · sensors sintètics de trànsit · refresc cada 2 min."
            )
            start_iot_consumer()

            @st.fragment(run_every="2m")
            def _iot_widget() -> None:
                import pydeck as pdk

                snap = get_iot_snapshot()
                if snap.empty:
                    st.info(
                        "Sense dades de `Sensors-IoT`. "
                        "Comprova que el contenidor `sensor-bcn` està en marxa."
                    )
                    return

                snap = snap.copy()
                _iot_colors = {
                    "low":    [26, 188, 94],
                    "medium": [245, 200, 40],
                    "high":   [230, 60, 45],
                }
                snap["color"] = snap["congestion_level"].apply(
                    lambda v: _iot_colors.get(str(v).lower(), [120, 120, 120])
                )

                st.metric("Sensors actius", len(snap))

                layer = pdk.Layer(
                    "ScatterplotLayer",
                    data=snap,
                    get_position=["lon", "lat"],
                    get_fill_color="color",
                    get_radius=150,
                    pickable=True,
                    auto_highlight=True,
                )
                view = pdk.ViewState(latitude=41.39, longitude=2.17, zoom=12)
                st.pydeck_chart(
                    pdk.Deck(
                        layers=[layer],
                        initial_view_state=view,
                        map_style="dark",
                        tooltip={
                            "html": (
                                "<b>{sensor_id}</b><br/>{ubicacion}<br/>"
                                "Velocitat: {avg_speed_kmh} km/h<br/>"
                                "Congestió: {congestion_level}"
                            )
                        },
                    ),
                    use_container_width=True,
                )

            _iot_widget()

    _section_title(
        "Predicció meteorològica (AEMET)",
        "Última predicció horària descarregada de l'API AEMET per a Barcelona i "
        "Hospitalet. Font: `exploitation-zone/aemet/` · actualització cada 6 h · "
        "lectura via `load_aemet_predictions_table` (ml_models).",
    )
    with st.container(border=True):
        render_aemet_current_forecast(exploitation_bucket, pfx_aemet)

    _section_title("Càmeres de trànsit")
    with st.container(border=True):
        section_traffic_cameras(exploitation_bucket, data_prefix=_env("DATA_PREFIX", ""))

    _section_title("AEMET — Clima i predicció de pluja")
    with st.container(border=True):
        section_aemet(exploitation_bucket)

    _section_title("Accidents GU — Predicció i estadístiques per zona")
    with st.container(border=True):
        section_accidents_gu(exploitation_bucket)

    _section_title("TomTom — Velocitat i congestió en temps real")
    with st.container(border=True):
        section_tomtom(exploitation_bucket)

    _section_title("Incidents de trànsit")
    with st.container(border=True):
        section_daily_incidents(exploitation_bucket)

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
