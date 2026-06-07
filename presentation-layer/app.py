"""
Dashboard (capa presentacio): mapa de congestio de trams + places de parking en temps real.
"""
from __future__ import annotations
import os
import streamlit as st
import altair as alt
import pandas as pd
from data_access import (get_s3_object_bytes, list_traffic_camera_gallery_objects, list_traffic_camera_gallery_sites, load_parquet_prefix, load_trams_map)
from hot_path import read_parking_counters, start_parking_consumer
from warm_path_cameras import get_cameras_snapshot, parse_s3_url, start_camera_consumer
from warm_path_trams import (get_iot_snapshot, get_trams_snapshot, start_iot_consumer, start_trams_consumer)
from map_view import render_trams_map
import io
import chromadb
from PIL import Image
import html as htmllib
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, confusion_matrix, f1_score, precision_score, recall_score
from sklearn.preprocessing import StandardScaler
import numpy as np
from ultralytics import YOLO
from sentence_transformers import SentenceTransformer


def _env(name: str, default: str) -> str:
    """Llegeix variable d'entorn; retorna default si es buida o absent."""
    v = os.environ.get(name)
    return v if v is not None and v != "" else default


def _inject_dashboard_css() -> None:
    """Injecta el CSS global del dashboard."""
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
        /* Metriques st.metric */
        [data-testid="stMetricValue"]  > div { color: #f1f5f9 !important; }
        [data-testid="stMetricLabel"]  > div { color: #94a3b8 !important; }
        [data-testid="stMetricDelta"]  > div { color: #5eead4 !important; }
        </style>
        """,
        unsafe_allow_html=True,
    )

def _hero() -> None:
    """Renderitza la capcalera principal del dashboard."""
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
            <p style="margin:0 0 0.35rem 0; font-size:0.75rem; text-transform:uppercase; letter-spacing:0.22em; color:#5eead4; font-weight:600;">BDM · live data</p>
            <h1 style="margin:0; font-size:clamp(1.85rem, 4.2vw, 2.5rem); font-weight:700; color:#f8fafc; line-height:1.12; text-shadow:0 0 40px rgba(45,212,191,0.25);">Urban Mobility Dashboard</h1>
            <p style="margin:0.85rem 0 0 0; font-size:1.05rem; color:#94a3b8; max-width:52rem; line-height:1.6;">
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

def _section_title(title: str, subtitle: str | None = None) -> None:
    """Renderitza el titol de seccio amb subtitol opcional."""
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

@st.cache_data(ttl=300, show_spinner="Loading trams map from S3…")
def cached_load_trams_map(bucket: str, prefix: str):
    """Carrega el mapa de trams des de S3 amb cache de 5 minuts."""
    try:
        return load_trams_map(bucket, prefix)
    except Exception as e:
        st.error(f"Error loading trams_map from S3: {e}")
        return None

def _fmt_camera_site(site_key: str) -> str:
    """Formata el site_key de camera com a text llegible."""
    import re
    s = site_key.strip()
    s = re.sub(r"_-_", " — ", s)
    s = s.replace("_", " ")
    s = re.sub(r"\b([a-z]{1,3})-(\d+)\b", lambda m: f"{m.group(1).upper()}-{m.group(2)}", s)
    return s.strip().title()

_YOLO_VEHICLE_CLASSES = {
    1: "Bicycle",
    2: "Car",
    3: "Motorcycle",
    5: "Bus",
    7: "Truck",
}

@st.cache_resource(show_spinner="Loading CLIP model…")
def _load_clip_model():
    """Carrega el model CLIP (cache per sessio)."""
    return SentenceTransformer("clip-ViT-B-32")


@st.cache_resource(show_spinner="Loading YOLO model…")
def _load_yolo_model():
    """Carrega el model YOLO (cache per sessio)."""
    return YOLO("yolov8n.pt")

def _detect_vehicles(image_bytes: bytes, model) -> tuple[bytes, dict]:
    """Executa YOLO sobre una imatge i retorna la imatge anotada i els comptadors per classe."""
    img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    results = model(img, verbose=False)[0]
    counts: dict[str, int] = {label: 0 for label in _YOLO_VEHICLE_CLASSES.values()}
    for box in results.boxes:
        cls_id = int(box.cls[0])
        if cls_id in _YOLO_VEHICLE_CLASSES:
            counts[_YOLO_VEHICLE_CLASSES[cls_id]] += 1

    annotated_bgr = results.plot()
    pil_img = Image.fromarray(annotated_bgr[:, :, ::-1])
    buf = io.BytesIO()
    pil_img.save(buf, format="JPEG", quality=90)
    return buf.getvalue(), counts

def section_traffic_cameras(bucket: str, data_prefix: str) -> None:
    """Seccio de visualitzacio de cameres de transit amb deteccio de vehicles opcional."""
    snapshot = get_cameras_snapshot()
    use_warm = bool(snapshot)

    if use_warm:
        sites = sorted(snapshot.keys())
    else:
        sites = list_traffic_camera_gallery_sites(bucket, data_prefix)

    if not sites:
        st.info("No camera images available.")
        return

    labels = {s: _fmt_camera_site(s) for s in sites}

    col_sel, col_toggle = st.columns([3, 1])
    with col_sel:
        selected = st.selectbox(
            "Camera",
            options=sites,
            format_func=lambda s: labels[s],
            label_visibility="collapsed",
        )
    with col_toggle:
        yolo_on = st.toggle("Vehicle detection", value=False, key="yolo_toggle")

    if use_warm:
        items = snapshot[selected]
        if not items:
            st.warning("No images for this camera.")
            return
        if len(items) > 1:
            names = [e["filename"] for e in items]
            choice_name = st.select_slider("Snapshot", options=names, label_visibility="collapsed")
            entry = next(e for e in items if e["filename"] == choice_name)
        else:
            entry = items[0]
        s3_bucket, s3_key = parse_s3_url(entry["url"])
        try:
            body = get_s3_object_bytes(s3_bucket, s3_key)
        except Exception as ex:
            st.error(f"Could not load image: {ex}")
            return
    else:
        items = list_traffic_camera_gallery_objects(bucket, data_prefix, selected)
        if not items:
            st.warning("No images for this camera.")
            return
        if len(items) > 1:
            names = [n for _, n in items]
            choice_name = st.select_slider("Snapshot", options=names, label_visibility="collapsed")
            key = next(k for k, n in items if n == choice_name)
        else:
            key, _ = items[0]
        try:
            body = get_s3_object_bytes(bucket, key)
        except Exception as ex:
            st.error(f"Could not load image: {ex}")
            return

    if not yolo_on:
        st.image(body, use_container_width=True)
        return

    try:
        model = _load_yolo_model()
    except Exception as yolo_err:
        st.error(f"YOLO error: {yolo_err}")
        st.image(body, use_container_width=True)
        return

    with st.spinner("Detecting vehicles…"):
        annotated_bytes, counts = _detect_vehicles(body, model)

    st.image(annotated_bytes, use_container_width=True)

    total = sum(counts.values())

    if total == 0:
        st.caption("No vehicles detected in this image.")
        return

    st.markdown(
        "<div style='margin-top:0.6rem;padding:0.75rem 1rem;"
        "background:rgba(13,148,136,0.12);border:1px solid rgba(94,234,212,0.2);"
        "border-radius:12px;display:flex;flex-wrap:wrap;gap:1.2rem;align-items:center;'>",
        unsafe_allow_html=True,
    )

    metric_cols = st.columns(len(_YOLO_VEHICLE_CLASSES) + 1)
    metric_cols[0].metric("Total vehicles", total)
    for i, (_, label) in enumerate(_YOLO_VEHICLE_CLASSES.items(), start=1):
        metric_cols[i].metric(label, counts.get(label, 0))


@st.cache_data(ttl=600, show_spinner="Training AEMET model…")
def _aemet_model(bucket: str):
    """Entrena el model de prediccio de pluja amb les dades AEMET de exploitation."""
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
        return {"error": "Training dataset has a single class. Re-run the exploitation pipeline to regenerate data with synthetic samples.", "n_train": len(X_train), "n_test": len(X_test)}

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
    """Renderitza les metriques del model ML i la matriu de confusio."""
    acc, pre, rec, f1 = bundle["accuracy"], bundle["precision"], bundle["recall"], bundle["f1"]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Accuracy",  f"{acc:.3f}")
    c2.metric("Precision", f"{pre:.3f}")
    c3.metric("Recall",    f"{rec:.3f}")
    c4.metric("F1",        f"{f1:.3f}")

    cm_raw = bundle["cm"]
    labels = labels if labels is not None else ["No rain", "Rain"]
    n = len(cm_raw)
    rows = [
        {"Actual": labels[i] if i < 2 else str(i), "Predicted": labels[j] if j < 2 else str(j), "n": cm_raw[i][j]}
        for i in range(n) for j in range(len(cm_raw[i]))
    ]
    df_cm = pd.DataFrame(rows)

    base = alt.Chart(df_cm).encode(
        x=alt.X("Predicted:N", title="Predicted"),
        y=alt.Y("Actual:N", title="Actual"),
    )
    heatmap = base.mark_rect().encode(
        color=alt.Color("n:Q", scale=alt.Scale(scheme="tealblues"), legend=None)
    )
    text = base.mark_text(color="white", fontSize=20, fontWeight="bold").encode(text="n:Q")
    chart = (heatmap + text).properties(
        height=200, background="#0f172a", title="Confusion Matrix"
    ).configure_axis(
        labelColor="#94a3b8", titleColor="#cbd5e1"
    ).configure_title(color="#f1f5f9")
    st.altair_chart(chart, use_container_width=True)
    st.caption(f"Train: {bundle['n_train']:,} samples · Test: {bundle['n_test']:,} samples · Logistic Regression")

def _render_climate_chart(bucket: str) -> None:
    """Renderitza el grafic de tendencia climatica per municipi."""
    df = load_parquet_prefix(bucket, "aemet/climate_by_municipio/")
    if df.empty:
        st.info("No climate_by_municipio data available.")
        return

    municipis = sorted(df["nombre_municipio"].dropna().unique().tolist())
    selected = st.selectbox("Municipality", municipis, key="aemet_municipio")

    sub = df[df["nombre_municipio"] == selected].copy()
    sub["data"] = pd.to_datetime(sub["dia_fecha_parsed"], errors="coerce")
    sub = sub.sort_values("data")

    metric_map = {
        "avg_temperatura":      "Temperature (°C)",
        "avg_sens_termica":     "Feels like (°C)",
        "avg_humedad_relativa": "Relative humidity (%)",
    }
    cols_ok = [c for c in metric_map if c in sub.columns]
    melt = sub[["data"] + cols_ok].melt(id_vars="data", var_name="clau", value_name="valor")
    melt["Metric"] = melt["clau"].map(metric_map)
    melt["valor"] = pd.to_numeric(melt["valor"], errors="coerce")
    melt = melt.dropna(subset=["valor"])

    colors = ["#2dd4bf", "#818cf8", "#fb923c"][: len(cols_ok)]
    dashes = [[1, 0], [6, 3], [4, 2]][: len(cols_ok)]
    domain = [metric_map[c] for c in cols_ok]

    chart = (
        alt.Chart(melt)
        .mark_line(point=True, strokeWidth=2.5)
        .encode(
            x=alt.X("data:T", title="Date"),
            y=alt.Y("valor:Q", title="Value"),
            color=alt.Color(
                "Metric:N",
                scale=alt.Scale(domain=domain, range=colors),
                legend=alt.Legend(
                    labelColor="#cbd5e1",
                    titleColor="#e2e8f0",
                    labelFontSize=12,
                    titleFontSize=12,
                ),
            ),
            strokeDash=alt.StrokeDash(
                "Metric:N",
                scale=alt.Scale(domain=domain, range=dashes),
                legend=None,
            ),
            tooltip=[alt.Tooltip("data:T", title="Date"), "Metric:N", alt.Tooltip("valor:Q", format=".1f")],
        )
        .properties(height=300, background="#0f172a", title=f"Climate trend — {selected}")
        .configure_axis(labelColor="#94a3b8", titleColor="#cbd5e1", gridColor="#1e293b")
        .configure_title(color="#f1f5f9")
    )
    st.altair_chart(chart, use_container_width=True)

def _render_rain_predictor(bundle: dict) -> None:
    """Widget interactiu per predir si plourà donades condicions meteorologiques."""
    st.markdown("##### Rain Predictor")
    model = bundle.get("model")
    scaler = bundle.get("scaler")
    features = bundle.get("features", [])
    if not model or not features:
        st.info("Model not available.")
        return

    label_map = {
        "mes": ("Month (1–12)", 1, 12, 6),
        "dia": ("Day (1–31)", 1, 31, 15),
        "hora_periodo": ("Hour (0–23)", 0, 23, 12),
        "temperatura": ("Temperature (°C)", -5, 45, 20),
        "sens_termica": ("Feels like (°C)", -10, 45, 18),
        "humedad_relativa": ("Relative humidity (%)", 0, 100, 65),
    }

    vals = {}
    ordered = [f for f in label_map if f in features]
    cols = st.columns(3)
    for i, feat in enumerate(ordered):
        label, mn, mx, default = label_map[feat]
        vals[feat] = cols[i % 3].slider(label, min_value=mn, max_value=mx, value=default, key=f"pred_{feat}")

    if st.button("Predict", key="rain_predict_btn", use_container_width=True):
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
                f"<span style='font-size:1.3rem;font-weight:700;color:#a5b4fc;'>RAIN EXPECTED</span><br>"
                f"<span style='color:#94a3b8;font-size:0.9rem;'>Probability: {prob_plou:.1%}</span></div>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f"<div style='text-align:center;padding:1rem;border-radius:12px;"
                f"background:rgba(13,148,136,0.2);border:1px solid #2dd4bf;'>"
                f"<span style='font-size:2rem;'>☀️</span><br>"
                f"<span style='font-size:1.3rem;font-weight:700;color:#5eead4;'>NO RAIN EXPECTED</span><br>"
                f"<span style='color:#94a3b8;font-size:0.9rem;'>Rain probability: {prob_plou:.1%}</span></div>",
                unsafe_allow_html=True,
            )

def section_aemet(bucket: str) -> None:
    """Seccio AEMET: model de pluja + tendencia climatica per municipi."""
    bundle = _aemet_model(bucket)
    col_ml, col_chart = st.columns(2, gap="large")
    with col_ml:
        st.markdown("##### Rain Prediction Model (Logistic Regression)")
        if bundle and "error" in bundle:
            st.warning(bundle["error"])
            st.caption(f"Train: {bundle['n_train']:,} samples · Test: {bundle['n_test']:,} samples")
        elif bundle:
            _render_ml_panel(bundle)
        else:
            st.info("No train/test data to fit the model.")
    with col_chart:
        st.markdown("##### Climate Trend by Municipality")
        _render_climate_chart(bucket)

    if bundle and "error" not in bundle:
        st.markdown("<div style='margin-top:1.25rem;border-top:1px solid rgba(94,234,212,0.15);padding-top:0.5rem;'></div>", unsafe_allow_html=True)
        _render_rain_predictor(bundle)

@st.cache_data(ttl=600, show_spinner="Training accident model…")
def _accidents_model(bucket: str):
    """Entrena el model de prediccio d'accidents greus amb les dades d'accidents GU."""
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
        return {"error": "Training dataset has only one class.", "n_train": len(X_train), "n_test": len(X_test)}

    common  = [c for c in X_train.columns if c in X_test.columns]
    X_train = X_train[common].fillna(0)
    X_test  = X_test[common].fillna(0)

    scaler = StandardScaler()
    # class_weight='balanced' compensa el desequilibri de classes (molts menys accidents greus)
    model  = LogisticRegression(max_iter=1000, random_state=42, class_weight="balanced")
    model.fit(scaler.fit_transform(X_train), y_train)
    y_pred = model.predict(scaler.transform(X_test))

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
    """Corregeix codificacio latin-1 mal interpretada com a UTF-8."""
    try:
        return s.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return s

def _render_accidents_chart(bucket: str) -> None:
    """Renderitza el grafic d'accidents per districte i barri."""
    df = load_parquet_prefix(bucket, "ajuntament_bcn/accidents_gu_by_district/")
    if df.empty:
        st.info("No accident data by district.")
        return

    df["Nom_districte"] = df["Nom_districte"].dropna().apply(_fix_enc)
    df["Nom_barri"]     = df["Nom_barri"].dropna().apply(_fix_enc)

    metric_cols = [c for c in ["total_morts", "total_lesionats_greus", "total_lesionats_lleus",
                                "total_victimes", "total_vehicles_implicats", "total_accidents"]
                   if c in df.columns]

    districtes = sorted(df["Nom_districte"].dropna().unique().tolist())
    sel_dist   = st.selectbox("District", districtes, key="acc_dist")
    df_dist    = df[df["Nom_districte"] == sel_dist]
    barris     = sorted(df_dist["Nom_barri"].dropna().unique().tolist())
    sel_barri  = st.selectbox("Neighbourhood", barris, key="acc_barri")

    row = df_dist[df_dist["Nom_barri"] == sel_barri]
    if row.empty or not metric_cols:
        st.warning("No data for this combination.")
        return

    vals   = row[metric_cols].iloc[0]
    df_bar = pd.DataFrame({"Metric": metric_cols, "Count": [int(vals[c]) for c in metric_cols]})

    chart = (
        alt.Chart(df_bar)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("Metric:N", title=None, sort=None,
                    axis=alt.Axis(labelAngle=-30, labelColor="#94a3b8")),
            y=alt.Y("Count:Q", title="Total",
                    axis=alt.Axis(labelColor="#94a3b8", titleColor="#cbd5e1")),
            color=alt.Color("Metric:N",
                            scale=alt.Scale(scheme="tealblues"),
                            legend=None),
            tooltip=["Metric:N", "Count:Q"],
        )
        .properties(height=300, background="#0f172a",
                    title=f"{sel_barri}  ·  {sel_dist}")
        .configure_title(color="#f1f5f9")
        .configure_view(strokeOpacity=0)
    )
    st.altair_chart(chart, use_container_width=True)

def _render_accidents_predictor(bundle: dict) -> None:
    """Widget interactiu per predir si un accident sera greu."""
    st.markdown("##### Serious Accident Predictor")
    model    = bundle.get("model")
    scaler   = bundle.get("scaler")
    features = bundle.get("features", [])
    if not model or not features:
        st.info("Model not available.")
        return

    label_map = {
        "NK_Any":  ("Year", 2020, 2025, 2024),
        "Mes_any": ("Month (1–12)", 1, 12, 6),
        "Dia_mes": ("Day (1–31)", 1, 31, 15),
        "Hora_dia":("Hour (0–23)", 0, 23, 12),
    }

    vals    = {}
    ordered = [f for f in label_map if f in features]
    cols    = st.columns(len(ordered) if ordered else 1)
    for i, feat in enumerate(ordered):
        label, mn, mx, default = label_map[feat]
        vals[feat] = cols[i].slider(label, min_value=mn, max_value=mx, value=default, key=f"acc_pred_{feat}")

    if st.button("Predict", key="acc_predict_btn", use_container_width=True):
        import numpy as np
        row = np.array([[vals.get(f, 0) for f in features]])
        y_pred = model.predict(scaler.transform(row))[0]
        proba = model.predict_proba(scaler.transform(row))[0]
        prob_greu = float(proba[1]) if len(proba) > 1 else float(proba[0])
        if y_pred == 1:
            st.markdown(
                f"<div style='text-align:center;padding:1rem;border-radius:12px;"
                f"background:rgba(220,38,38,0.18);border:1px solid #ef4444;'>"
                f"<span style='font-size:2rem;'>🚨</span><br>"
                f"<span style='font-size:1.3rem;font-weight:700;color:#fca5a5;'>SERIOUS ACCIDENT</span><br>"
                f"<span style='color:#94a3b8;font-size:0.9rem;'>Probability: {prob_greu:.1%}</span></div>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f"<div style='text-align:center;padding:1rem;border-radius:12px;"
                f"background:rgba(13,148,136,0.18);border:1px solid #2dd4bf;'>"
                f"<span style='font-size:2rem;'>✅</span><br>"
                f"<span style='font-size:1.3rem;font-weight:700;color:#5eead4;'>MINOR / NO ACCIDENT</span><br>"
                f"<span style='color:#94a3b8;font-size:0.9rem;'>Serious accident probability: {prob_greu:.1%}</span></div>",
                unsafe_allow_html=True,
            )

def section_accidents_gu(bucket: str) -> None:
    """Seccio accidents GU: model de prediccio + grafic per districte i barri."""
    bundle = _accidents_model(bucket)
    col_ml, col_chart = st.columns(2, gap="large")
    with col_ml:
        st.markdown("##### Serious Accident Prediction Model (Logistic Regression + balanced)")
        if bundle and "error" in bundle:
            st.warning(bundle["error"])
            st.caption(f"Train: {bundle['n_train']:,} samples · Test: {bundle['n_test']:,} samples")
        elif bundle:
            cc = bundle.get("class_counts", {})
            st.caption(f"Train distribution → Non-serious (0): {cc.get(0, '?'):,} · Serious (1): {cc.get(1, '?'):,}")
            _render_ml_panel(bundle, labels=["Non-serious", "Serious"])
        else:
            st.info("No train/test data to fit the model.")
    with col_chart:
        st.markdown("##### Accidents by District and Neighbourhood")
        _render_accidents_chart(bucket)

    if bundle and "error" not in bundle:
        st.markdown("<div style='margin-top:1.25rem;border-top:1px solid rgba(94,234,212,0.15);padding-top:0.5rem;'></div>", unsafe_allow_html=True)
        _render_accidents_predictor(bundle)

def section_tomtom(bucket: str) -> None:
    """Seccio TomTom: velocitat actual vs velocitat lliure per punt de mesura."""
    df = load_parquet_prefix(bucket, "TOMTOM/")
    if df.empty:
        st.info("No TomTom data. Run the exploitation pipeline.")
        return

    need = ["point_name", "avg_current_speed_kmh", "avg_free_flow_speed_kmh",
            "avg_speed_diff_kmh", "avg_congestion_pct", "n_measurements"]
    df = df[[c for c in need if c in df.columns]].copy()
    df["avg_congestion_pct"]    = pd.to_numeric(df["avg_congestion_pct"],    errors="coerce").fillna(0)
    df["avg_current_speed_kmh"] = pd.to_numeric(df["avg_current_speed_kmh"], errors="coerce").fillna(0)
    df["avg_free_flow_speed_kmh"] = pd.to_numeric(df["avg_free_flow_speed_kmh"], errors="coerce").fillna(0)
    df = df.sort_values("avg_congestion_pct", ascending=False).reset_index(drop=True)

    n_points    = len(df)
    avg_cong    = df["avg_congestion_pct"].mean()
    worst       = df.iloc[0]["point_name"] if n_points > 0 else "—"
    worst_pct   = df.iloc[0]["avg_congestion_pct"] if n_points > 0 else 0
    congested   = int((df["avg_congestion_pct"] > 20).sum())

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Monitored points",       f"{n_points}")
    k2.metric("Average congestion",     f"{avg_cong:.1f} %")
    k3.metric("Most congested point",   worst, delta=f"{worst_pct:.0f} %", delta_color="inverse")
    k4.metric("Points > 20% congestion", f"{congested}")

    st.markdown("<div style='margin-top:0.75rem;'></div>", unsafe_allow_html=True)

    base = alt.Chart(df).encode(
        y=alt.Y("point_name:N",
                sort=alt.EncodingSortField(field="avg_congestion_pct", order="descending"),
                title=None,
                axis=alt.Axis(labelColor="#e2e8f0", labelFontSize=12, ticks=False)),
    )

    bar_free = base.mark_bar(color="#1e293b", height=18).encode(
        x=alt.X("avg_free_flow_speed_kmh:Q", title="Speed (km/h)",
                axis=alt.Axis(labelColor="#64748b", titleColor="#94a3b8", gridColor="#1e293b")),
    )

    bar_curr = base.mark_bar(height=11, cornerRadiusTopRight=4, cornerRadiusBottomRight=4).encode(
        x=alt.X("avg_current_speed_kmh:Q", title=""),
        color=alt.Color("avg_congestion_pct:Q",
            scale=alt.Scale(domain=[0, 15, 30, 45],
                            range=["#2dd4bf", "#a3e635", "#fb923c", "#ef4444"]),
            title="Congestion (%)",
            legend=alt.Legend(
                labelColor="#cbd5e1", titleColor="#e2e8f0",
                orient="bottom", direction="horizontal",
                gradientLength=200,
            ),
        ),
        tooltip=[
            alt.Tooltip("point_name:N",              title="Point"),
            alt.Tooltip("avg_current_speed_kmh:Q",   title="Current speed (km/h)",    format=".0f"),
            alt.Tooltip("avg_free_flow_speed_kmh:Q", title="Free-flow speed (km/h)",  format=".0f"),
            alt.Tooltip("avg_speed_diff_kmh:Q",      title="Difference (km/h)",       format=".0f"),
            alt.Tooltip("avg_congestion_pct:Q",      title="Congestion (%)",          format=".1f"),
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
                "Current speed vs. free-flow speed by TomTom point",
                color="#f1f5f9", fontSize=14,
                subtitle="Dark bar = free-flow speed (reference)  ·  Coloured bar = current speed  ·  % = congestion",
                subtitleColor="#64748b", subtitleFontSize=11,
            ),
        )
        .configure_view(strokeOpacity=0)
    )
    st.altair_chart(chart, use_container_width=True)

def section_daily_incidents(bucket: str) -> None:
    """Seccio d'incidents de transit del dia (obres SCT + posts de transit)."""

    df = load_parquet_prefix(bucket, "incidents/daily_incidents/")
    if df.empty:
        st.info("No incidents available. Run the exploitation pipeline.")
        return

    df = df.copy()
    if "data_ts" in df.columns:
        df["data_ts"] = pd.to_datetime(df["data_ts"], errors="coerce")
        df = df.sort_values("data_ts", ascending=False)

    total = len(df)
    st.caption(f"{total} incident{'s' if total != 1 else ''} loaded")

    cards = []
    for _, row in df.iterrows():
        titol     = str(row.get("titol", "") or "").strip()
        desc      = str(row.get("descripcio", "") or "").strip()
        autor     = str(row.get("autor", "@transit") or "@transit").strip()
        link      = str(row.get("link", "") or "").strip()
        data_ts   = row.get("data_ts")
        carretera = str(row.get("carretera", "") or "").strip()

        if titol.lower() == "nan":
            titol = "SCT_traffic_Open_Data"
        if autor.lower() == "nan":
            autor = "SCT_traffic_Open_Data"

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
                f'View post ↗</a>'
            )

        cards.append(f"""
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

    if cards:
        st.html(
            '<div style="max-height:520px;overflow-y:auto;padding-right:0.25rem;">'
            + "".join(cards)
            + "</div>"
        )

def section_camera_search() -> None:
    """Cerca la camera mes similar a una imatge pujada per l'usuari via CLIP + ChromaDB."""

    chroma_host = os.environ.get("CHROMA_HOST", "chromadb")
    chroma_port = int(os.environ.get("CHROMA_PORT", "8000"))

    uploaded = st.file_uploader(
        "Upload image", type=["jpg", "jpeg", "png"], label_visibility="collapsed"
    )
    if uploaded is None:
        return

    img_bytes = uploaded.read()

    col_in, col_res = st.columns([1, 2])

    with col_in:
        st.markdown("**Your image**")
        st.image(img_bytes, use_container_width=True)

    with col_res:
        st.markdown("**Most likely camera location**")

        try:
            chroma     = chromadb.HttpClient(host=chroma_host, port=chroma_port)
            collection = chroma.get_collection("camera_images")
            n_indexed  = collection.count()
        except Exception as exc:
            st.warning(f"ChromaDB not available or not indexed yet: {exc}")
            st.info("Generate embeddings first:\n```\ndocker compose --profile embed up camera-embedder\n```")
            return

        if n_indexed == 0:
            st.info("No embeddings yet.\n```\ndocker compose --profile embed up camera-embedder\n```")
            return

        with st.spinner("Generating embedding and searching…"):
            model     = _load_clip_model()
            query_img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
            embedding = model.encode(query_img).tolist()
            results   = collection.query(query_embeddings=[embedding], n_results=1, include=["metadatas", "distances"])

        meta       = results["metadatas"][0][0]
        dist       = results["distances"][0][0]
        similarity = max(0.0, 1.0 - dist)
        cam_label  = _fmt_camera_site(meta["camera"])

        st.markdown(
            f"<div style='margin:0.5rem 0 0.35rem 0;padding:0.6rem 1rem;"
            f"background:rgba(13,148,136,0.12);border-left:3px solid #2dd4bf;"
            f"border-radius:8px;display:flex;justify-content:space-between;align-items:center;'>"
            f"<span style='font-weight:700;color:#f1f5f9;font-size:0.95rem;'>{cam_label}</span>"
            f"<span style='color:#5eead4;font-size:0.85rem;font-weight:600;'>{similarity:.1%} similarity</span>"
            f"</div>",
            unsafe_allow_html=True,
        )
        try:
            st.image(get_s3_object_bytes(meta["bucket"], meta["s3_key"]), use_container_width=True)
        except Exception:
            st.caption(f"Image not available: {meta['s3_key']}")

def section_realtime_parking() -> None:
    """Seccio de places de parking en temps real (Kafka + Redis)."""
    start_parking_consumer()

    @st.fragment(run_every="5s")
    def _live_widget() -> None:
        free, total, _ = read_parking_counters()
        occupied = max(0, total - free)
        cols = st.columns(3)
        with cols[0]:
            st.metric("Free spaces", f"{free:,}")
        with cols[1]:
            st.metric("Occupied spaces", f"{occupied:,}")
        with cols[2]:
            st.metric("Active sensors", f"{total:,}")

    _live_widget()

def main() -> None:
    """Punt d'entrada del dashboard Streamlit."""
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
        "Real-time parking spaces",
        "Live counter from Kafka, aggregated in Redis (db=1) and refreshed every 5 s.",
    )
    with st.container(border=True):
        section_realtime_parking()

    _section_title(
        "Tram congestion map",
        "Three sources: **warm path · trams** (Kafka direct, 2-min refresh), "
        "**cold path** (exploitation-zone, hourly refresh) and "
        "**warm path · IoT** (synthetic sensors, 2-min refresh). "
        "Tram geometry is static reference data loaded once from the cold store.",
    )
    with st.container(border=True):
        tab_warm, tab_cold, tab_iot = st.tabs([
            "Warm path · trams (Kafka)",
            "Cold path · trams (batch)",
            "Warm path · IoT sensors (Kafka)",
        ])

        with tab_warm:
            st.caption(
                "Source: Kafka `trams_congestion` · congestion held in memory, 2-min refresh · "
                "geometry: `exploitation-zone/ajuntament_bcn/trams_map/` (1-h cache)."
            )
            start_trams_consumer()

            @st.fragment(run_every="2m")
            def _warm_map_widget() -> None:
                geometry = cached_load_trams_map(exploitation_bucket, pfx_trams_map)
                snapshot = get_trams_snapshot()

                if snapshot.empty:
                    st.info(
                        "Waiting for Kafka data (`trams_congestion`). "
                        "The consumer replays the topic on startup; "
                        "this may take a few seconds the first time."
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
                "Source: `s3://exploitation-zone/ajuntament_bcn/trams_map/` · "
                "materialized by `exploitation_zone.py` (Spark) · hourly refresh."
            )
            render_trams_map(cached_load_trams_map(exploitation_bucket, pfx_trams_map))

        with tab_iot:
            st.caption(
                "Source: Kafka `Sensors-IoT` · synthetic traffic sensors · 2-min refresh."
            )
            start_iot_consumer()

            @st.fragment(run_every="2m")
            def _iot_widget() -> None:
                import pydeck as pdk

                snap = get_iot_snapshot()
                if snap.empty:
                    st.info(
                        "No data from `Sensors-IoT`. "
                        "Check that the `sensor-bcn` container is running."
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

                st.metric("Active sensors", len(snap))

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
                                "Speed: {avg_speed_kmh} km/h<br/>"
                                "Congestion: {congestion_level}"
                            )
                        },
                    ),
                    use_container_width=True,
                )

            _iot_widget()

    _section_title("Traffic cameras")
    with st.container(border=True):
        start_camera_consumer()

        @st.fragment(run_every="10m")
        def _cameras_widget() -> None:
            section_traffic_cameras(exploitation_bucket, data_prefix=_env("DATA_PREFIX", ""))

        _cameras_widget()

    _section_title(
        "Vehicle location search",
        "Upload a photo of a vehicle and find the camera locations where it is most likely to appear.",
    )
    with st.container(border=True):
        section_camera_search()

    _section_title("AEMET — Climate & Rain Prediction")
    with st.container(border=True):
        section_aemet(exploitation_bucket)

    _section_title("Accidents GU — Prediction & Stats by Area")
    with st.container(border=True):
        section_accidents_gu(exploitation_bucket)

    _section_title("TomTom — Speed & Congestion")
    with st.container(border=True):
        section_tomtom(exploitation_bucket)

    _section_title("Traffic incidents")
    with st.container(border=True):
        section_daily_incidents(exploitation_bucket)

    st.markdown(
        """
        <div style="text-align:center; margin-top:2.5rem; padding-top:1.5rem; border-top:1px solid rgba(148,163,184,0.15); color:#64748b; font-size:0.85rem;">
            BDM · presentation layer
        </div>
        """,
        unsafe_allow_html=True,
    )

if __name__ == "__main__":
    main()
