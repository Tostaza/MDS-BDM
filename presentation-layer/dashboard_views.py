"""
Vistes Streamlit del panell: gràfiques, formularis i feeds.

Aquest mòdul només **carrega Parquet/imatges** via ``data_access`` i aplica **models**
via ``ml_models`` (entrenament en memòria per a la UI). Les unions i ETL pesades
van a ``exploitation-zone/`` (Spark), no aquí.
"""

from __future__ import annotations

import html
from datetime import datetime
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st

from data_access import get_s3_object_bytes, list_traffic_camera_gallery_objects, list_traffic_camera_gallery_sites, load_parquet_prefix
from ml_models import (
    normalize_columns,
    predict_accidents_gu,
    predict_kanishka,
    train_accidents_gu_from_df,
    train_kanishka_rain_model,
)


@st.cache_data(ttl=600, show_spinner="BCN: carregant Parquet i entrenant…")
def _cached_bcn_ml_ready_train(bucket: str, ml_ready_prefix: str) -> tuple[pd.DataFrame, dict[str, Any] | None]:
    """Llegeix el dataset ML-ready (materialitzat al job Spark d'exploitation-zone)."""
    df = normalize_columns(load_parquet_prefix(bucket, ml_ready_prefix))
    return df, train_accidents_gu_from_df(df)


@st.cache_data(ttl=600, show_spinner="Kanishka: entrenament…")
def _cached_kanishka_bundle(bucket: str, prefix: str) -> dict[str, Any] | None:
    return train_kanishka_rain_model(bucket, prefix)


def _pick_first_datetime_column(df: pd.DataFrame) -> str | None:
    for c in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[c]):
            return c
    for c in df.columns:
        s = pd.to_datetime(df[c], errors="coerce")
        if s.notna().mean() > 0.4:
            return c
    return None


def _pick_numeric_metric_columns(df: pd.DataFrame, skip: set[str], max_n: int = 4) -> list[str]:
    out: list[str] = []
    for c in df.columns:
        if c in skip:
            continue
        s = pd.to_numeric(df[c], errors="coerce")
        if s.notna().sum() >= max(5, int(len(df) * 0.2)):
            out.append(c)
        if len(out) >= max_n:
            break
    return out


def render_tomtom_section(bucket: str, prefix: str) -> None:
    st.subheader("TomTom — evolució")
    st.caption(f"Origen: `s3://{bucket}/{prefix.strip('/')}/`")
    df = load_parquet_prefix(bucket, prefix)
    df = normalize_columns(df)
    if df.empty:
        st.info("Sense dades TomTom en aquest prefix. Defineix `TOMTOM_ML_PREFIX` i assegura't que el pipeline hagi escrit el Parquet al bucket.")
        return

    tcol = _pick_first_datetime_column(df)
    work = df.copy()
    if tcol:
        work["_t"] = pd.to_datetime(work[tcol], errors="coerce")
    else:
        work["_t"] = pd.RangeIndex(len(work))

    skip = {"_t"}
    if tcol:
        skip.add(tcol)
    num_cols = _pick_numeric_metric_columns(work, skip=skip, max_n=12)
    if not num_cols:
        st.warning("No s'han trobat columnes numèriques adequades per al gràfic.")
        st.dataframe(work.head(20), use_container_width=True)
        return

    c1, c2 = st.columns(2)
    with c1:
        m1 = st.selectbox("Mètrica principal", options=num_cols, key="tt_m1")
    with c2:
        rest = [c for c in num_cols if c != m1]
        opts2 = ["— Només una mètrica —"] + rest
        m2_choice = st.selectbox("Segona mètrica (comparar)", options=opts2, key="tt_m2")
    pick = [m1] if m2_choice.startswith("—") else [m1, m2_choice]
    if not pick:
        return

    melt = work[["_t"] + pick].melt(id_vars=["_t"], var_name="metrica", value_name="valor")
    melt["valor"] = pd.to_numeric(melt["valor"], errors="coerce")
    melt = melt.dropna(subset=["valor"])
    if melt.empty:
        st.warning("Sense valors numèrics vàlids després de la conversió.")
        return

    x_enc: Any = alt.X("_t:T", title="Temps")
    if not pd.api.types.is_datetime64_any_dtype(melt["_t"]):
        x_enc = alt.X("_t:Q", title="Índex / temps")

    chart = (
        alt.Chart(melt)
        .mark_line(point=True, strokeWidth=2)
        .encode(
            x=x_enc,
            y=alt.Y("valor:Q", title="Valor"),
            color=alt.Color("metrica:N", title="Mètrica"),
            tooltip=["_t", "metrica", "valor"],
        )
        .properties(height=320, background="#0f172a", title="Evolució TomTom")
        .configure_axis(labelColor="#94a3b8", titleColor="#cbd5e1", gridColor="#334155")
        .configure_legend(labelColor="#cbd5e1", titleColor="#e2e8f0")
        .configure_title(color="#f1f5f9")
    )
    st.altair_chart(chart, use_container_width=True)


def render_bcn_zone_model_section(bucket: str, ml_ready_prefix: str) -> None:
    st.subheader("Ajuntament BCN — predicció per zona")
    st.caption(
        "Parquet **`ml_ready_accidents_traffic`** (generat al job d'**exploitation-zone**). "
        "Aquí només es llegeix el fitxer i s'entrena un classificador lleuger per a la interfície. "
        "Zona: **districte** o **barri**."
    )
    df_ml, bundle = _cached_bcn_ml_ready_train(bucket, ml_ready_prefix)

    if df_ml.empty:
        st.warning(
            "Sense dades al prefix ML-ready. Executa el job Spark `exploitation_zone.py` després de trusted; "
            "comprova `BCN_ML_READY_PREFIX` (per defecte `ajuntament_bcn/ml_ready_accidents_traffic/`)."
        )
        return

    st.caption(f"**{len(df_ml):,}** files al dataset carregat.")

    with st.expander("Mostra 5 files", expanded=False):
        st.dataframe(df_ml.head(5), use_container_width=True)

    if not bundle:
        st.warning(
            "No s'ha pogut entrenar el classificador (poc volum, objectiu no detectat o sense features). "
            "Comprova columnes `Numero_morts`, `Numero_lesionats_greus`, etc."
        )
        return

    m = bundle["metrics"]
    st.markdown(
        f"**Objectiu:** {bundle.get('target_description', '—')} · **Accuracy (test):** `{m.get('accuracy', 0):.3f}` · "
        f"**Train / test:** {m.get('n_train', 0)} / {m.get('n_test', 0)}"
    )
    with st.expander("Informe de classificació", expanded=False):
        st.text(m.get("report", ""))

    zone_col = None
    if "Nom_districte" in df_ml.columns:
        zone_col = "Nom_districte"
    elif "Nom_barri" in df_ml.columns:
        zone_col = "Nom_barri"

    inputs: dict[str, Any] = {}
    st.markdown("##### Predicció interactiva")
    with st.expander("Valors numèrics (edita o deixa les medianes)", expanded=False):
        cols = st.columns(min(3, max(1, len(bundle["numeric_columns"]))))
        idx = 0
        for c in bundle["numeric_columns"]:
            col = cols[idx % len(cols)]
            idx += 1
            default = float(bundle["defaults"].get(c, 0.0))
            inputs[c] = col.number_input(c, value=default, key=f"bcn_num_{c}")

    st.caption("Categories: tria cada camp al desplegable.")
    for c in bundle["categorical_columns"]:
        choices = bundle["categorical_choices"].get(c, [])
        default_v = str(df_ml[c].dropna().astype(str).mode().iloc[0]) if choices else ""
        if choices:
            inputs[c] = st.selectbox(c.replace("_", " "), options=choices, key=f"bcn_cat_{c}")
        else:
            inputs[c] = st.text_input(c.replace("_", " "), value=default_v, key=f"bcn_txt_{c}")

    if zone_col and zone_col not in bundle.get("categorical_columns", []):
        zones = sorted(df_ml[zone_col].dropna().astype(str).unique().tolist())[:100]
        if zones:
            z = st.selectbox("Zona (districte o barri)", zones, key="bcn_zone_pick")
            inputs[zone_col] = z

    if st.button("Calcular probabilitat / classe", key="bcn_predict_btn"):
        if zone_col and zone_col in bundle["feature_columns"] and zone_col not in inputs:
            zm = df_ml[zone_col].dropna().astype(str)
            inputs[zone_col] = zm.mode().iloc[0] if not zm.empty else ""
        pred = predict_accidents_gu(bundle, inputs)
        st.success(f"Predicció (classe 0/1 greu o morts): **{int(pred[0])}**")


def render_traffic_cameras_compact(bucket: str, data_prefix: str) -> None:
    st.subheader("Càmeres de trànsit")
    from data_access import traffic_cameras_gallery_prefix

    st.caption(f"Galeria: `s3://{bucket}/{traffic_cameras_gallery_prefix(data_prefix)}`")
    sites = list_traffic_camera_gallery_sites(bucket, data_prefix)
    if not sites:
        st.info("Encara no hi ha imatges a la galeria.")
        return
    c1, c2 = st.columns([1, 2])
    with c1:
        site = st.selectbox("Ubicació", sites, key="uni_cam_site")
        items = list_traffic_camera_gallery_objects(bucket, data_prefix, site)
        if not items:
            st.warning("Sense fitxers.")
            return
        names = [n for _k, n in items]
        choice = st.selectbox("Captura", names, key="uni_cam_file")
        key = next(k for k, n in items if n == choice)
    with c2:
        try:
            body = get_s3_object_bytes(bucket, key)
            st.image(body, caption=choice, use_container_width=True)
        except Exception as ex:
            st.error(str(ex))


def render_kanishka_predict_section(bucket: str, prefix: str) -> None:
    st.subheader("Kanishka (HuggingFace) — predicció de pluja")
    bundle = _cached_kanishka_bundle(bucket, prefix)
    if not bundle:
        st.info("Sense dades o columnes esperades (`Rain`, temperatura, humitat…).")
        return
    st.caption(bundle.get("target_description", ""))
    st.caption(f"R² test: `{bundle['metrics'].get('r2', 0):.3f}` · MAE: `{bundle['metrics'].get('mae', 0):.4f}`")
    mode = st.selectbox(
        "Entrada per a la predicció",
        ("Valors per defecte (mediana del dataset)", "Ajustar manualment"),
        key="kan_mode",
    )
    row: dict[str, float] = {}
    if mode.startswith("Valors per defecte"):
        for c in bundle["numeric_columns"]:
            row[c] = float(bundle["defaults"].get(c, 0.0))
        st.caption("S'estan usant les medianes del Parquet carregat.")
    else:
        ncols = st.columns(min(3, max(1, len(bundle["numeric_columns"]))))
        for i, c in enumerate(bundle["numeric_columns"]):
            d = float(bundle["defaults"].get(c, 0.0))
            row[c] = ncols[i % len(ncols)].number_input(c.replace("_", " "), value=d, format="%.4f", key=f"kan_{c}")
    if st.button("Predir Rain", key="kan_btn"):
        val = float(predict_kanishka(bundle, row)[0])
        st.success(f"Rain predit: **{val:.4f}**")


def render_aemet_hourly_chart(bucket: str, prefix: str) -> None:
    st.subheader("AEMET — prediccions per hora")
    df = normalize_columns(load_parquet_prefix(bucket, prefix))
    if df.empty:
        st.info("Sense Parquet AEMET.")
        return

    hora_c = None
    for cand in ("hora_periodo", "hora", "periodo", "hora_period"):
        if cand in df.columns:
            hora_c = cand
            break
    if not hora_c:
        st.warning("No hi ha columna d'hora (`hora_periodo`, etc.). Es mostra resum numèric.")
        from analytics import render_parquet_analysis

        render_parquet_analysis(df, "aemet_uni")
        return

    temp_c = None
    for cand in ("temperatura", "temperatura_value", "temp"):
        if cand in df.columns:
            temp_c = cand
            break
    prec_c = None
    for cand in ("precipitacion", "precipitació", "prob_precipitacion"):
        if cand in df.columns:
            prec_c = cand
            break

    work = df.copy()
    work["_h"] = pd.to_numeric(work[hora_c], errors="coerce")
    work = work[work["_h"].between(0, 23, inclusive="both")]

    metrics: list[tuple[str, str]] = []
    if temp_c:
        metrics.append((temp_c, "Temperatura"))
    if prec_c:
        metrics.append((prec_c, "Precipitació / prob."))

    if not metrics:
        st.dataframe(work.head(30), use_container_width=True)
        return

    frames = []
    for col, label in metrics:
        sub = work[["_h", col]].copy()
        sub[col] = pd.to_numeric(sub[col], errors="coerce")
        sub = sub.dropna()
        if sub.empty:
            continue
        g = sub.groupby("_h", as_index=False)[col].mean()
        g["metrica"] = label
        g = g.rename(columns={col: "valor"})
        frames.append(g)
    if not frames:
        st.warning("Sense valors numèrics per hora.")
        return

    plot_df = pd.concat(frames, ignore_index=True)
    chart = (
        alt.Chart(plot_df)
        .mark_line(point=True)
        .encode(
            x=alt.X("_h:O", title="Hora (0–23)"),
            y=alt.Y("valor:Q", title="Valor mitjà"),
            color=alt.Color("metrica:N", title=""),
            tooltip=["_h", "metrica", "valor"],
        )
        .properties(height=280, background="#0f172a", title="Mitjana per hora (totes les files carregades)")
        .configure_axis(labelColor="#94a3b8", titleColor="#cbd5e1", gridColor="#334155")
        .configure_legend(labelColor="#cbd5e1")
        .configure_title(color="#f1f5f9")
    )
    st.altair_chart(chart, use_container_width=True)


def _parse_news_datetime(row: pd.Series, col: str | None) -> datetime | None:
    if not col or col not in row.index:
        return None
    v = row.get(col)
    if pd.isna(v):
        return None
    ts = pd.to_datetime(v, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.to_pydatetime()


def _sct_news_body(row: pd.Series) -> str:
    parts = []
    for c in ("descripcio", "descripcio_tipus", "carretera", "tipus", "subtipus", "causa"):
        if c in row.index and pd.notna(row[c]) and str(row[c]).strip():
            parts.append(str(row[c]).strip())
    return " · ".join(parts) if parts else "(sense text)"


def render_news_feed_section(bucket: str, twitter_prefix: str, sct_prefix: str) -> None:
    st.subheader("Notícies — trànsit (SCT) i Twitter")

    items: list[dict[str, Any]] = []

    df_tw = normalize_columns(load_parquet_prefix(bucket, twitter_prefix))
    if not df_tw.empty:
        from analytics import resolve_tweet_display_columns

        m = resolve_tweet_display_columns(df_tw)
        text_c, author_c, date_c, link_c = m["text"], m["author"], m["date"], m["link"]
        if text_c:
            for _, row in df_tw.iterrows():
                ts = _parse_news_datetime(row, date_c)
                text = str(row.get(text_c, "")) if pd.notna(row.get(text_c)) else ""
                author = f"@{row.get(author_c)}" if author_c and pd.notna(row.get(author_c)) else "Twitter"
                link = str(row.get(link_c)) if link_c and pd.notna(row.get(link_c)) else ""
                items.append(
                    {
                        "ts": ts,
                        "tipo": "twitter",
                        "titulo": author[:120],
                        "cuerpo": text,
                        "url": link,
                    }
                )

    df_sct = normalize_columns(load_parquet_prefix(bucket, sct_prefix))
    if not df_sct.empty:
        date_c = None
        for cand in ("data", "timestamp", "published", "created_at"):
            if cand in df_sct.columns:
                date_c = cand
                break
        for _, row in df_sct.iterrows():
            ts = _parse_news_datetime(row, date_c)
            ident = row.get("identificador", "") if "identificador" in row.index else ""
            tit = f"SCT · {ident}" if pd.notna(ident) and str(ident) else "SCT trànsit"
            items.append(
                {
                    "ts": ts,
                    "tipo": "transit",
                    "titulo": str(tit)[:160],
                    "cuerpo": _sct_news_body(row),
                    "url": "",
                }
            )

    if not items:
        st.info("Cap font amb dades (Twitter o SCT). Revisa `TRANSIT_POSTS_PREFIX` i `SCT_TRAFFIC_INCIDENTS_PREFIX`.")
        return

    items.sort(key=lambda x: (x["ts"] is not None, x["ts"] or datetime.min), reverse=True)

    f1, f2 = st.columns(2)
    with f1:
        filtre = st.selectbox(
            "Font",
            ("Totes", "Només Twitter", "Només trànsit (SCT)"),
            key="news_font",
        )
    with f2:
        q_mode = st.selectbox(
            "Cerca de text",
            ("Sense filtre", "Filtrar per paraules"),
            key="news_qmode",
        )
    q = ""
    if q_mode.startswith("Filtrar"):
        q = st.text_input("Paraules clau", placeholder="accident, obres, retenció…", key="news_q")

    if filtre == "Només Twitter":
        items = [it for it in items if it["tipo"] == "twitter"]
    elif filtre == "Només trànsit (SCT)":
        items = [it for it in items if it["tipo"] == "transit"]

    if q and q.strip():
        ql = q.strip().lower()
        items = [it for it in items if ql in it["cuerpo"].lower() or ql in it["titulo"].lower()]

    n_tot = len(items)
    if n_tot == 0:
        st.warning("Cap notícia amb aquests filtres.")
        return

    c_a, c_b = st.columns([1, 1])
    with c_a:
        ventana = st.selectbox(
            "Notícies a la caixa",
            (3, 5, 8, 12),
            index=1,
            format_func=lambda x: f"{x} notícies",
            key="news_win",
        )
    max_start = max(0, n_tot - ventana)
    with c_b:
        start = st.slider(
            "Desplaçar (posició a la llista)",
            min_value=0,
            max_value=max_start,
            value=0,
            key="news_slide",
        )

    visible = items[start : start + ventana]
    st.caption(
        f"**{n_tot}** amb els filtres actuals · mostrant **{start + 1}–{start + len(visible)}** · desplaça amb el slider."
    )

    parts: list[str] = ['<div class="news-scroll-box">']
    for it in visible:
        fecha = it["ts"].strftime("%Y-%m-%d %H:%M") if it["ts"] else "—"
        badge = "🐦 Twitter" if it["tipo"] == "twitter" else "🚧 Trànsit"
        safe_title = html.escape(it["titulo"])
        safe_body = html.escape(it["cuerpo"][:2000])
        link_html = ""
        if it.get("url"):
            link_html = f'<a href="{html.escape(it["url"])}" target="_blank" rel="noopener" style="color:#5eead4;">Enllaç ↗</a>'
        parts.append(
            f"""
            <div class="tweet-card-wrap">
              <div class="tweet-card">
                <div class="tweet-body">
                  <div class="tweet-meta">
                    <span class="tweet-author">{badge}</span>
                    <span class="tweet-dot">·</span>
                    <span class="tweet-date">{html.escape(fecha)}</span>
                    <span style="margin-left:auto;">{link_html}</span>
                  </div>
                  <div style="font-weight:600;color:#f8fafc;margin-bottom:0.35rem;">{safe_title}</div>
                  <div class="tweet-text">{safe_body}</div>
                </div>
              </div>
            </div>
            """
        )
    parts.append("</div>")
    st.markdown("".join(parts), unsafe_allow_html=True)


def render_unified_dashboard(
    bucket: str,
    data_prefix: str,
    *,
    tomtom_prefix: str,
    bcn_ml_ready_prefix: str,
    aemet_prefix: str,
    hf_prefix: str,
    twitter_prefix: str,
    sct_prefix: str,
) -> None:
    """Bloc principal del dashboard (una columna vertical de seccions)."""
    with st.container(border=True):
        render_tomtom_section(bucket, tomtom_prefix)

    c_a, c_b = st.columns(2, gap="large")
    with c_a:
        with st.container(border=True):
            render_bcn_zone_model_section(bucket, bcn_ml_ready_prefix)
    with c_b:
        with st.container(border=True):
            render_traffic_cameras_compact(bucket, data_prefix)

    c_m1, c_m2 = st.columns(2, gap="large")
    with c_m1:
        with st.container(border=True):
            render_kanishka_predict_section(bucket, hf_prefix)
    with c_m2:
        with st.container(border=True):
            render_aemet_hourly_chart(bucket, aemet_prefix)

    with st.container(border=True):
        render_news_feed_section(bucket, twitter_prefix, sct_prefix)
