"""
Análisis exploratorio de tablas Parquet (sin modelos de predicción).

Resúmenes numéricos (media, mediana, min, max, desv. típica), histogramas y barras de frecuencia
para columnas categóricas de baja cardinalidad. Pensado para accidentes, lluvia (Kanishka), AEMET, etc.
"""

from __future__ import annotations

import html
from typing import Any

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

from ml_models import normalize_columns


def infer_numeric_columns(df: pd.DataFrame) -> list[str]:
    """Columnas numéricas o mayormente convertibles a número (p. ej. strings con dígitos)."""
    out: list[str] = []
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c]):
            out.append(c)
            continue
        s = pd.to_numeric(df[c], errors="coerce")
        if s.notna().mean() > 0.5 and s.notna().sum() >= 3:
            out.append(c)
    return out


def categorical_barplot_columns(df: pd.DataFrame, max_unique: int = 24, min_unique: int = 2) -> list[str]:
    """Columnas objeto/categoría con pocos valores distintos (adecuadas para barplot)."""
    numeric_like = set(infer_numeric_columns(df))
    out: list[str] = []
    for c in df.columns:
        if c in numeric_like:
            continue
        s = df[c].astype(str).replace({"nan": np.nan, "None": np.nan})
        n = s.nunique(dropna=True)
        if min_unique <= n <= max_unique:
            out.append(c)
    return sorted(out, key=lambda x: df[x].nunique(dropna=True))


def numeric_summary_table(df: pd.DataFrame, numeric_cols: list[str]) -> pd.DataFrame:
    """Tabla resumen: n válidos, media, mediana, desv. típica, mínimo, máximo por columna numérica."""
    rows: list[dict[str, Any]] = []
    for c in numeric_cols:
        s = pd.to_numeric(df[c], errors="coerce").dropna()
        if s.empty:
            continue
        rows.append(
            {
                "columna": c,
                "n_válidos": int(s.shape[0]),
                "media": float(s.mean()),
                "mediana": float(s.median()),
                "desv._típica": float(s.std(ddof=0)) if len(s) > 1 else 0.0,
                "mínimo": float(s.min()),
                "máximo": float(s.max()),
            }
        )
    return pd.DataFrame(rows)


def chart_histogram(df: pd.DataFrame, col: str, maxbins: int = 35) -> alt.Chart:
    d = pd.DataFrame({col: pd.to_numeric(df[col], errors="coerce")}).dropna()
    if d.empty:
        return alt.Chart(pd.DataFrame({col: [0]})).mark_text(text="Sin datos numéricos").encode()
    return (
        alt.Chart(d)
        .mark_bar(color="#2dd4bf", opacity=0.85)
        .encode(
            alt.X(col, bin=alt.Bin(maxbins=maxbins), title=col),
            alt.Y("count()", title="Frecuencia"),
            tooltip=[
                alt.Tooltip(col, bin=True, title="intervalo"),
                alt.Tooltip("count()", title="n"),
            ],
        )
        .properties(height=240, title=f"Histograma · {col}", background="#0f172a")
        .configure_axis(labelColor="#94a3b8", titleColor="#cbd5e1", gridColor="#334155")
        .configure_title(color="#f1f5f9", fontSize=16)
    )


def chart_bar_top(df: pd.DataFrame, col: str, top_n: int = 18) -> alt.Chart:
    vc = df[col].astype(str).replace({"nan": "(vacío)", "None": "(vacío)"}).value_counts().head(top_n)
    plot_df = vc.reset_index()
    plot_df.columns = ["categoría", "frecuencia"]
    return (
        alt.Chart(plot_df)
        .mark_bar(color="#818cf8", opacity=0.9)
        .encode(
            x=alt.X("frecuencia:Q", title="Frecuencia"),
            y=alt.Y("categoría:N", sort="-x", title=col),
            tooltip=["categoría", "frecuencia"],
        )
        .properties(height=min(320, 24 * len(plot_df) + 80), title=f"Frecuencias (top {top_n}) · {col}", background="#0f172a")
        .configure_axis(labelColor="#94a3b8", titleColor="#cbd5e1", gridColor="#334155")
        .configure_title(color="#f1f5f9", fontSize=16)
    )


def render_parquet_analysis(df: pd.DataFrame, key_prefix: str, max_histograms: int = 6, max_barplots: int = 4) -> None:
    """Muestra resumen estadístico, histogramas y barplots en Streamlit."""
    if df.empty:
        st.warning("DataFrame vacío: no hay filas que analizar.")
        return

    df = normalize_columns(df)
    n, p = df.shape
    st.markdown(f"**{n:,}** filas · **{p}** columnas")

    num_cols = infer_numeric_columns(df)
    cat_cols = categorical_barplot_columns(df)

    if num_cols:
        summ = numeric_summary_table(df, num_cols)
        st.markdown("##### Estadísticos por variable numérica")
        if summ.empty:
            st.caption("No se pudo calcular el resumen numérico.")
        else:
            fmt = {
                "media": "{:.4g}",
                "mediana": "{:.4g}",
                "desv._típica": "{:.4g}",
                "mínimo": "{:.4g}",
                "máximo": "{:.4g}",
            }
            try:
                st.dataframe(
                    summ.style.format(fmt, na_rep="—"),
                    use_container_width=True,
                    height=min(420, 42 + 28 * len(summ)),
                )
            except Exception:
                st.dataframe(summ.round(6), use_container_width=True, height=min(420, 42 + 28 * len(summ)))

        st.markdown("##### Histogramas (distribución)")
        pick_h = st.multiselect(
            "Columnas para histograma",
            options=num_cols,
            default=num_cols[: min(max_histograms, len(num_cols))],
            key=f"{key_prefix}_hist_cols",
        )
        for col in pick_h[:max_histograms]:
            st.altair_chart(chart_histogram(df, col), use_container_width=True)
    else:
        st.info("No se detectaron columnas numéricas útiles para histogramas.")

    if cat_cols:
        st.markdown("##### Barras de frecuencia (categorías)")
        pick_c = st.multiselect(
            "Columnas categóricas (baja cardinalidad)",
            options=cat_cols,
            default=cat_cols[: min(max_barplots, len(cat_cols))],
            key=f"{key_prefix}_bar_cols",
        )
        for col in pick_c[:max_barplots]:
            st.altair_chart(chart_bar_top(df, col), use_container_width=True)
    else:
        st.caption("No hay columnas categóricas con cardinalidad adecuada para barras (2–24 valores).")


def _pick_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lower_map = {str(c).lower().replace(" ", "_"): c for c in df.columns}
    for cand in candidates:
        k = cand.lower().replace(" ", "_")
        if k in lower_map:
            return lower_map[k]
        for lk, orig in lower_map.items():
            if lk.endswith(k) or k in lk:
                return orig
    return None


def resolve_tweet_display_columns(df: pd.DataFrame) -> dict[str, str | None]:
    """Mapea columnas típicas del pipeline ``transit_posts`` a texto, autor, fecha y enlace."""
    text = _pick_column(
        df,
        [
            "summary_text_plain",
            "summary",
            "content",
            "text",
            "body",
            "message",
            "title",
        ],
    )
    author = _pick_column(df, ["author_resolved", "author", "user", "username", "screen_name"])
    date_col = _pick_column(df, ["published_ts", "published", "date", "created_at", "timestamp"])
    link_col = _pick_column(df, ["link", "url", "permalink"])
    return {"text": text, "author": author, "date": date_col, "link": link_col}


def render_tweet_feed(df: pd.DataFrame, key_prefix: str, max_rows_cap: int = 15_000) -> None:
    """
    Vista tipo timeline: tarjetas con autor, fecha, texto y enlace.
    Paginación en memoria (todos los tweets cargados hasta ``max_rows_cap``).
    """
    if df.empty:
        st.warning("Sin tweets en este Parquet.")
        return

    df = normalize_columns(df)
    if len(df) > max_rows_cap:
        st.warning(
            f"El dataset tiene **{len(df):,}** filas; para la vista se usan las primeras **{max_rows_cap:,}** "
            "(rendimiento del dashboard). Exporta desde el pipeline si necesitas el total completo."
        )
        df = df.iloc[:max_rows_cap].copy()

    cols = resolve_tweet_display_columns(df)
    if not cols.get("text"):
        st.error(
            "No se encontró una columna de texto reconocible "
            "(p. ej. `summary_text_plain`, `summary`, `content`). "
            f"Columnas disponibles: `{list(df.columns)[:40]}`…"
        )
        st.dataframe(df.head(30), use_container_width=True)
        return

    text_c = cols["text"]
    author_c = cols["author"]
    date_c = cols["date"]
    link_c = cols["link"]

    order = st.radio(
        "Orden",
        ("Más recientes primero", "Más antiguos primero", "Orden del fichero"),
        horizontal=True,
        key=f"{key_prefix}_tweet_order",
    )
    work = df.copy()
    if date_c and date_c in work.columns and order != "Orden del fichero":
        work["_ts"] = pd.to_datetime(work[date_c], errors="coerce")
        ascending = order == "Más antiguos primero"
        work = work.sort_values("_ts", ascending=ascending, na_position="last")
    per_page = st.select_slider("Tweets por página", options=[10, 20, 40, 60, 100], value=20, key=f"{key_prefix}_per_page")
    n_pages = max(1, (len(work) + per_page - 1) // per_page)
    page = st.number_input("Página", min_value=1, max_value=n_pages, value=1, step=1, key=f"{key_prefix}_tweet_page")
    start = (page - 1) * per_page
    slice_df = work.iloc[start : start + per_page]

    st.caption(f"Mostrando filas **{start + 1}–{min(start + per_page, len(work))}** de **{len(work):,}**")

    for _, row in slice_df.iterrows():
        raw_text = row.get(text_c, "")
        text_str = "" if pd.isna(raw_text) else str(raw_text)
        if len(text_str) > 2800:
            text_str = text_str[:2800] + "…"
        safe_text = html.escape(text_str).replace("\n", "<br/>")

        author_display = "—"
        if author_c and author_c in row.index:
            a = row.get(author_c)
            if pd.notna(a) and str(a).strip():
                au_raw = str(a).strip()
                author_display = html.escape(au_raw) if au_raw.startswith("@") else "@" + html.escape(au_raw)

        date_s = ""
        if date_c and date_c in row.index:
            d = row.get(date_c)
            if pd.notna(d):
                date_s = html.escape(str(d)[:32])

        link_html = ""
        if link_c and link_c in row.index:
            u = row.get(link_c)
            if pd.notna(u) and str(u).strip():
                url = html.escape(str(u).strip())
                link_html = f'<a href="{url}" target="_blank" rel="noopener" style="color:#5eead4;font-size:0.85rem;">Enlace ↗</a>'

        st.markdown(
            f"""
            <div class="tweet-card-wrap">
              <div class="tweet-card">
                <div class="tweet-avatar" aria-hidden="true"></div>
                <div class="tweet-body">
                  <div class="tweet-meta">
                    <span class="tweet-author">{author_display}</span>
                    <span class="tweet-dot">·</span>
                    <span class="tweet-date">{date_s}</span>
                    <span style="margin-left:auto;">{link_html}</span>
                  </div>
                  <div class="tweet-text">{safe_text}</div>
                </div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
