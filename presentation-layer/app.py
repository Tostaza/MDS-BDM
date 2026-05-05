"""
Dashboard capa presentación: datos en exploitation-zone (S3), modelos ML sencillos
(RandomForest) y ChromaDB para similaridad Twitter.

Uso (Docker): http://localhost:8501 tras `docker compose up dashboard`.
"""

from __future__ import annotations

import os

import chromadb
import streamlit as st

from data_access import (
    get_s3_object_bytes,
    list_traffic_camera_gallery_objects,
    list_traffic_camera_gallery_sites,
    load_parquet_prefix,
    traffic_cameras_gallery_prefix,
)
from ml_models import (
    load_aemet_predictions_table,
    predict_accidents_gu,
    predict_aemet,
    predict_kanishka,
    train_accidents_gu_model,
    train_aemet_model,
    train_kaggle_accidents_model,
    train_kanishka_rain_model,
)


def _env(name: str, default: str) -> str:
    v = os.environ.get(name)
    return v if v is not None and v != "" else default


@st.cache_resource
def chroma_client():
    host = _env("CHROMA_HOST", "chromadb")
    port = int(_env("CHROMA_PORT", "8000"))
    return chromadb.HttpClient(host=host, port=port)


@st.cache_data(ttl=300, show_spinner="Entrenando modelo (accidents GU)…")
def cached_train_accidents_gu(bucket: str, prefix: str):
    return train_accidents_gu_model(bucket, prefix)


@st.cache_data(ttl=300, show_spinner="Entrenando modelo (AEMET)…")
def cached_train_aemet(bucket: str, prefix: str):
    return train_aemet_model(bucket, prefix)


@st.cache_data(ttl=300, show_spinner="Entrenando modelo (Kanishka / lluvia)…")
def cached_train_kanishka(bucket: str, prefix: str):
    return train_kanishka_rain_model(bucket, prefix)


@st.cache_data(ttl=300, show_spinner="Entrenando modelo (Kaggle)…")
def cached_train_kaggle(bucket: str, prefix: str):
    return train_kaggle_accidents_model(bucket, prefix)


def _render_prediction_form_regression(bundle: dict, key_prefix: str) -> dict[str, float]:
    inputs: dict[str, float] = {}
    for col in bundle["feature_columns"]:
        med = float(bundle.get("defaults", {}).get(col, 0.0))
        inputs[col] = st.number_input(f"`{col}`", value=med, key=f"{key_prefix}_{col}")
    return inputs


def _render_prediction_form_classification(bundle: dict, key_prefix: str) -> dict[str, float | str]:
    inputs: dict[str, float | str] = {}
    choices = bundle.get("categorical_choices", {})
    for col in bundle["feature_columns"]:
        if col in bundle.get("categorical_columns", []):
            opts = choices.get(col) or [""]
            inputs[col] = st.selectbox(f"`{col}` (categórica)", opts, key=f"{key_prefix}_{col}")
        else:
            med = float(bundle.get("defaults", {}).get(col, 0.0))
            inputs[col] = st.number_input(f"`{col}`", value=med, key=f"{key_prefix}_{col}")
    return inputs


def tab_accidents_gu(bucket: str, prefix: str) -> None:
    st.markdown(
        "Datos **accidents_gu** (Ajuntament de Barcelona) en exploitation. "
        "Modelo de **clasificación**: predice si el accidente tiene **víctimas graves o fallecidos** "
        "cuando existen columnas como `Victimes greus` o `Numero_morts` en el Parquet."
    )
    bundle = cached_train_accidents_gu(bucket, prefix)
    if bundle is None:
        st.warning(
            f"No se pudo entrenar (sin datos o sin columnas de objetivo en "
            f"`s3://{bucket}/{prefix.strip('/')}/`). Ejecuta el pipeline trusted → exploitation."
        )
        return
    m = bundle["metrics"]
    st.success(
        f"**Accuracy** (holdout): {m['accuracy']:.3f} · train={m['n_train']} · test={m['n_test']} · objetivo: {m['target']}"
    )
    with st.expander("Informe de clasificación"):
        st.text(m["report"])

    st.subheader("Predicción con valores de entrada")
    inputs = _render_prediction_form_classification(bundle, "gu")
    if st.button("Predecir (accidents GU)", key="pred_gu"):
        p = predict_accidents_gu(bundle, inputs)[0]
        st.info(f"Clase predicha: **{int(p)}** (0 = sin víctimas graves/muertos según umbral, 1 = con)")


def tab_aemet(bucket: str, prefix: str) -> None:
    st.markdown(
        "**AEMET**: primero se muestran las **predicciones almacenadas** en exploitation; "
        "debajo, un modelo de **regresión** sobre columnas numéricas (p. ej. temperatura) si hay datos tabulares suficientes."
    )
    df_raw = load_aemet_predictions_table(bucket, prefix)
    if df_raw.empty:
        st.warning(f"Sin Parquet en `s3://{bucket}/{prefix.strip('/')}/`.")
    else:
        st.write(f"Filas cargadas: **{len(df_raw)}** · columnas: {list(df_raw.columns)[:15]}…")
        st.dataframe(df_raw.head(200), use_container_width=True, height=280)

    bundle = cached_train_aemet(bucket, prefix)
    if bundle is None:
        st.info(
            "No se entrenó modelo auxiliar (pocos numéricos o estructura distinta). "
            "Los JSON horarios a veces requieren aplanado adicional en el ETL."
        )
        return
    m = bundle["metrics"]
    st.success(f"**R²**={m['r2']:.3f} · **MAE**={m['mae']:.3f} · objetivo: `{m['target']}`")
    st.subheader("Predicción (regresión AEMET)")
    inputs = _render_prediction_form_regression(bundle, "aemet")
    if st.button("Predecir (AEMET)", key="pred_aemet"):
        val = predict_aemet(bundle, inputs)[0]
        st.info(f"Valor predicho para `{m['target']}`: **{val:.4f}**")


def tab_kanishka(bucket: str, prefix: str) -> None:
    st.markdown(
        "Dataset **HuggingFace** `kanishka089/RainPrediction` (ruta típica `huggingface/kanishka089/`). "
        "Regresión: predice **Rain** a partir de temperatura, humedad, etc."
    )
    bundle = cached_train_kanishka(bucket, prefix)
    if bundle is None:
        st.warning("Sin datos o sin columnas `Rain` / meteorológicas en exploitation.")
        return
    m = bundle["metrics"]
    st.success(f"**R²**={m['r2']:.3f} · **MAE**={m['mae']:.3f}")
    st.subheader("Predicción de lluvia (Rain)")
    inputs = _render_prediction_form_regression(bundle, "kanishka")
    if st.button("Predecir (Kanishka)", key="pred_kan"):
        val = predict_kanishka(bundle, inputs)[0]
        st.info(f"**Rain** predicho: **{val:.4f}**")


def tab_traffic_cameras(bucket: str, data_prefix: str) -> None:
    st.markdown(
        "Galería **traffic_cameras** en exploitation (`traffic_cameras/gallery/{ubicación}/…`). "
        "Origen: imágenes en persistent → **trusted** (Pillow: brillo / nitidez) → **exploitation** "
        "(orden por sitio). Requiere haber ejecutado `trusted_zone.py` y `exploitation_zone.py`."
    )
    gprefix = traffic_cameras_gallery_prefix(data_prefix)
    st.caption(f"Ruta listada: `s3://{bucket}/{gprefix}` · `DATA_PREFIX={data_prefix!r}`")

    sites = list_traffic_camera_gallery_sites(bucket, data_prefix)
    if not sites:
        st.info(
            "No hay carpetas de galería. Comprueba que existan claves bajo el prefijo anterior, "
            "que `EXPLOITATION_BUCKET` y `DATA_PREFIX` coincidan con el pipeline, y que SeaweedFS tenga datos."
        )
        return

    site = st.selectbox("Ubicación / cámara (site_key)", sites, key="traffic_cam_site")
    items = list_traffic_camera_gallery_objects(bucket, data_prefix, site)
    if not items:
        st.warning("Esta carpeta no tiene imágenes listadas.")
        return

    names = [n for _k, n in items]
    choice = st.selectbox("Captura (fichero)", names, key="traffic_cam_file")
    key = next(k for k, n in items if n == choice)

    try:
        body = get_s3_object_bytes(bucket, key)
    except Exception as ex:
        st.error(f"No se pudo leer `s3://{bucket}/{key}`: {ex}")
        return

    st.caption(f"Clave S3: `{key}`")
    try:
        st.image(body, caption=choice, use_container_width=True)
    except Exception:
        import io

        from PIL import Image

        st.image(Image.open(io.BytesIO(body)), caption=choice, use_container_width=True)


def tab_kaggle(bucket: str, prefix: str) -> None:
    st.markdown(
        "Parquet de **Kaggle** (accidentes u otro CSV ingerido al mismo bucket). "
        "Configura el prefijo con la variable de entorno `KAGGLE_ML_PREFIX` si tus ficheros "
        "no están en la ruta por defecto."
    )
    if not prefix.strip():
        st.warning("Prefijo Kaggle vacío: define `KAGGLE_ML_PREFIX` en el entorno del contenedor `dashboard`.")
        return
    bundle = cached_train_kaggle(bucket, prefix)
    if bundle is None:
        st.warning("No se pudo entrenar (misma lógica que accidents GU: hace falta columna de víctimas graves/muertos).")
        return
    m = bundle["metrics"]
    st.success(f"**Accuracy**: {m['accuracy']:.3f} · objetivo: {m['target']}")
    with st.expander("Informe"):
        st.text(m["report"])
    inputs = _render_prediction_form_classification(bundle, "kaggle")
    if st.button("Predecir (Kaggle)", key="pred_kaggle"):
        p = predict_accidents_gu(bundle, inputs)[0]
        st.info(f"Clase predicha: **{int(p)}**")


def main() -> None:
    st.set_page_config(page_title="BDM — Dashboard", layout="wide")
    st.title("BDM — Dashboard (exploitation-zone + ML)")
    st.caption(
        "Parquet en **exploitation-zone** (S3/SeaweedFS). Modelos **RandomForest** (scikit-learn) "
        "solo para demostración; no sustituyen análisis operativos."
    )

    exploitation_bucket = _env("EXPLOITATION_BUCKET", "exploitation-zone")
    default_sim = _env("SIMILARITY_OUTPUT_PREFIX", "twitter_similarity_pairs")
    pfx_acc = _env("ACCIDENTS_GU_ML_PREFIX", "ajuntament_bcn/accidents_gu/")
    pfx_aemet = _env("AEMET_ML_PREFIX", "aemet/")
    pfx_hf = _env("HF_RAIN_ML_PREFIX", "huggingface/kanishka089/")
    pfx_kaggle = _env("KAGGLE_ML_PREFIX", "")
    data_prefix = _env("DATA_PREFIX", "")

    tab_ml1, tab_ml2, tab_ml3, tab_ml4, tab_cam, tab_sim, tab_expl, tab_chr = st.tabs(
        [
            "ML · Accidents GU",
            "ML · AEMET",
            "ML · Kanishka (HF)",
            "ML · Kaggle",
            "Cámaras tráfico",
            "Similaridad Twitter",
            "Explorador Parquet",
            "ChromaDB",
        ]
    )

    with tab_ml1:
        tab_accidents_gu(exploitation_bucket, pfx_acc)
    with tab_ml2:
        tab_aemet(exploitation_bucket, pfx_aemet)
    with tab_ml3:
        tab_kanishka(exploitation_bucket, pfx_hf)
    with tab_ml4:
        tab_kaggle(exploitation_bucket, pfx_kaggle)

    with tab_cam:
        tab_traffic_cameras(exploitation_bucket, data_prefix)

    with tab_sim:
        st.subheader("Pares de mensajes similares (≥ umbral en pipeline)")
        st.write(
            "Salida de `exploitation-zone/twitter_similarity.py` "
            "(fichero `similarity_edges.parquet` bajo el prefijo configurado)."
        )
        thr = st.slider("Umbral mínimo para filtrar en vista", 0.5, 1.0, 0.8, 0.05)
        df_sim = load_parquet_prefix(exploitation_bucket, default_sim)
        if df_sim.empty:
            st.info(
                "No hay datos en "
                f"`s3://{exploitation_bucket}/{default_sim}/`. "
                "Ejecuta antes el pipeline de embeddings y el script de similaridad."
            )
        else:
            if "similarity" in df_sim.columns:
                df_sim = df_sim[df_sim["similarity"] >= thr]
            st.dataframe(df_sim, use_container_width=True, height=400)

    with tab_expl:
        st.subheader("Vista genérica de un prefijo Parquet en exploitation")
        prefix = st.text_input(
            "Prefijo dentro del bucket (sin s3:// ni nombre de bucket)",
            value="",
            placeholder="ej: mi_proyecto/reportes/",
        )
        if st.button("Cargar muestra"):
            df = load_parquet_prefix(exploitation_bucket, prefix)
            if df.empty:
                st.warning("Sin tablas Parquet legibles en esa ruta.")
            else:
                st.write(f"Muestra: **{len(df)}** filas.")
                st.dataframe(df.head(500), use_container_width=True, height=400)

    with tab_chr:
        st.subheader("Colecciones Chroma")
        try:
            client = chroma_client()
            cols = client.list_collections()
            st.write(f"Colecciones encontradas: **{len(cols)}**")
            for c in cols:
                st.write(f"- **{c.name}** (id={c.id})")
                coll = client.get_collection(c.name)
                st.caption(f"  documentos indexados: {coll.count()}")
        except Exception as ex:
            st.error(f"No se pudo conectar a Chroma ({_env('CHROMA_HOST', 'chromadb')}): {ex}")


if __name__ == "__main__":
    main()
