"""
Modelos sklearn entrenados **en memoria** a partir de Parquet ya cargados (vía ``data_access``).

Sirven para formularios de predicción en Streamlit. No materializan tablas ni hacen joins;
el pipeline Spark que prepara los datasets está en ``exploitation-zone/``.
"""

from __future__ import annotations

import json
import re
from typing import Any

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score, classification_report, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder

from data_access import load_parquet_prefix


def _norm_col(c: str) -> str:
    s = str(c).strip()
    if s.startswith("\ufeff"):
        s = s[1:]
    return s


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [_norm_col(c) for c in out.columns]
    return out


def _pick_accidents_target(df: pd.DataFrame) -> tuple[pd.Series | None, str | None, str]:
    """
    Objetivo binario: accidente con víctimas graves o fallecidos, si existen columnas.
    Devuelve (y, nombre_columna_fuente, etiqueta_legible).
    """
    lower = {c.lower(): c for c in df.columns}
    checks = [
        ("numero_morts", "Numero_morts>0"),
        ("número_morts", "Numero_morts>0"),
        ("victimes_greus", "Victimes_greus>0"),
        ("víctimes_greus", "Victimes_greus>0"),
        ("victimes greus", "Victimes greus>0"),
        ("victimes_greues", "Victimes_greues>0"),
    ]
    for key, label in checks:
        if key in lower:
            colname = lower[key]
            col = df[colname]
            s = pd.to_numeric(col, errors="coerce").fillna(0)
            return (s > 0).astype(int), colname, label
    return None, None, ""


def train_accidents_gu_from_df(df: pd.DataFrame) -> dict[str, Any] | None:
    """
    Clasificador RandomForest sobre un DataFrame de accidentes (GU o Kaggle con columnas similares).
    """
    df = normalize_columns(df)
    if df.empty or len(df) < 50:
        return None

    y, source_col, y_name = _pick_accidents_target(df)
    if y is None or source_col is None:
        return None

    y = y.reindex(df.index)
    base = df.drop(columns=[source_col], errors="ignore")

    num_cols: list[str] = []
    cat_cols: list[str] = []
    for c in base.columns:
        if pd.api.types.is_numeric_dtype(base[c]):
            num_cols.append(c)
            continue
        nu = pd.to_numeric(base[c], errors="coerce").notna().mean()
        if nu > 0.9:
            num_cols.append(c)
            continue
        if base[c].nunique(dropna=True) <= 40 and base[c].nunique(dropna=True) >= 2:
            cat_cols.append(c)

    if not num_cols and not cat_cols:
        return None

    X_small = base[[c for c in (num_cols + cat_cols) if c in base.columns]].copy()
    mask = y.notna()
    X_small = X_small.loc[mask]
    y = y.loc[mask]
    for c in cat_cols:
        if c in X_small.columns:
            X_small[c] = X_small[c].astype(str)
    if len(y.unique()) < 2:
        return None

    num_use = [c for c in num_cols if c in X_small.columns]
    cat_use = [c for c in cat_cols if c in X_small.columns]

    transformers = []
    if num_use:
        transformers.append(
            (
                "num",
                Pipeline(
                    [
                        ("imp", SimpleImputer(strategy="median")),
                    ]
                ),
                num_use,
            )
        )
    if cat_use:
        transformers.append(
            (
                "cat",
                Pipeline(
                    [
                        ("imp", SimpleImputer(strategy="most_frequent")),
                        ("enc", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)),
                    ]
                ),
                cat_use,
            )
        )
    if not transformers:
        return None

    pre = ColumnTransformer(transformers, remainder="drop")
    pipe = Pipeline(
        [
            ("prep", pre),
            (
                "model",
                RandomForestClassifier(
                    n_estimators=80,
                    max_depth=12,
                    random_state=42,
                    class_weight="balanced",
                ),
            ),
        ]
    )

    X_train, X_test, y_train, y_test = train_test_split(
        X_small, y, test_size=0.25, random_state=42, stratify=y if y.nunique() > 1 else None
    )
    pipe.fit(X_train, y_train)
    pred = pipe.predict(X_test)
    metrics = {
        "accuracy": float(accuracy_score(y_test, pred)),
        "report": classification_report(y_test, pred, zero_division=0),
        "n_train": len(X_train),
        "n_test": len(X_test),
        "target": y_name,
    }
    cat_choices = {
        c: sorted(X_small[c].dropna().astype(str).unique().tolist())[:40] for c in cat_use
    }
    defaults: dict[str, float] = {}
    for c in num_use:
        defaults[c] = float(pd.to_numeric(X_small[c], errors="coerce").median())
    return {
        "pipeline": pipe,
        "metrics": metrics,
        "feature_columns": num_use + cat_use,
        "numeric_columns": num_use,
        "categorical_columns": cat_use,
        "categorical_choices": cat_choices,
        "defaults": defaults,
        "task": "classification",
        "target_description": y_name,
    }


def train_accidents_gu_model(bucket: str, prefix: str) -> dict[str, Any] | None:
    """Lee `prefix` en exploitation y entrena el modelo de accidentes GU."""
    return train_accidents_gu_from_df(load_parquet_prefix(bucket, prefix))


def predict_accidents_gu(bundle: dict[str, Any], inputs: dict[str, float | str]) -> np.ndarray:
    row = {c: inputs.get(c, np.nan) for c in bundle["feature_columns"]}
    X = pd.DataFrame([row])
    return bundle["pipeline"].predict(X)


def _flatten_aemet_if_needed(df: pd.DataFrame) -> pd.DataFrame:
    """Intenta extraer números útiles si las columnas son cadenas JSON anidadas."""
    if df.empty:
        return df
    out = df.copy()
    for c in out.columns:
        if out[c].dtype != object:
            continue
        sample = out[c].dropna().head(3)
        if sample.empty:
            continue
        if not isinstance(sample.iloc[0], str):
            continue
        if not (sample.iloc[0].strip().startswith("{") or sample.iloc[0].strip().startswith("[")):
            continue
        try:
            parsed = pd.json_normalize(
                out[c].map(lambda x: json.loads(x) if pd.notna(x) and str(x).strip() else {})
            )
            parsed.columns = [f"{c}_{sub}" for sub in parsed.columns]
            out = pd.concat([out.drop(columns=[c]), parsed], axis=1)
        except Exception:
            continue
    return out


def train_aemet_model(bucket: str, prefix: str) -> dict[str, Any] | None:
    """
    Regresión sobre columnas numéricas de AEMET (p. ej. temperatura / viento si existen).
    Objetivo: primera columna numérica que parezca magnitud meteorológica; features = resto numéricas.
    """
    df = normalize_columns(load_parquet_prefix(bucket, prefix))
    df = _flatten_aemet_if_needed(df)
    if df.empty or len(df) < 30:
        return None

    num_candidates: list[str] = []
    for c in df.columns:
        conv = pd.to_numeric(df[c], errors="coerce")
        if conv.notna().sum() < max(20, len(df) * 0.3):
            continue
        num_candidates.append(c)

    if len(num_candidates) < 2:
        return None

    priority = re.compile(r"temperature|temperatura|temp_|maxima|mínima|minima|wind|vent|precip|humedad|humitat", re.I)
    target_col = None
    for c in num_candidates:
        if priority.search(c):
            target_col = c
            break
    if target_col is None:
        target_col = num_candidates[-1]

    feat_cols = [c for c in num_candidates if c != target_col]
    if not feat_cols:
        return None

    X = df[feat_cols].apply(pd.to_numeric, errors="coerce")
    y = pd.to_numeric(df[target_col], errors="coerce")
    mask = y.notna() & X.notna().all(axis=1)
    X = X.loc[mask]
    y = y.loc[mask]
    if len(X) < 30:
        return None

    pre = ColumnTransformer([("num", SimpleImputer(strategy="median"), feat_cols)], remainder="drop")
    pipe = Pipeline(
        [
            ("prep", pre),
            ("model", RandomForestRegressor(n_estimators=60, max_depth=10, random_state=42)),
        ]
    )
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)
    pipe.fit(X_train, y_train)
    pred = pipe.predict(X_test)
    metrics = {
        "r2": float(r2_score(y_test, pred)),
        "mae": float(mean_absolute_error(y_test, pred)),
        "n_train": len(X_train),
        "n_test": len(X_test),
        "target": target_col,
    }
    defaults = {c: float(X_train[c].median()) for c in feat_cols}
    return {
        "pipeline": pipe,
        "metrics": metrics,
        "feature_columns": feat_cols,
        "numeric_columns": feat_cols,
        "categorical_columns": [],
        "defaults": defaults,
        "task": "regression",
        "target_description": f"Predicción de `{target_col}`",
    }


def predict_aemet(bundle: dict[str, Any], inputs: dict[str, float]) -> np.ndarray:
    row = {c: float(inputs.get(c, np.nan)) for c in bundle["feature_columns"]}
    X = pd.DataFrame([row])
    return bundle["pipeline"].predict(X)


def train_kanishka_rain_model(bucket: str, prefix: str) -> dict[str, Any] | None:
    """
    Dataset HuggingFace kanishka089 / RainPrediction (según pipeline del repo).
    Predice `Rain` a partir de variables meteorológicas típicas.
    """
    df = normalize_columns(load_parquet_prefix(bucket, prefix))
    if df.empty or len(df) < 30:
        return None

    aliases = {
        "rain": ["rain", "Rain", "precipitación", "precipitacion"],
        "temperature": ["temperature", "Temperature", "temp"],
        "humidity": ["humidity", "Humidity"],
        "heat": ["heat index", "Heat Index", "heat_index"],
        "pressure": ["atmospheric pressure", "Atmospheric pressure", "pressure"],
        "altitude": ["altitude", "Altitude"],
    }
    resolved: dict[str, str] = {}
    for logical, names in aliases.items():
        for n in names:
            key = n.lower().replace(" ", "_")
            for c in df.columns:
                if c.lower().replace(" ", "_") == key:
                    resolved[logical] = c
                    break
            if logical in resolved:
                break

    if "rain" not in resolved:
        return None

    target = resolved["rain"]
    feat_names = [resolved[k] for k in ("temperature", "humidity", "heat", "pressure", "altitude") if k in resolved]
    if len(feat_names) < 2:
        return None

    X = df[feat_names].apply(pd.to_numeric, errors="coerce")
    y = pd.to_numeric(df[target], errors="coerce")
    mask = y.notna() & X.notna().all(axis=1)
    X = X.loc[mask]
    y = y.loc[mask]
    if len(X) < 30:
        return None

    pre = ColumnTransformer([("num", SimpleImputer(strategy="median"), feat_names)], remainder="drop")
    pipe = Pipeline(
        [
            ("prep", pre),
            ("model", RandomForestRegressor(n_estimators=80, max_depth=12, random_state=42)),
        ]
    )
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)
    pipe.fit(X_train, y_train)
    pred = pipe.predict(X_test)
    metrics = {
        "r2": float(r2_score(y_test, pred)),
        "mae": float(mean_absolute_error(y_test, pred)),
        "n_train": len(X_train),
        "n_test": len(X_test),
        "target": target,
    }
    defaults = {c: float(X_train[c].median()) for c in feat_names}
    return {
        "pipeline": pipe,
        "metrics": metrics,
        "feature_columns": feat_names,
        "numeric_columns": feat_names,
        "categorical_columns": [],
        "defaults": defaults,
        "task": "regression",
        "target_description": "Predicción de Rain (lluvia)",
    }


def predict_kanishka(bundle: dict[str, Any], inputs: dict[str, float]) -> np.ndarray:
    row = {c: float(inputs.get(c, np.nan)) for c in bundle["feature_columns"]}
    X = pd.DataFrame([row])
    return bundle["pipeline"].predict(X)


def train_kaggle_accidents_model(bucket: str, prefix: str) -> dict[str, Any] | None:
    """
    Misma lógica que accidents GU sobre el prefijo de Kaggle (si el Parquet existe en exploitation).
    """
    return train_accidents_gu_from_df(load_parquet_prefix(bucket, prefix))


def load_aemet_predictions_table(bucket: str, prefix: str) -> pd.DataFrame:
    """Muestra cruda de filas AEMET en exploitation (predicciones almacenadas)."""
    return normalize_columns(load_parquet_prefix(bucket, prefix))
