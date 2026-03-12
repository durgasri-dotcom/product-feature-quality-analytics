from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split

logger = logging.getLogger(__name__)

MODELS_PATH = Path("models")
ARTIFACTS_PATH = Path("artifacts/reports")
SILVER_PATH = Path("data/silver")

MODELS_PATH.mkdir(parents=True, exist_ok=True)
ARTIFACTS_PATH.mkdir(parents=True, exist_ok=True)
SILVER_PATH.mkdir(parents=True, exist_ok=True)


@dataclass
class MLConfig:
    label_col: str = "is_high_risk"
    feature_cols: Tuple[str, ...] = (
        "avg_latency", "crash_rate", "avg_feedback", "usage_count"
    )
    test_size: float = 0.2
    random_state: int = 42
    n_estimators: int = 200
    max_depth: Optional[int] = None


def create_target(df: pd.DataFrame, config: MLConfig) -> pd.DataFrame:
    required = set(config.feature_cols)
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"create_target: Missing required columns: {missing}")

    crash_thr = df["crash_rate"].quantile(0.85)
    latency_thr = df["avg_latency"].quantile(0.85)
    feedback_thr = df["avg_feedback"].quantile(0.15)

    df = df.copy()
    df[config.label_col] = (
        (df["crash_rate"] >= crash_thr)
        | (df["avg_latency"] >= latency_thr)
        | (df["avg_feedback"] <= feedback_thr)
    ).astype(int)

    return df


def train_model(
    df: pd.DataFrame,
    config: MLConfig,
) -> Tuple[RandomForestClassifier, Dict[str, Any]]:
    df = df.copy()
    df[list(config.feature_cols)] = (
        df[list(config.feature_cols)]
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
    )

    X = df[list(config.feature_cols)]
    y = df[config.label_col].astype(int)

    stratify = y if y.nunique() > 1 else None
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=config.test_size,
        random_state=config.random_state,
        stratify=stratify,
    )

    model = RandomForestClassifier(
        n_estimators=config.n_estimators,
        random_state=config.random_state,
        max_depth=config.max_depth,
        class_weight="balanced",
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    metrics = {
        "accuracy": float(accuracy_score(y_test, y_pred)),
        "roc_auc": float(roc_auc_score(y_test, y_proba)) if y.nunique() > 1 else 0.0,
        "confusion_matrix": confusion_matrix(y_test, y_pred).tolist(),
        "classification_report": classification_report(y_test, y_pred, output_dict=True),
        "train_rows": len(X_train),
        "test_rows": len(X_test),
        "feature_cols": list(config.feature_cols),
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
    }

    logger.info(
        f"Model trained | AUC={metrics['roc_auc']:.4f} | "
        f"Accuracy={metrics['accuracy']:.4f} | "
        f"Train={metrics['train_rows']} | Test={metrics['test_rows']}"
    )

    try:
        from mlflow_tracking.mlflow_logger import log_training_run
        fi_df = pd.DataFrame({
            "feature": list(config.feature_cols),
            "importance": model.feature_importances_,
        }).sort_values("importance", ascending=False)
        log_training_run(model, metrics, config, fi_df, run_name="auto_run")
    except Exception as e:
        logger.debug(f"MLflow logging skipped: {e}")

    return model, metrics


def compute_shap_values(model, df, config):
    """
    Compute SHAP feature importance values.
    Handles MLConfig dataclass OR plain dict for config.
    """
    try:
        import shap

        
        if hasattr(config, "feature_cols"):
            feature_cols = list(config.feature_cols)
        elif isinstance(config, dict):
            feature_cols = config.get("feature_cols", [])
        else:
            feature_cols = []

        
        fallback_cols = [
            "avg_latency", "crash_rate", "avg_feedback", "usage_count",
            "avg_latency_ms", "avg_error_count", "avg_feedback_score",
            "quality_score", "session_quality_index",
        ]
        if not feature_cols:
            feature_cols = fallback_cols

        
        feature_cols = [c for c in feature_cols if c in df.columns]

        if not feature_cols:
            logger.warning("SHAP skipped: no valid feature columns found in DataFrame")
            return pd.DataFrame()

        X = df[feature_cols].replace([np.inf, -np.inf], np.nan).fillna(0)

        explainer = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X)

        # Handle list output (one array per class) from RandomForest
        if isinstance(shap_values, list):
            shap_array = shap_values[1]  # class=1 (high risk)
        else:
            shap_array = shap_values

        # Handle 3D array (n_samples, n_features, n_classes)
        if shap_array.ndim == 3:
            shap_array = shap_array[:, :, 1]

        #  guaranteed 2D → mean across rows → 1D
        mean_abs = np.abs(shap_array).mean(axis=0)

        mean_shap = pd.DataFrame({
            "feature": feature_cols,
            "mean_abs_shap": mean_abs.tolist(),  # .tolist() ensures 1D Python list
        }).sort_values("mean_abs_shap", ascending=False).reset_index(drop=True)

        logger.info(f"SHAP complete | top feature: {mean_shap.iloc[0]['feature']}")
        return mean_shap

    except Exception as e:
        logger.warning(f"SHAP skipped: {e}")
        return pd.DataFrame()


def score_dataframe(
    df: pd.DataFrame,
    model: RandomForestClassifier,
    config: MLConfig,
) -> pd.DataFrame:
    df = df.copy()
    X = df[list(config.feature_cols)].replace([np.inf, -np.inf], np.nan).fillna(0)
    df["risk_probability"] = model.predict_proba(X)[:, 1]
    df["risk_label"] = model.predict(X)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    silver_partition = SILVER_PATH / f"date={today}"
    silver_partition.mkdir(parents=True, exist_ok=True)
    silver_path = silver_partition / "scored_features.parquet"
    df.to_parquet(silver_path, index=False, engine="pyarrow")
    logger.info(f"Silver layer saved → {silver_path}")

    return df


def save_artifacts(
    model: RandomForestClassifier,
    metrics: Dict[str, Any],
    feature_importance: pd.DataFrame,
) -> None:
    joblib.dump(model, MODELS_PATH / "risk_model.joblib")

    with open(ARTIFACTS_PATH / "metrics.json", "w") as f:
        json.dump(metrics, f, indent=2, default=str)

    feature_importance.to_csv(ARTIFACTS_PATH / "feature_importance.csv", index=False)

    logger.info(f"Artifacts saved → {MODELS_PATH} and {ARTIFACTS_PATH}")