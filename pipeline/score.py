from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple, Dict, Any
import json
from pathlib import Path

import numpy as np
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    roc_auc_score,
    accuracy_score,
    confusion_matrix,
    classification_report,
)


# ---------------- Config ----------------
@dataclass
class MLConfig:
    label_col: str = "is_high_risk"
    feature_cols: Tuple[str, ...] = ("avg_latency", "crash_rate", "avg_feedback", "usage_count")
    test_size: float = 0.2
    random_state: int = 42
    n_estimators: int = 200
    max_depth: int | None = None


def create_target(df: pd.DataFrame, config: MLConfig) -> pd.DataFrame:
    """
    Create a binary target label for model training.
    We mark 'high risk' features based on crash_rate + latency + feedback.
    """
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


def train_model(df: pd.DataFrame, config: MLConfig) -> Tuple[RandomForestClassifier, Dict[str, Any]]:
    """
    Train a baseline RandomForest model and return metrics.
    """
    df = df.copy()

    df[list(config.feature_cols)] = df[list(config.feature_cols)].replace([np.inf, -np.inf], np.nan)
    df[list(config.feature_cols)] = df[list(config.feature_cols)].fillna(0)

    X = df[list(config.feature_cols)]
    y = df[config.label_col].astype(int)

    # NOTE: stratify needs both classes; if your dataset sometimes has 1 class,
    # handle it safely:
    stratify = y if y.nunique() > 1 else None

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=config.test_size, random_state=config.random_state, stratify=stratify
    )

    model = RandomForestClassifier(
        n_estimators=config.n_estimators,
        random_state=config.random_state,
        max_depth=config.max_depth,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    proba = model.predict_proba(X_test)[:, 1]
    preds = (proba >= 0.5).astype(int)

    metrics = {
        "auc": float(roc_auc_score(y_test, proba)) if len(np.unique(y_test)) > 1 else None,
        "accuracy": float(accuracy_score(y_test, preds)),
        "confusion_matrix": confusion_matrix(y_test, preds).tolist(),
        "classification_report": classification_report(y_test, preds, output_dict=True),
        "n_train": int(len(X_train)),
        "n_test": int(len(X_test)),
        "features": list(config.feature_cols),
        "label_col": config.label_col,
    }

    return model, metrics


def score_dataframe(df: pd.DataFrame, model: RandomForestClassifier, config: MLConfig) -> pd.DataFrame:
    """
    Add model risk score predictions to the dataframe.
    """
    df = df.copy()
    X = df[list(config.feature_cols)].replace([np.inf, -np.inf], np.nan).fillna(0)

    df["risk_score"] = model.predict_proba(X)[:, 1]
    df["risk_bucket"] = pd.cut(
        df["risk_score"],
        bins=[-0.01, 0.33, 0.66, 1.01],
        labels=["low", "medium", "high"],
    )
    return df


def save_artifacts(
    model: RandomForestClassifier,
    metrics: Dict[str, Any],
    df_scored: pd.DataFrame,
    config: MLConfig,
    artifacts_dir: str = "artifacts",
) -> None:
    """
    Save model + metrics + feature importance + baseline stats.
    """
    import joblib

    base = Path(artifacts_dir)
    (base / "models").mkdir(parents=True, exist_ok=True)
    (base / "reports").mkdir(parents=True, exist_ok=True)

    # Save model
    model_path = base / "models" / "risk_model.joblib"
    joblib.dump(model, model_path)

    # Save metrics JSON
    metrics_path = base / "reports" / "metrics.json"
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    # Save feature importance
    importances = getattr(model, "feature_importances_", None)
    if importances is not None:
        fi = pd.DataFrame(
            {"feature": list(config.feature_cols), "importance": importances}
        ).sort_values("importance", ascending=False)
        fi.to_csv(base / "reports" / "feature_importance.csv", index=False)

    # Save baseline stats for drift
    baseline = df_scored[list(config.feature_cols)].describe().to_dict()
    with open(base / "reports" / "baseline_stats.json", "w", encoding="utf-8") as f:
        json.dump(baseline, f, indent=2)