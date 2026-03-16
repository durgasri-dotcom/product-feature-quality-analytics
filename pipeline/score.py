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
    average_precision_score,
)
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split
from sklearn.preprocessing import RobustScaler

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
        "avg_latency", "crash_rate", "avg_feedback",
        "usage_count", "avg_error_count",
    )
    test_size: float = 0.2
    random_state: int = 42
    n_estimators: int = 300
    max_depth: Optional[int] = 8
    min_samples_leaf: int = 2


def create_target(df: pd.DataFrame, config: MLConfig) -> pd.DataFrame:
    """
    Create a meaningful binary target: is_high_risk.

    Strategy: A feature is high-risk only when it shows degradation
    across MULTIPLE dimensions simultaneously — not just one.
    Uses a scoring approach: flag rows that breach 2+ of 3 strict thresholds.

    This produces ~10-20% positive rate, giving the classifier
    real discriminative signal.
    """
    required = set(config.feature_cols)
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"create_target: Missing required columns: {missing}")

    df = df.copy()

    # Moderate thresholds — 75th/25th percentile
    # Produces ~15-25% positive rate for meaningful ML signal
    crash_thr    = df["crash_rate"].quantile(0.75)
    latency_thr  = df["avg_latency"].quantile(0.75)
    feedback_thr = df["avg_feedback"].quantile(0.25)
    error_thr    = df["avg_error_count"].quantile(0.75) if "avg_error_count" in df.columns else None

    # Score: how many conditions does each row breach?
    risk_score = (
        (df["crash_rate"]   >= crash_thr).astype(int)
        + (df["avg_latency"]  >= latency_thr).astype(int)
        + (df["avg_feedback"] <= feedback_thr).astype(int)
    )
    if error_thr is not None:
        risk_score += (df["avg_error_count"] >= error_thr).astype(int)
        # High risk = breaches at least 2 of 4 conditions
        df[config.label_col] = (risk_score >= 2).astype(int)
    else:
        # High risk = breaches at least 2 of 3 conditions
        df[config.label_col] = (risk_score >= 2).astype(int)

    pos_rate = df[config.label_col].mean() * 100
    logger.info(
        f"create_target | positive_rate={pos_rate:.1f}% | "
        f"high_risk={df[config.label_col].sum()} | total={len(df)}"
    )

    # Warn if label is degenerate
    if pos_rate < 2:
        logger.warning(
            "create_target: Very few positives (<2%) — "
            "consider lowering thresholds or checking data volume"
        )
    if pos_rate > 40:
        logger.warning(
            "create_target: Too many positives (>40%) — "
            "label has weak signal, consider stricter thresholds"
        )

    return df


def train_model(
    df: pd.DataFrame,
    config: MLConfig,
) -> Tuple[RandomForestClassifier, Dict[str, Any]]:
    """
    Train a Random Forest classifier with:
    - Stratified train/test split (preserves class ratio)
    - class_weight='balanced' (handles class imbalance)
    - Cross-validated AUC for robust evaluation
    - Feature scaling via RobustScaler (handles outliers)
    """
    df = df.copy()

    # ── Validate we have enough data ─────────────────────────
    if len(df) < 20:
        raise ValueError(
            f"train_model: Only {len(df)} rows — need at least 20 to train reliably"
        )

    # ── Clean features ────────────────────────────────────────
    # Use all available feature_cols that exist in df
    available_features = [c for c in config.feature_cols if c in df.columns]
    if len(available_features) < 2:
        raise ValueError(
            f"train_model: Only {len(available_features)} feature columns found. "
            f"Need at least 2. Available: {list(df.columns)}"
        )

    df[available_features] = (
        df[available_features]
        .replace([np.inf, -np.inf], np.nan)
        .fillna(0)
    )

    X = df[available_features]
    y = df[config.label_col].astype(int)

    # ── Scale features (RobustScaler handles latency outliers) ─
    scaler = RobustScaler()
    X_scaled = pd.DataFrame(
        scaler.fit_transform(X),
        columns=available_features
    )

    # ── Stratified split ──────────────────────────────────────
    if y.nunique() < 2:
        raise ValueError(
            "train_model: Target column has only one class — "
            "cannot train a classifier. Check create_target()."
        )

    stratify = y if y.value_counts().min() >= 2 else None
    X_train, X_test, y_train, y_test = train_test_split(
        X_scaled, y,
        test_size=config.test_size,
        random_state=config.random_state,
        stratify=stratify,
    )

    # ── Train model ───────────────────────────────────────────
    model = RandomForestClassifier(
        n_estimators=config.n_estimators,
        max_depth=config.max_depth,
        min_samples_leaf=config.min_samples_leaf,
        random_state=config.random_state,
        class_weight="balanced",
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    # ── Evaluate ──────────────────────────────────────────────
    y_pred  = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    roc_auc = float(roc_auc_score(y_test, y_proba))
    avg_precision = float(average_precision_score(y_test, y_proba))

    # Cross-validated AUC (more reliable on small datasets)
    if len(df) >= 50:
        cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=config.random_state)
        cv_auc_scores = cross_val_score(
            RandomForestClassifier(
                n_estimators=config.n_estimators,
                max_depth=config.max_depth,
                min_samples_leaf=config.min_samples_leaf,
                random_state=config.random_state,
                class_weight="balanced",
            ),
            X_scaled, y,
            cv=cv,
            scoring="roc_auc",
        )
        cv_auc_mean = float(cv_auc_scores.mean())
        cv_auc_std  = float(cv_auc_scores.std())
    else:
        cv_auc_mean = roc_auc
        cv_auc_std  = 0.0
        logger.warning("Cross-validation skipped — fewer than 50 rows")

    metrics = {
        "roc_auc":            roc_auc,
        "cv_auc_mean":        cv_auc_mean,
        "cv_auc_std":         cv_auc_std,
        "average_precision":  avg_precision,
        "accuracy":           float(accuracy_score(y_test, y_pred)),
        "positive_rate":      float(y.mean()),
        "confusion_matrix":   confusion_matrix(y_test, y_pred).tolist(),
        "classification_report": classification_report(
            y_test, y_pred, output_dict=True, zero_division=0
        ),
        "train_rows":         len(X_train),
        "test_rows":          len(X_test),
        "feature_cols":       available_features,
        "run_timestamp":      datetime.now(timezone.utc).isoformat(),
    }

    logger.info(
        f"train_model | AUC={roc_auc:.4f} | CV_AUC={cv_auc_mean:.4f}±{cv_auc_std:.4f} | "
        f"AvgPrecision={avg_precision:.4f} | "
        f"Accuracy={metrics['accuracy']:.4f} | "
        f"Train={metrics['train_rows']} | Test={metrics['test_rows']}"
    )

    if roc_auc < 0.60:
        logger.warning(
            f"train_model: Low AUC ({roc_auc:.4f}) — consider adding more features "
            "or increasing data volume"
        )

    # ── MLflow logging ────────────────────────────────────────
    try:
        from mlflow_tracking.mlflow_logger import log_training_run
        fi_df = pd.DataFrame({
            "feature":    available_features,
            "importance": model.feature_importances_,
        }).sort_values("importance", ascending=False)
        log_training_run(model, metrics, config, fi_df, run_name="auto_run")
    except Exception as e:
        logger.debug(f"MLflow logging skipped: {e}")

    return model, metrics


def compute_shap_values(
    model: RandomForestClassifier,
    df: pd.DataFrame,
    config: MLConfig,
) -> pd.DataFrame:
    """
    Compute SHAP feature importance values using TreeExplainer.
    Returns a DataFrame with mean absolute SHAP values per feature.
    """
    try:
        import shap

        # Resolve feature columns
        if hasattr(config, "feature_cols"):
            feature_cols = list(config.feature_cols)
        elif isinstance(config, dict):
            feature_cols = config.get("feature_cols", [])
        else:
            feature_cols = []

        fallback_cols = [
            "avg_latency", "crash_rate", "avg_feedback", "usage_count",
            "avg_error_count", "avg_latency_ms", "avg_feedback_score",
            "quality_score", "session_quality_index",
        ]
        if not feature_cols:
            feature_cols = fallback_cols

        feature_cols = [c for c in feature_cols if c in df.columns]

        if not feature_cols:
            logger.warning("SHAP skipped: no valid feature columns found in DataFrame")
            return pd.DataFrame()

        X = df[feature_cols].replace([np.inf, -np.inf], np.nan).fillna(0)

        explainer   = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X)

        # RandomForest returns list (one array per class) — take class=1
        if isinstance(shap_values, list):
            shap_array = shap_values[1]
        else:
            shap_array = shap_values

        # Handle 3D array (n_samples, n_features, n_classes)
        if shap_array.ndim == 3:
            shap_array = shap_array[:, :, 1]

        mean_abs = np.abs(shap_array).mean(axis=0)

        mean_shap = pd.DataFrame({
            "feature":        feature_cols,
            "mean_abs_shap":  mean_abs.tolist(),
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
    """Score all rows and write risk_probability + risk_label to Silver layer."""
    df = df.copy()

    available_features = [c for c in config.feature_cols if c in df.columns]
    X = df[available_features].replace([np.inf, -np.inf], np.nan).fillna(0)

    df["risk_probability"] = model.predict_proba(X)[:, 1]
    df["risk_label"]       = model.predict(X)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    silver_partition = SILVER_PATH / f"date={today}"
    silver_partition.mkdir(parents=True, exist_ok=True)
    silver_path = silver_partition / "scored_features.parquet"
    df.to_parquet(silver_path, index=False, engine="pyarrow")
    logger.info(f"score_dataframe | scored={len(df)} rows | saved → {silver_path}")

    return df


def save_artifacts(
    model: RandomForestClassifier,
    metrics: Dict[str, Any],
    feature_importance: pd.DataFrame,
) -> None:
    """Persist model, metrics, and feature importance to disk."""
    joblib.dump(model, MODELS_PATH / "risk_model.joblib")

    with open(ARTIFACTS_PATH / "metrics.json", "w") as f:
        json.dump(metrics, f, indent=2, default=str)

    feature_importance.to_csv(
        ARTIFACTS_PATH / "feature_importance.csv", index=False
    )

    logger.info(f"Artifacts saved → {MODELS_PATH} | {ARTIFACTS_PATH}")