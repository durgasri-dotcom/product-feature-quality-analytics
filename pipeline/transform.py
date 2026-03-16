import logging
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

SILVER_PATH = Path("data/silver")
SILVER_PATH.mkdir(parents=True, exist_ok=True)


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
   
    if df.empty:
        logger.warning("transform_skipped: empty DataFrame received")
        return df

    df = df.copy()

    # ── STEP 1: Parse timestamp ───────────────────────────────
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["date"] = df["timestamp"].dt.date.astype(str)

    bad_timestamps = df["timestamp"].isnull().sum()
    if bad_timestamps > 0:
        logger.warning(f"Dropped {bad_timestamps} rows with unparseable timestamps")
        df = df.dropna(subset=["timestamp"])

    # ── STEP 2: Time-based features ───────────────────────────
    df["hour_of_day"] = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.dayofweek   # 0=Monday, 6=Sunday
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)
    df["is_business_hours"] = df["hour_of_day"].between(9, 17).astype(int)

    # ── STEP 3: Original quality score  ──
    df["quality_score"] = (
        1 / (1 + df["latency_ms"])
        + (1 - df["crash_flag"])
        + df["feedback_score"] / 5
    ) / 3

    # ── STEP 4: Latency bucket (categorical signal) ───────────
    
    df["latency_bucket"] = pd.cut(
        df["latency_ms"],
        bins=[0, 100, 300, 600, 1000, float("inf")],
        labels=["excellent", "good", "moderate", "poor", "critical"],
    ).astype(str)

    # ── STEP 5: Anomaly flag ──────────────────────────────────
    # Flags events that look suspicious — high latency AND crash AND bad feedback
    df["is_anomaly"] = (
        (df["latency_ms"] > df["latency_ms"].quantile(0.95))
        & (df["crash_flag"] == 1)
        & (df["feedback_score"] < 2.0)
    ).astype(int)

    # ── STEP 6: Error rate signal ─────────────────────────────
    df["has_errors"] = (df["error_count"] > 0).astype(int)
    df["high_error"] = (df["error_count"] > 3).astype(int)

    # ── STEP 7: Session quality index (0 to 1) ───────────────
    # Combines session duration + feedback into one score
    max_duration = df["session_duration"].max() if df["session_duration"].max() > 0 else 1
    df["session_quality_index"] = (
        (df["session_duration"] / max_duration) * 0.5
        + (df["feedback_score"] / 5.0) * 0.5
    ).round(4)

    # ── STEP 8: Log-transform latency ────────────────────────
    # Log transform reduces skew — helps ML models learn better
    df["log_latency"] = np.log1p(df["latency_ms"])

    # ── Save to Silver layer as Parquet ──────────────────────
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    silver_partition = SILVER_PATH / f"date={today}"
    silver_partition.mkdir(parents=True, exist_ok=True)
    silver_path = silver_partition / "transformed_events.parquet"
    df.to_parquet(silver_path, index=False, engine="pyarrow")

    logger.info(
        f"transform_complete | rows={len(df)} | "
        f"anomalies={df['is_anomaly'].sum()} | "
        f"silver_path={silver_path}"
    )

    return df