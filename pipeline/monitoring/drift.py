import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from scipy import stats  # KS test comes from scipy

logger = logging.getLogger(__name__)

ARTIFACTS_PATH = Path("artifacts/reports")
ARTIFACTS_PATH.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────
# PSI THRESHOLDS (industry standard):
#   PSI < 0.1  → No significant change
#   PSI 0.1–0.2 → Moderate change, monitor closely
#   PSI > 0.2  → Significant drift, investigate!
# ─────────────────────────────────────────────
PSI_NO_CHANGE = 0.10
PSI_MODERATE  = 0.20

# KS Test p-value threshold:
# p < 0.05 → distributions are statistically different (95% confidence)
KS_P_VALUE_THRESHOLD = 0.05


def compute_psi(baseline_series: pd.Series, current_series: pd.Series, bins: int = 10) -> float:
    """
    Compute Population Stability Index (PSI).
    
    PSI measures how much a variable's distribution has shifted
    between a baseline period and current period.
    
    This is the STANDARD metric used by:
    - Goldman Sachs for credit score monitoring
    - Netflix for recommendation model health
    - Google for ad ranking model stability
    
    Args:
        baseline_series: The reference distribution (historical data)
        current_series: The current distribution (new data)
        bins: Number of buckets to divide the distribution into
    
    Returns:
        PSI score (float). Higher = more drift.
    """
    # Remove nulls
    baseline = baseline_series.dropna()
    current = current_series.dropna()

    if len(baseline) == 0 or len(current) == 0:
        return 0.0

    # Create bins from baseline distribution
    min_val = min(baseline.min(), current.min())
    max_val = max(baseline.max(), current.max())

    if min_val == max_val:
        return 0.0

    bin_edges = np.linspace(min_val, max_val, bins + 1)

    # Count how many values fall in each bin
    baseline_counts, _ = np.histogram(baseline, bins=bin_edges)
    current_counts, _ = np.histogram(current, bins=bin_edges)

    # Convert to proportions (avoid division by zero with small epsilon)
    epsilon = 1e-6
    baseline_pct = (baseline_counts + epsilon) / (len(baseline) + epsilon * bins)
    current_pct = (current_counts + epsilon) / (len(current) + epsilon * bins)

    # PSI formula: sum of (current% - baseline%) * ln(current% / baseline%)
    psi = np.sum((current_pct - baseline_pct) * np.log(current_pct / baseline_pct))

    return float(round(psi, 6))


def run_ks_test(baseline_series: pd.Series, current_series: pd.Series) -> Dict:
    """
    Kolmogorov-Smirnov (KS) Test for distribution shift.
    
    The KS test asks: "Are these two samples from the same distribution?"
    It returns:
    - statistic: How different the distributions are (0=same, 1=completely different)
    - p_value: Probability the difference is due to random chance
               p < 0.05 means the drift is statistically significant
    
    Args:
        baseline_series: Historical data
        current_series: Current data
    
    Returns:
        Dict with ks_statistic, p_value, and drift_detected flag
    """
    baseline = baseline_series.dropna()
    current = current_series.dropna()

    if len(baseline) < 5 or len(current) < 5:
        return {
            "ks_statistic": 0.0,
            "p_value": 1.0,
            "drift_detected": False,
            "note": "Insufficient data for KS test (need ≥ 5 samples)"
        }

    ks_stat, p_value = stats.ks_2samp(baseline, current)

    return {
        "ks_statistic": float(round(ks_stat, 6)),
        "p_value": float(round(p_value, 6)),
        "drift_detected": p_value < KS_P_VALUE_THRESHOLD,
        "interpretation": (
            f"KS={ks_stat:.4f}, p={p_value:.4f} → "
            + ("⚠️ Drift detected (p < 0.05)" if p_value < KS_P_VALUE_THRESHOLD
               else "✅ No significant drift")
        )
    }


def detect_data_drift(
    df: pd.DataFrame,
    baseline: dict,
    threshold: float = 0.20,
    baseline_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Full drift detection suite combining:
    1. Original % change method (backward compatible)
    2. PSI scores per column
    3. KS test per column (if baseline_df provided)
    
    Args:
        df: Current data DataFrame
        baseline: Baseline stats dict (from baseline.py)
        threshold: % change threshold for legacy alerts
        baseline_df: Optional historical DataFrame for statistical tests
    
    Returns:
        Comprehensive drift report dictionary
    """
    columns_to_monitor = ["latency_ms", "crash_flag", "feedback_score", "error_count"]
    
    current_stats = {
        "latency_ms_mean": float(df["latency_ms"].mean()) if "latency_ms" in df.columns else 0,
        "crash_flag_rate": float(df["crash_flag"].mean()) if "crash_flag" in df.columns else 0,
        "feedback_score_mean": float(df["feedback_score"].mean()) if "feedback_score" in df.columns else 0,
        "error_count_mean": float(df["error_count"].mean()) if "error_count" in df.columns else 0,
    }

    # ── LAYER 1: Original % change (kept for backward compatibility) ──
    drift = {}
    alerts = []
    for k, v in current_stats.items():
        base_v = float(baseline.get(k, 0))
        change = _pct_change(v, base_v)
        drift[k] = {
            "baseline": base_v,
            "current": v,
            "pct_change": float(change),
        }
        if abs(change) >= threshold:
            alerts.append(f"{k} drifted by {change:.2%} (threshold {threshold:.0%})")

    # ── LAYER 2: PSI Scores ───────────────────────────────────────────
    psi_scores = {}
    psi_alerts = []

    col_map = {
        "latency_ms": "latency_ms",
        "crash_flag": "crash_flag",
        "feedback_score": "feedback_score",
        "error_count": "error_count",
    }

    if baseline_df is not None:
        for col in columns_to_monitor:
            if col in df.columns and col in baseline_df.columns:
                psi = compute_psi(baseline_df[col], df[col])
                psi_scores[col] = {
                    "psi": psi,
                    "severity": (
                        "HIGH" if psi > PSI_MODERATE
                        else "MODERATE" if psi > PSI_NO_CHANGE
                        else "LOW"
                    )
                }
                if psi > PSI_MODERATE:
                    psi_alerts.append(
                        f"PSI ALERT: {col} PSI={psi:.4f} > {PSI_MODERATE} (HIGH drift)"
                    )
                elif psi > PSI_NO_CHANGE:
                    psi_alerts.append(
                        f"PSI WARNING: {col} PSI={psi:.4f} — moderate drift, monitor closely"
                    )

    # ── LAYER 3: KS Test ─────────────────────────────────────────────
    ks_results = {}
    ks_alerts = []

    if baseline_df is not None:
        for col in columns_to_monitor:
            if col in df.columns and col in baseline_df.columns:
                ks = run_ks_test(baseline_df[col], df[col])
                ks_results[col] = ks
                if ks["drift_detected"]:
                    ks_alerts.append(
                        f"KS ALERT: {col} — {ks['interpretation']}"
                    )

    # ── BUILD FINAL REPORT ────────────────────────────────────────────
    all_alerts = alerts + psi_alerts + ks_alerts
    report = {
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "drift": drift,
        "alerts": all_alerts,
        "psi_scores": psi_scores,
        "ks_test_results": ks_results,
        "overall_drift_detected": len(all_alerts) > 0,
        "alert_count": len(all_alerts),
    }

    # Log summary
    if all_alerts:
        logger.warning(f"⚠️  DRIFT DETECTED: {len(all_alerts)} alerts")
        for a in all_alerts:
            logger.warning(f"   {a}")
    else:
        logger.info("✅ No significant drift detected across all methods")

    return report


def save_data_drift(report: dict) -> None:
    """Save drift report to disk."""
    ARTIFACTS_PATH.mkdir(parents=True, exist_ok=True)
    with open(ARTIFACTS_PATH / "data_drift.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    logger.info(f"Drift report saved → {ARTIFACTS_PATH / 'data_drift.json'}")


def _pct_change(current: float, baseline: float) -> float:
    """Calculate percentage change (original helper — kept for compatibility)."""
    if baseline == 0:
        return 0.0
    return (current - baseline) / baseline
