import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


QUALITY_REPORT_PATH = Path("artifacts/reports/data_quality")
QUALITY_REPORT_PATH.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────

def run_great_expectations_suite(df: pd.DataFrame) -> Dict:
    """
    Run a Great Expectations-style validation suite.
    
    We implement the same logic as GE but without requiring
    the full GE server setup (works offline, no extra config).
    
    Returns a report dict with:
    - passed: True/False overall
    - results: list of individual check results
    - score: % of checks that passed
    """
    results = []
    
    def add_result(expectation: str, passed: bool, detail: str):
        results.append({
            "expectation": expectation,
            "passed": passed,
            "detail": detail,
        })

    # ── CHECK 1: Required columns exist ──────────────────────
    required_cols = [
        "user_id", "feature_name", "latency_ms",
        "crash_flag", "feedback_score", "timestamp"
    ]
    missing = [c for c in required_cols if c not in df.columns]
    add_result(
        "expect_columns_to_exist",
        passed=len(missing) == 0,
        detail=f"Missing columns: {missing}" if missing else "All required columns present"
    )

    # ── CHECK 2: No duplicate event records ──────────────────
    if "user_id" in df.columns and "timestamp" in df.columns:
        dup_count = df.duplicated(subset=["user_id", "timestamp"]).sum()
        add_result(
            "expect_no_duplicate_events",
            passed=dup_count == 0,
            detail=f"Found {dup_count} duplicate (user_id, timestamp) pairs"
        )

    # ── CHECK 3: Null rate below threshold ───────────────────
    for col in ["latency_ms", "crash_flag", "feedback_score"]:
        if col in df.columns:
            null_rate = df[col].isnull().mean()
            threshold = 0.05  # Max 5% nulls allowed
            add_result(
                f"expect_column_null_rate_below_threshold_{col}",
                passed=null_rate <= threshold,
                detail=f"{col} null rate: {null_rate:.2%} (threshold: {threshold:.0%})"
            )

    # ── CHECK 4: Latency values in valid range ────────────────
    if "latency_ms" in df.columns:
        min_latency = df["latency_ms"].min()
        max_latency = df["latency_ms"].max()
        valid = (min_latency >= 0) and (max_latency <= 10_000)
        add_result(
            "expect_latency_in_valid_range",
            passed=valid,
            detail=f"Latency range: [{min_latency:.1f}, {max_latency:.1f}] ms. Expected: [0, 10000]"
        )

    # ── CHECK 5: Crash flag is binary (0 or 1 only) ──────────
    if "crash_flag" in df.columns:
        unique_values = set(df["crash_flag"].dropna().unique())
        valid_values = {0, 1, 0.0, 1.0}
        valid = unique_values.issubset(valid_values)
        add_result(
            "expect_crash_flag_is_binary",
            passed=valid,
            detail=f"crash_flag unique values: {unique_values}. Expected: {{0, 1}}"
        )

    # ── CHECK 6: Feedback score in range [1, 5] ──────────────
    if "feedback_score" in df.columns:
        out_of_range = df[(df["feedback_score"] < 1) | (df["feedback_score"] > 5)].shape[0]
        add_result(
            "expect_feedback_score_between_1_and_5",
            passed=out_of_range == 0,
            detail=f"Records with feedback_score outside [1,5]: {out_of_range}"
        )

    # ── CHECK 7: Row count is above minimum ──────────────────
    MIN_ROWS = 50
    add_result(
        "expect_minimum_row_count",
        passed=len(df) >= MIN_ROWS,
        detail=f"Row count: {len(df)}. Minimum required: {MIN_ROWS}"
    )

    # ── CHECK 8: feature_name has known valid values ──────────
    if "feature_name" in df.columns:
        null_names = df["feature_name"].isnull().sum()
        empty_names = (df["feature_name"].astype(str).str.strip() == "").sum()
        add_result(
            "expect_feature_name_not_null_or_empty",
            passed=(null_names == 0 and empty_names == 0),
            detail=f"Null feature names: {null_names}, Empty: {empty_names}"
        )

    # ── CHECK 9: Timestamp is parseable ──────────────────────
    if "timestamp" in df.columns:
        try:
            pd.to_datetime(df["timestamp"], errors="raise")
            add_result("expect_timestamp_parseable", passed=True,
                      detail="All timestamps parsed successfully")
        except Exception as e:
            add_result("expect_timestamp_parseable", passed=False,
                      detail=f"Timestamp parse error: {e}")

    # ── CHECK 10: No extreme latency spikes ──────────────────
    if "latency_ms" in df.columns:
        p99 = df["latency_ms"].quantile(0.99)
        extreme_spikes = (df["latency_ms"] > 5000).sum()
        add_result(
            "expect_no_extreme_latency_spikes",
            passed=extreme_spikes == 0,
            detail=f"Events with latency > 5000ms: {extreme_spikes}. P99 latency: {p99:.1f}ms"
        )

    # ── BUILD FINAL REPORT ────────────────────────────────────
    total = len(results)
    passed_count = sum(1 for r in results if r["passed"])
    failed = [r for r in results if not r["passed"]]

    report = {
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "total_checks": total,
        "passed": passed_count,
        "failed": total - passed_count,
        "score_pct": round(passed_count / total * 100, 1),
        "overall_passed": len(failed) == 0,
        "failures": failed,
        "results": results,
    }

    # Saving report to disk
    report_file = QUALITY_REPORT_PATH / "latest_quality_report.json"
    with open(report_file, "w") as f:
        json.dump(report, f, indent=2, default=str)

    # Log summary
    status = "✅ PASSED" if report["overall_passed"] else "❌ FAILED"
    logger.info(f"Data Quality Suite: {status} | "
               f"Score: {report['score_pct']}% ({passed_count}/{total} checks passed)")

    if failed:
        for f_check in failed:
            logger.warning(f"  FAILED: {f_check['expectation']} → {f_check['detail']}")

    return report


# ─────────────────────────────────────────────


def check_null_rates(df: pd.DataFrame, threshold: float = 0.2) -> bool:
    """Original null rate check — kept for existing tests."""
    null_rates = df.isnull().mean()
    high_null_cols = null_rates[null_rates > threshold]
    if not high_null_cols.empty:
        raise ValueError(
            f"Null rate exceeded threshold for columns: {list(high_null_cols.index)}"
        )
    return True


def check_latency_outliers(df: pd.DataFrame, max_latency: float = 5000) -> bool:
    """Original latency check — kept for existing tests."""
    if df["latency_ms"].max() > max_latency:
        raise ValueError("Extreme latency spike detected")
    return True
