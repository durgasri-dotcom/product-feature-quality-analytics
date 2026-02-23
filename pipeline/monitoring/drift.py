import json
import os
import pandas as pd

def pct_change(current: float, baseline: float) -> float:
    if baseline == 0:
        return 0.0
    return (current - baseline) / baseline

def detect_data_drift(df: pd.DataFrame, baseline: dict, threshold: float = 0.20) -> dict:
    current = {
        "latency_ms_mean": float(df["latency_ms"].mean()),
        "crash_flag_rate": float(df["crash_flag"].mean()),
        "feedback_score_mean": float(df["feedback_score"].mean()),
        "error_count_mean": float(df["error_count"].mean()),
    }

    drift = {}
    alerts = []

    for k, v in current.items():
        base_v = float(baseline.get(k, 0))
        change = pct_change(v, base_v)
        drift[k] = {"baseline": base_v, "current": v, "pct_change": float(change)}

        if abs(change) >= threshold:
            alerts.append(f"{k} drifted by {change:.2%} (threshold {threshold:.0%})")

    return {"drift": drift, "alerts": alerts}

def save_data_drift(report: dict) -> None:
    os.makedirs("artifacts/reports", exist_ok=True)
    with open("artifacts/reports/data_drift.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)