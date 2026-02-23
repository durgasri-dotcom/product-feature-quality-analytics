import json
import os
import pandas as pd

BASELINE_PATH = "artifacts/reports/baseline_stats.json"

def compute_baseline(df: pd.DataFrame) -> dict:
    return {
        "row_count": int(len(df)),
        "latency_ms_mean": float(df["latency_ms"].mean()),
        "latency_ms_std": float(df["latency_ms"].std()),
        "crash_flag_rate": float(df["crash_flag"].mean()),
        "feedback_score_mean": float(df["feedback_score"].mean()),
        "error_count_mean": float(df["error_count"].mean()),
    }

def save_baseline(stats: dict) -> None:
    os.makedirs("artifacts/reports", exist_ok=True)
    with open(BASELINE_PATH, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)

def load_baseline() -> dict | None:
    if not os.path.exists(BASELINE_PATH):
        return None
    with open(BASELINE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)