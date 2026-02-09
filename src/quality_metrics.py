def compute_quality_score(df):
    df["quality_score"] = (
        0.4 * df["feedback_score"]
        - 0.3 * df["crash_flag"]
        - 0.2 * df["latency_ms"] / 1000
        - 0.1 * df["error_count"]
    )
    return df
