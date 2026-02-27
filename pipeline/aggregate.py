import pandas as pd

def aggregate_daily(df):
    df = df.copy()

    # Ensure timestamp is datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # Create daily grain column
    df["date"] = df["timestamp"].dt.date.astype(str)

    aggregated = (
        df.groupby(["feature_name", "date"], as_index=False)
        .agg(
            avg_latency=("latency_ms", "mean"),
            crash_rate=("crash_flag", "mean"),
            avg_feedback=("feedback_score", "mean"),
            usage_count=("user_id", "count"),
        )
    )

    return aggregated