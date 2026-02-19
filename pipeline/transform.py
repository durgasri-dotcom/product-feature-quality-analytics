import pandas as pd

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Feature engineering layer.
    Creates date, crash_rate, and usage_count
    """

    # Convert timestamp to datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Create date column
    df["date"] = df["timestamp"].dt.date

    # Create usage count (each row = 1 session)
    df["usage_count"] = 1

    # Crash rate proxy (0 or 1 already)
    df["crash_rate"] = df["crash_flag"]

    # Quality score (your existing logic)
    df["quality_score"] = (
        1 / (1 + df["latency_ms"])
        + (1 - df["crash_flag"])
        + df["feedback_score"] / 5
    ) / 3

    return df
