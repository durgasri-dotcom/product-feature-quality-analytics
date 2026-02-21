import pandas as pd

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    #  create date from timestamp
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["date"] = df["timestamp"].dt.date.astype(str)

    
    df["quality_score"] = (
        1 / (1 + df["latency_ms"])
        + (1 - df["crash_flag"])
        + df["feedback_score"] / 5
    ) / 3

    return df