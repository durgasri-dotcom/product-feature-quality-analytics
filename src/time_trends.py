import pandas as pd

df = pd.read_csv("data/raw/product_logs.csv")
df["timestamp"] = pd.to_datetime(df["timestamp"])

# Ensure daily granularity
daily = df.groupby(
    [df["timestamp"].dt.date, "feature_name"]
).agg(
    avg_latency_ms=("latency_ms", "mean"),
    avg_feedback=("feedback_score", "mean")
).reset_index()

daily.rename(columns={"timestamp": "date"}, inplace=True)

# Rolling averages (this is the key)
daily["latency_trend"] = (
    daily.groupby("feature_name")["avg_latency_ms"]
    .rolling(window=3, min_periods=1)
    .mean()
    .reset_index(level=0, drop=True)
)

daily["feedback_trend"] = (
    daily.groupby("feature_name")["avg_feedback"]
    .rolling(window=3, min_periods=1)
    .mean()
    .reset_index(level=0, drop=True)
)

daily.to_csv("data/processed/feature_daily_trends.csv", index=False)

print("Enhanced time-trend data generated")
