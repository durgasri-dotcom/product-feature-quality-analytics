def check_null_rates(df, threshold=0.2):
    null_rates = df.isnull().mean()
    high_null_cols = null_rates[null_rates > threshold]

    if not high_null_cols.empty:
        raise ValueError(
            f"Null rate exceeded threshold for columns: {list(high_null_cols.index)}"
        )

    return True


def check_latency_outliers(df, max_latency=5000):
    if df["latency_ms"].max() > max_latency:
        raise ValueError("Extreme latency spike detected")

    return True
