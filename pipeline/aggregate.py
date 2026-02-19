def aggregate_daily(df):
    """
    Aggregates feature metrics per feature per day
    """

    aggregated = (
        df.groupby(["feature_name", "date"])
        .agg(
            avg_latency=("latency_ms", "mean"),
            crash_rate=("crash_rate", "mean"),
            avg_feedback=("feedback_score", "mean"),
            usage_count=("usage_count", "sum"),
        )
        .reset_index()
    )

    return aggregated
