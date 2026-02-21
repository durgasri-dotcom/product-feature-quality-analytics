def aggregate_daily(df):
    aggregated = (
        df.groupby(["feature_name", "date"])
        .agg(
            avg_latency=("latency_ms", "mean"),
            crash_rate=("crash_flag", "mean"),   
            avg_feedback=("feedback_score", "mean"),
            usage_count=("user_id", "count")     
        )
        .reset_index()
    )
    return aggregated