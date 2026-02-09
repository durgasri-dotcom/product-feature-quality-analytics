import pandas as pd

def main():
    raw_df = pd.read_csv("data/raw/product_logs.csv")

    feature_metrics = raw_df.groupby("feature_name").agg(
        avg_latency_ms=("latency_ms", "mean"),
        crash_rate=("crash_flag", "mean"),
        avg_error_count=("error_count", "mean"),
        avg_feedback_score=("feedback_score", "mean"),
        usage_count=("user_id", "count")
    ).reset_index()

    feature_metrics["quality_score"] = (
        0.4 * feature_metrics["avg_feedback_score"]
        - 0.3 * feature_metrics["crash_rate"]
        - 0.2 * (feature_metrics["avg_latency_ms"] / 1000)
        - 0.1 * feature_metrics["avg_error_count"]
    )

    feature_metrics.to_csv("data/processed/feature_metrics.csv", index=False)
    print("feature_metrics.csv created successfully")

if __name__ == "__main__":
    main()
