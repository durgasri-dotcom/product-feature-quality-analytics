# -------- Raw Data Schema --------
RAW_REQUIRED_COLUMNS = [
    "user_id",
    "feature_name",
    "session_duration",
    "latency_ms",
    "crash_flag",
    "error_count",
    "feedback_score",
    "timestamp"
]

# -------- Processed Data Schema --------
PROCESSED_REQUIRED_COLUMNS = [
    "feature_name",
    "date",
    "crash_rate",
    "usage_count"
]


def validate_schema(df, stage="raw"):
    """
    Validates dataframe schema depending on pipeline stage.
    stage = "raw" or "processed"
    """

    if stage == "raw":
        required_columns = RAW_REQUIRED_COLUMNS

    elif stage == "processed":
        required_columns = PROCESSED_REQUIRED_COLUMNS

    else:
        raise ValueError("Invalid stage provided to validate_schema")

    missing = [col for col in required_columns if col not in df.columns]

    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    return df

