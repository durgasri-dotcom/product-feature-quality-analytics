import pandas as pd
from pipeline.aggregate import aggregate_daily
from pipeline.validate import validate_schema


def test_aggregate_daily_produces_valid_schema():

    df = pd.DataFrame({
        "feature_name": ["search", "search"],
        "latency_ms": [100, 200],
        "crash_flag": [0, 1],
        "feedback_score": [4.0, 3.0],
        "timestamp": [
            "2026-01-01T00:00:00Z",
            "2026-01-01T01:00:00Z"
        ],
        "user_id": [1, 2],
        "session_duration": [100, 200],
        "error_count": [0, 1]
    })

    out = aggregate_daily(df)

    validate_schema(out, stage="processed")

    assert "feature_name" in out.columns
    assert len(out) > 0