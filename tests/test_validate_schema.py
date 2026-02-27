import pandas as pd
import pytest
from pipeline.validate import validate_schema


def test_validate_schema_raw_passes_with_required_columns():
    df = pd.DataFrame({
        "user_id": [1],
        "feature_name": ["search"],
        "session_duration": [120],
        "latency_ms": [250],
        "crash_flag": [0],
        "error_count": [0],
        "feedback_score": [4.5],
        "timestamp": ["2026-01-01T00:00:00Z"]
    })

    out = validate_schema(df, stage="raw")

    assert out is df


def test_validate_schema_raw_raises_on_missing_column():
    df = pd.DataFrame({
        "feature_name": ["search"]
    })

    with pytest.raises(ValueError):
        validate_schema(df, stage="raw")