import pandas as pd
import pytest
from pipeline.quality_checks import check_null_rates


def test_check_null_rates_passes_when_valid():
    df = pd.DataFrame({
        "latency_ms": [100, 200, 300],
        "feature_name": ["a", "b", "c"]
    })

    check_null_rates(df)


def test_check_null_rates_raises_when_nulls_present():
    df = pd.DataFrame({
        "latency_ms": [100, None, 300],
        "feature_name": ["a", "b", "c"]
    })

    with pytest.raises(ValueError):
        check_null_rates(df)