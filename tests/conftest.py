import pytest
import pandas as pd
import numpy as np
import sys
import os

# Point to project root so 'from pipeline.x import y' works
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

@pytest.fixture
def sample_raw_df():
    """Synthetic raw telemetry fixture for unit tests."""
    np.random.seed(42)
    n = 100
    return pd.DataFrame({
        "user_id": [f"u{i}" for i in range(n)],
        "feature_name": np.random.choice(["search", "checkout", "login", "dashboard"], n),
        "session_duration": np.random.uniform(10, 300, n),
        "latency_ms": np.random.uniform(80, 600, n),
        "crash_flag": np.random.choice([0, 1], n, p=[0.95, 0.05]),
        "error_count": np.random.randint(0, 10, n),
        "feedback_score": np.random.choice([1, 2, 3, 4, 5], n),
        "timestamp": pd.date_range("2025-01-01", periods=n, freq="h").astype(str),
    })


@pytest.fixture
def sample_processed_df():
    """Synthetic aggregated/processed fixture for pipeline tests."""
    np.random.seed(42)
    n = 50
    return pd.DataFrame({
        "feature_name": np.random.choice(["search", "checkout", "login"], n),
        "date": pd.date_range("2025-01-01", periods=n, freq="D").astype(str),
        "crash_rate": np.random.uniform(0, 0.1, n),
        "usage_count": np.random.randint(50, 500, n),
    })