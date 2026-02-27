import pandas as pd
from pipeline.monitoring.drift import detect_data_drift


def test_detect_data_drift_flags_large_shift():
    baseline = {
        "latency_ms_mean": 120.0,
        "crash_flag_rate": 0.02,
        "feedback_score_mean": 4.2,
        "error_count_mean": 0.1,
    }

    df = pd.DataFrame({
        "latency_ms": [400, 410, 420, 430, 440],
        "crash_flag": [0, 0, 0, 0, 0],
        "feedback_score": [4.2, 4.1, 4.3, 4.2, 4.1],
        "error_count": [0, 0, 0, 0, 0],
    })

    report = detect_data_drift(df, baseline, threshold=0.20)

    assert "drift" in report
    assert "alerts" in report
    assert len(report["alerts"]) >= 1