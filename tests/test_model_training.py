import pandas as pd
from pipeline.score import MLConfig, create_target, train_model, score_dataframe


def _make_training_df(n: int = 200) -> pd.DataFrame:
    return pd.DataFrame({
        "avg_latency": list(range(50, 50 + n)),
        "crash_rate": [0.01] * (n // 2) + [0.20] * (n - n // 2),
        "avg_feedback": [4.7] * (n // 2) + [2.3] * (n - n // 2),
        "usage_count": [1000] * (n // 2) + [200] * (n - n // 2),
    })


def test_train_model_returns_model_and_metrics():
    config = MLConfig()
    df = _make_training_df(200)

    df_labeled = create_target(df, config)
    model, metrics = train_model(df_labeled, config)

    assert hasattr(model, "predict_proba")
    assert isinstance(metrics, dict)
    assert "accuracy" in metrics
    assert "features" in metrics
    assert metrics["label_col"] == config.label_col


def test_score_dataframe_adds_risk_score_and_bucket():
    config = MLConfig()
    df = _make_training_df(200)

    df_labeled = create_target(df, config)
    model, _ = train_model(df_labeled, config)

    df_scored = score_dataframe(df_labeled, model, config)

    assert "risk_score" in df_scored.columns
    assert "risk_bucket" in df_scored.columns
    assert df_scored["risk_score"].between(0, 1).all()