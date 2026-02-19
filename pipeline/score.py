import pandas as pd
import joblib
import logging

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score

logger = logging.getLogger(__name__)


# ---------------------------------------------------
# Create Target Variable
# ---------------------------------------------------
def create_target(df: pd.DataFrame, threshold: float = 0.03) -> pd.DataFrame:
    """
    Create binary risk label from crash_rate.
    """

    if "crash_rate" not in df.columns:
        raise ValueError("crash_rate column required to create target")

    df["risk_label"] = (df["crash_rate"] > threshold).astype(int)

    logger.info(
        f"Target distribution:\n{df['risk_label'].value_counts(normalize=True)}"
    )

    return df


# ---------------------------------------------------
# Train Model
# ---------------------------------------------------
def train_model(df: pd.DataFrame):

    required_cols = ["avg_latency", "avg_feedback", "usage_count", "risk_label"]

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for training: {missing}")

    X = df[["avg_latency", "avg_feedback", "usage_count"]]
    y = df["risk_label"]

    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = RandomForestClassifier(random_state=42)
    model.fit(X_train, y_train)

    preds = model.predict_proba(X_val)[:, 1]
    auc = roc_auc_score(y_val, preds)

    logger.info(f"Model trained | Validation AUC: {auc:.4f}")

    return model


# ---------------------------------------------------
# Save Model
# ---------------------------------------------------
def save_model(model, path="models/risk_model.pkl"):
    joblib.dump(model, path)
    logger.info(f"Model saved to {path}")
