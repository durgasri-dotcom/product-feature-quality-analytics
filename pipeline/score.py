from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import logging

logger = logging.getLogger(__name__)

def create_target(df):
    """
    Create binary degradation target.
    1 = feature at risk
    """
    df["at_risk"] = (
        (df["crash_rate"] > 0.05) |
        (df["avg_latency"] > 400)
    ).astype(int)

    return df


def train_model(df):
    """
    Train simple RandomForest model
    """

    features = ["avg_latency", "crash_rate", "usage_count"]
    X = df[features]
    y = df["at_risk"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = RandomForestClassifier(random_state=42)
    model.fit(X_train, y_train)

    score = model.score(X_test, y_test)
    logger.info(f"Model validation accuracy: {score:.3f}")

    return model


def score_features(model, df):
    """
    Generate risk probabilities
    """

    features = ["avg_latency", "crash_rate", "usage_count"]
    df["risk_probability"] = model.predict_proba(df[features])[:, 1]

    return df
