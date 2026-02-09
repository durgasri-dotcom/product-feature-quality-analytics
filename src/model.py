import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

# Load processed metrics
df = pd.read_csv("data/processed/feature_metrics.csv")

# Define risk label
threshold = df["quality_score"].median()
df["high_risk"] = (df["quality_score"] < threshold).astype(int)

features = [
    "avg_latency_ms",
    "crash_rate",
    "avg_error_count",
    "avg_feedback_score",
    "usage_count"
]

X = df[features]
y = df["high_risk"]

# Safety check: ensure at least 2 classes
if y.nunique() < 2:
    print("⚠️ Only one risk class detected. Assigning neutral probabilities.")
    df["risk_probability"] = 0.5
else:
    model = RandomForestClassifier(
        n_estimators=100,
        random_state=42
    )
    model.fit(X, y)

    # Safe probability extraction
    proba = model.predict_proba(X)
    class_index = list(model.classes_).index(1)
    df["risk_probability"] = proba[:, class_index]

# Save results
df.to_csv("data/processed/feature_metrics_with_risk.csv", index=False)

print("ML risk scoring completed successfully")

