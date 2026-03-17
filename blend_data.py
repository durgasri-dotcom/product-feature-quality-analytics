import pandas as pd
import numpy as np
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("blend_data")

# ── Config ────────────────────────────────────────────────────
KAGGLE_PATH = "C:/Users/sridu/OneDrive/Desktop/archive/2019-Oct.csv"
OUTPUT_PATH = "data/raw/product_logs.csv"
SAMPLE_SIZE = 50_000  
RANDOM_SEED = 42

# ── Feature mapping from category_code ───────────────────────
CATEGORY_TO_FEATURE = {
    "electronics.smartphone":     "Payments",
    "electronics.audio":          "VideoPlayback",
    "electronics.tablet":         "Search",
    "electronics.laptop":         "Recommendations",
    "computers.notebook":         "Recommendations",
    "computers.desktop":          "Search",
    "appliances.kitchen":         "Login",
    "appliances.environment":     "Login",
    "sport.trainer":              "Search",
    "kids.toys":                  "Recommendations",
}

def map_feature(category_code):
    if pd.isna(category_code):
        return np.random.choice(["Login", "Payments", "VideoPlayback", "Recommendations", "Search"])
    for key, feature in CATEGORY_TO_FEATURE.items():
        if str(category_code).startswith(key):
            return feature
    # Map by first category word
    first = str(category_code).split(".")[0]
    mapping = {
        "electronics": "VideoPlayback",
        "computers":   "Recommendations",
        "appliances":  "Login",
        "sport":       "Search",
        "kids":        "Recommendations",
        "auto":        "Payments",
        "furniture":   "Search",
        "medicine":    "Login",
        "construction":"Payments",
        "clothing":    "Search",
    }
    return mapping.get(first, np.random.choice(["Login", "Payments", "VideoPlayback"]))


def generate_latency(event_type, price, feature):
    """Generate realistic latency based on event complexity."""
    base = {
        "view":    150,
        "cart":    220,
        "purchase":380,
        "remove_from_cart": 180,
    }.get(event_type, 200)

    # Higher price items = more complex pages = more latency
    price_factor = min(price / 500, 2.0) if not np.isnan(price) else 1.0

    # Feature-specific latency profiles
    feature_factor = {
        "Payments":        1.4,
        "VideoPlayback":   1.6,
        "Search":          0.8,
        "Recommendations": 1.1,
        "Login":           0.7,
    }.get(feature, 1.0)

    noise = np.random.normal(0, 30)
    latency = base * price_factor * feature_factor + noise
    return max(80, min(600, round(latency)))


def generate_crash_flag(event_type, latency):
    """Low crash rate (~2%), higher for purchases with high latency."""
    if event_type == "purchase" and latency > 450:
        return np.random.choice([0, 1], p=[0.93, 0.07])
    return np.random.choice([0, 1], p=[0.98, 0.02])


def generate_feedback(event_type, price):
    """Feedback score based on event type and price satisfaction."""
    if event_type == "purchase":
        # Purchases generally positive
        return round(np.random.normal(4.1, 0.6), 1)
    elif event_type == "remove_from_cart":
        # Abandonment = negative signal
        return round(np.random.normal(2.8, 0.7), 1)
    else:
        return round(np.random.normal(3.5, 0.8), 1)


def generate_error_count(latency, crash_flag):
    if crash_flag == 1:
        return np.random.randint(3, 8)
    if latency > 400:
        return np.random.randint(1, 4)
    return np.random.randint(0, 2)


def generate_session_duration(event_type, feature):
    base = {
        "view":    45,
        "cart":    120,
        "purchase":240,
        "remove_from_cart": 60,
    }.get(event_type, 90)
    noise = np.random.normal(0, 20)
    return max(10, round(base + noise))


def blend(kaggle_path: str, output_path: str, sample_size: int):
    logger.info(f"Loading {sample_size:,} rows from Kaggle dataset...")
    
    df = pd.read_csv(kaggle_path, nrows=sample_size * 3)  # load extra, then sample
    df = df.dropna(subset=["event_time", "user_id", "event_type"])
    df = df.sample(n=min(sample_size, len(df)), random_state=RANDOM_SEED).reset_index(drop=True)

    logger.info(f"Sampled {len(df):,} rows — blending schema...")

    np.random.seed(RANDOM_SEED)

    # ── Map feature names ─────────────────────────────────────
    df["feature_name"] = df["category_code"].apply(map_feature)

    # ── Generate synthetic reliability metrics ────────────────
    df["latency_ms"] = df.apply(
        lambda r: generate_latency(r["event_type"], r["price"] if not pd.isna(r["price"]) else 100, r["feature_name"]),
        axis=1
    )
    df["crash_flag"] = df.apply(
        lambda r: generate_crash_flag(r["event_type"], r["latency_ms"]), axis=1
    )
    df["feedback_score"] = df.apply(
        lambda r: generate_feedback(r["event_type"], r["price"] if not pd.isna(r["price"]) else 100), axis=1
    ).clip(1.0, 5.0)
    df["error_count"] = df.apply(
        lambda r: generate_error_count(r["latency_ms"], r["crash_flag"]), axis=1
    )
    df["session_duration"] = df.apply(
        lambda r: generate_session_duration(r["event_type"], r["feature_name"]), axis=1
    )

   # ── Clean timestamp ───────────────────────────────────────
    df["timestamp"] = pd.to_datetime(df["event_time"].str.replace(" UTC", ""), errors="coerce")
    df = df.dropna(subset=["timestamp"])
    # Spread across 30 days for meaningful daily aggregation
    date_offsets = pd.to_timedelta(np.random.randint(0, 30, size=len(df)), unit='D')
    df["timestamp"] = (df["timestamp"] + date_offsets).dt.strftime("%Y-%m-%d %H:%M:%S")

    # ── Select final columns matching pipeline schema ─────────
    output = df[[
        "user_id", "feature_name", "session_duration",
        "latency_ms", "crash_flag", "error_count",
        "feedback_score", "timestamp"
    ]].copy()

    output["user_id"] = output["user_id"].astype(str)

    # ── Save ──────────────────────────────────────────────────
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(output_path, index=False)

    logger.info(f"✓ Blended dataset saved → {output_path}")
    logger.info(f"  Rows        : {len(output):,}")
    logger.info(f"  Features    : {output['feature_name'].value_counts().to_dict()}")
    logger.info(f"  Crash rate  : {output['crash_flag'].mean():.2%}")
    logger.info(f"  Avg latency : {output['latency_ms'].mean():.1f}ms")
    logger.info(f"  Avg feedback: {output['feedback_score'].mean():.2f}/5")
    logger.info(f"  Date range  : {output['timestamp'].min()} → {output['timestamp'].max()}")

    return output


if __name__ == "__main__":
    blend(KAGGLE_PATH, OUTPUT_PATH, SAMPLE_SIZE)