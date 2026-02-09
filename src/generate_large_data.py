import pandas as pd
import random
from datetime import datetime, timedelta

features = ["Login", "Search", "VideoPlayback", "Recommendations", "Payments"]

rows = []
start_date = datetime(2024, 1, 1)

for day in range(30):  # 30 days
    date = start_date + timedelta(days=day)

    for feature in features:
        events_per_day = random.randint(20, 50)  # BIG volume

        for _ in range(events_per_day):
            rows.append({
                "user_id": random.randint(1000, 20000),
                "feature_name": feature,
                "session_duration": random.randint(30, 600),
                "latency_ms": (
                    random.randint(80, 150) if feature == "Login"
                    else random.randint(200, 600)
                ),
                "crash_flag": random.choices([0, 1], weights=[0.9, 0.1])[0],
                "error_count": random.randint(0, 5),
                "feedback_score": random.randint(1, 5),
                "timestamp": date.date()
            })

df = pd.DataFrame(rows)
df.to_csv("data/raw/product_logs.csv", index=False)

print(f"Large dataset created: {len(df)} rows")
