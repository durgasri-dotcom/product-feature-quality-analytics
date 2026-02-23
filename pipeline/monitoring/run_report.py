import json
import os
from datetime import datetime

def save_run_report(payload: dict) -> None:
    os.makedirs("artifacts/reports", exist_ok=True)
    payload["timestamp"] = datetime.utcnow().isoformat() + "Z"

    with open("artifacts/reports/run_report.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)