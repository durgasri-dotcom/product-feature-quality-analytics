import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

try:
    from kafka import KafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
KAFKA_TOPIC = "feature-telemetry-events"
KAFKA_GROUP_ID = "analytics-pipeline-consumer"

# Bronze layer = raw data, partitioned by date
# This creates: data/bronze/date=2024-01-15/events.parquet
BRONZE_OUTPUT_PATH = Path("data/bronze")


def save_to_bronze(events: list) -> Optional[Path]:
    """
    Save a batch of events to the Bronze data layer as Parquet.
    
    WHY PARQUET? 
    - Much smaller file size than CSV (columnar compression)
    - 10-100x faster to query than CSV
    - Industry standard at FAANG companies
    
    WHY BRONZE?
    - Bronze = raw, unmodified data exactly as it arrived
    - Silver = cleaned and validated data  
    - Gold = business-ready aggregated data
    This is called the Medallion Architecture (used at Databricks/Netflix).
    """
    if not events:
        return None

    df = pd.DataFrame(events)

    # Partition by date (like a real data lake)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    partition_path = BRONZE_OUTPUT_PATH / f"date={today}"
    partition_path.mkdir(parents=True, exist_ok=True)

    # Add ingestion metadata
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    df["_source"] = "kafka_consumer"
    df["_partition_date"] = today

    # Save as Parquet (not CSV — this is production standard)
    ts = datetime.now(timezone.utc).strftime("%H%M%S")
    output_file = partition_path / f"events_{ts}.parquet"
    df.to_parquet(output_file, index=False, engine="pyarrow")

    logger.info(f"✅ Saved {len(df)} events → {output_file}")
    return output_file


def consume_from_kafka(max_messages: int = 500, timeout_ms: int = 5000) -> list:
    """
    Read messages from Kafka topic.
    
    Args:
        max_messages: Stop after reading this many messages
        timeout_ms: Stop if no new messages arrive within this time
    
    Returns:
        List of event dictionaries
    """
    if not KAFKA_AVAILABLE:
        logger.warning("kafka-python not installed — cannot consume from Kafka")
        return []

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset="earliest",           # Start from beginning if new consumer
            enable_auto_commit=True,                # Auto-commit offsets
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=timeout_ms,
        )
        logger.info(f"✅ Connected to Kafka topic '{KAFKA_TOPIC}'")
    except Exception as e:
        logger.error(f"❌ Cannot connect to Kafka: {e}")
        return []

    events = []
    batch = []
    BATCH_SIZE = 50  # Save to disk every 50 events (micro-batching)

    logger.info(f" Listening for events (max={max_messages}, timeout={timeout_ms}ms)...")

    try:
        for message in consumer:
            event = message.value
            batch.append(event)
            events.append(event)

            # Save in batches (efficient I/O)
            if len(batch) >= BATCH_SIZE:
                save_to_bronze(batch)
                batch = []
                logger.info(f" Progress: {len(events)}/{max_messages} events consumed")

            if len(events) >= max_messages:
                break

    except Exception as e:
        logger.error(f"Consumer error: {e}")
    finally:
        # Save any remaining events
        if batch:
            save_to_bronze(batch)
        consumer.close()

    logger.info(f"✅ Consumer done. Total events consumed: {len(events)}")
    return events


def consume_from_csv_fallback(csv_path: str = "data/raw/product_logs.csv") -> list:
    """
    FALLBACK: If Kafka is not running, read from your existing CSV file
    and save it to Bronze layer in Parquet format.
    
    This means your pipeline still works perfectly without Kafka running.
    """
    logger.info(f" FALLBACK MODE: Reading from CSV → {csv_path}")
    df = pd.read_csv(csv_path)
    events = df.to_dict(orient="records")
    save_to_bronze(events)
    logger.info(f"✅ Loaded {len(events)} events from CSV into Bronze layer")
    return events


# ─────────────────────────────────────────────

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("  KAFKA CONSUMER — BRONZE LAYER INGESTION")
    logger.info("=" * 60)

    if KAFKA_AVAILABLE:
        events = consume_from_kafka(max_messages=500)
        if not events:
            logger.info("No Kafka events — falling back to CSV")
            consume_from_csv_fallback()
    else:
        consume_from_csv_fallback()
