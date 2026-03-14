# Simulates real-time product feature telemetry at ~1 event/sec.
# Falls back to CSV replay mode if Kafka broker is unavailable.
import json
import random
import time
import logging
from datetime import datetime, timezone
from typing import Optional

# --- Try to import Kafka; fall back to simulation mode if not running ---
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
KAFKA_TOPIC = "feature-telemetry-events"

# Simulated product features 
FEATURE_NAMES = [
    "search_ranking",
    "recommendation_engine",
    "checkout_flow",
    "video_player",
    "notification_service",
    "auth_service",
    "payment_gateway",
    "content_delivery",
    "user_profile",
    "ab_test_controller",
]


def generate_telemetry_event(feature_name: Optional[str] = None) -> dict:
    """
    Generate one realistic telemetry event for a product feature.
    
    In production, this data would come from your actual app servers.
    Here, we simulate it with realistic random values.
    
    Returns a dictionary (JSON-serializable) event.
    """
    feature = feature_name or random.choice(FEATURE_NAMES)

    # Simulate occasional degradation (10% of events are "bad")
    is_degraded = random.random() < 0.10

    if is_degraded:
        latency_ms = random.uniform(800, 3000)   # High latency = bad
        crash_flag = random.choices([0, 1], weights=[0.6, 0.4])[0]
        feedback_score = round(random.uniform(1.0, 2.5), 2)
        error_count = random.randint(3, 10)
    else:
        latency_ms = random.uniform(50, 300)      # Normal latency
        crash_flag = random.choices([0, 1], weights=[0.97, 0.03])[0]
        feedback_score = round(random.uniform(3.5, 5.0), 2)
        error_count = random.randint(0, 1)

    event = {
        "event_id": f"evt_{random.randint(100000, 999999)}",
        "user_id": f"user_{random.randint(1000, 9999)}",
        "feature_name": feature,
        "session_duration": round(random.uniform(10, 600), 2),
        "latency_ms": round(latency_ms, 2),
        "crash_flag": crash_flag,
        "error_count": error_count,
        "feedback_score": feedback_score,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "region": random.choice(["us-east-1", "us-west-2", "eu-west-1"]),
        "platform": random.choice(["ios", "android", "web"]),
        "app_version": random.choice(["3.1.0", "3.2.0", "3.3.0-beta"]),
    }
    return event


def create_kafka_producer() -> Optional["KafkaProducer"]:
    """
    Create and return a Kafka producer.
    Returns None if Kafka is not available (simulation mode).
    """
    if not KAFKA_AVAILABLE:
        logger.warning("kafka-python not installed. Running in SIMULATION MODE.")
        return None

    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",             # Wait for all replicas to confirm (reliability)
            retries=3,              # Retry on failure
            linger_ms=10,           # Small batching delay for efficiency
        )
        logger.info(f"✅ Kafka producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        logger.error(f"❌ Could not connect to Kafka: {e}")
        logger.info("Falling back to SIMULATION MODE (logging events only)")
        return None


def produce_events(
    num_events: int = 100,
    delay_seconds: float = 0.1,
    feature_name: Optional[str] = None,
) -> list:
    """
    Produce telemetry events and send to Kafka topic.
    
    Args:
        num_events: How many events to produce
        delay_seconds: Delay between each event (simulates real-time pace)
        feature_name: Optional — target a specific feature
    
    Returns:
        List of all events produced (useful for testing)
    """
    producer = create_kafka_producer()
    events_produced = []

    logger.info(f" Starting event production: {num_events} events for topic '{KAFKA_TOPIC}'")

    for i in range(num_events):
        event = generate_telemetry_event(feature_name)
        events_produced.append(event)

        if producer:
            # Send to Kafka — key by feature_name so same feature always
            # goes to same partition (ordering guarantee)
            producer.send(
                topic=KAFKA_TOPIC,
                key=event["feature_name"],
                value=event,
            )
        else:
            # Simulation mode: just log the event
            logger.info(f"[SIM] Event {i+1}/{num_events}: {event['feature_name']} | "
                       f"latency={event['latency_ms']}ms | crash={event['crash_flag']}")

        if delay_seconds > 0:
            time.sleep(delay_seconds)

    if producer:
        producer.flush()  # Make sure all messages are sent before exiting
        logger.info(f"✅ Flushed {num_events} events to Kafka topic '{KAFKA_TOPIC}'")
    else:
        logger.info(f"✅ Simulated {num_events} events (Kafka not connected)")

    return events_produced


# ─────────────────────────────────────────────

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("  KAFKA TELEMETRY PRODUCER — FAANG-LEVEL DATA INGESTION")
    logger.info("=" * 60)
    produce_events(num_events=200, delay_seconds=0.05)
