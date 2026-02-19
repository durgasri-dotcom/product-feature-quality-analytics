from ingest import load_raw_data
from validate import validate_schema
from transform import engineer_features
from aggregate import aggregate_daily
from quality_checks import check_null_rates, check_latency_outliers

import logging
import time

# ---------------- Logging Configuration ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# ---------------- Config ----------------
RAW_PATH = "data/raw/product_logs.csv"
OUTPUT_PATH = "data/processed/feature_metrics.csv"


def log_data_profile(df):
    """
    Logs lightweight profiling information for observability.
    """
    logger.info(f"Row count: {len(df)}")
    logger.info(f"Column count: {len(df.columns)}")
    logger.info(f"Columns: {list(df.columns)}")

    if "date" in df.columns:
        logger.info(
            f"Date range: {df['date'].min()} to {df['date'].max()}"
        )

    if "latency_ms" in df.columns:
        logger.info(
            f"Latency stats | min: {df['latency_ms'].min()}, "
            f"max: {df['latency_ms'].max()}, "
            f"mean: {df['latency_ms'].mean():.2f}"
        )

    # Null distribution
    null_summary = df.isnull().mean()
    logger.info(f"Null rate summary:\n{null_summary}")

    # Memory footprint
    memory_mb = df.memory_usage(deep=True).sum() / (1024 ** 2)
    logger.info(f"Approx memory usage: {memory_mb:.2f} MB")


def run():
    start_time = time.time()

    try:
        logger.info("========== PIPELINE STARTED ==========")

        # ---------- Ingestion ----------
        df = load_raw_data(RAW_PATH)
        df = validate_schema(df, stage="raw")
        logger.info("Raw data loaded successfully")
        log_data_profile(df)

        # ---------- Schema Validation ----------
        df = validate_schema(df, stage="raw")
        logger.info("Schema validation passed")

        # ---------- Data Quality Checks ----------
        check_null_rates(df)
        logger.info("Null rate validation passed")

        check_latency_outliers(df)
        logger.info("Latency threshold validation passed")

        # ---------- Feature Engineering ----------
        df = engineer_features(df)
        logger.info("Feature engineering completed")

        # ---------- Aggregation ----------
        df = aggregate_daily(df)
        df = validate_schema(df, stage="processed")

        logger.info("Daily aggregation completed")
        logger.info(f"Aggregated row count: {len(df)}")

        # ---------- Persist Output ----------
        logger.info("Overwriting existing output if present (idempotent batch execution)")
        df.to_csv(OUTPUT_PATH, index=False)
        logger.info(f"Processed data saved to {OUTPUT_PATH}")

        # ---------- Execution Metadata ----------
        runtime = round(time.time() - start_time, 2)
        logger.info(f"Pipeline runtime: {runtime} seconds")

        logger.info("========== PIPELINE COMPLETED SUCCESSFULLY ==========")

    except Exception as e:
        logger.error("========== PIPELINE FAILED ==========")
        logger.error(f"Failure reason: {str(e)}")
        raise


if __name__ == "__main__":
    run()
