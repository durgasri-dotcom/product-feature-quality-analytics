from ingest import load_raw_data
from validate import validate_schema
from transform import engineer_features
from aggregate import aggregate_daily
from quality_checks import check_null_rates, check_latency_outliers

from score import MLConfig, create_target, train_model, score_dataframe, save_artifacts

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
    logger.info(f"Row count: {len(df)}")
    logger.info(f"Column count: {len(df.columns)}")
    logger.info(f"Columns: {list(df.columns)}")

    if "latency_ms" in df.columns:
        logger.info(
            f"Latency stats | min: {df['latency_ms'].min()}, "
            f"max: {df['latency_ms'].max()}, "
            f"mean: {df['latency_ms'].mean():.2f}"
        )


def run():
    start_time = time.time()

    try:
        logger.info("========== PIPELINE STARTED ==========")

        # ---------- Ingestion ----------
        df = load_raw_data(RAW_PATH)
        df = validate_schema(df, stage="raw")
        logger.info("Raw data loaded successfully")
        log_data_profile(df)

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
        logger.info("Daily aggregation completed")

        # Validate processed schema (minimal)
        df = validate_schema(df, stage="processed")
        logger.info("Processed schema validation passed")

        # ---------- ML Layer ----------
        logger.info("Starting ML risk modeling")
        config = MLConfig()

        df = create_target(df, config)
        model, metrics = train_model(df, config)
        df = score_dataframe(df, model, config)

        logger.info("ML layer completed")
        logger.info(f"Metrics: AUC={metrics.get('auc')}, ACC={metrics.get('accuracy')}")

        # Save model + metrics + feature importance baseline
        save_artifacts(model, metrics, df, config)
        logger.info("Artifacts saved to /artifacts")

        # ---------- Persist Output ----------
        df.to_csv(OUTPUT_PATH, index=False)
        logger.info(f"Processed data saved to {OUTPUT_PATH}")

        runtime = round(time.time() - start_time, 2)
        logger.info(f"Pipeline runtime: {runtime} seconds")
        logger.info("========== PIPELINE COMPLETED SUCCESSFULLY ==========")

    except Exception as e:
        logger.error("========== PIPELINE FAILED ==========")
        logger.error(f"Failure reason: {str(e)}")
        raise


if __name__ == "__main__":
    run()