import os
import sys
import logging
import time
from datetime import datetime, timezone
from pathlib import Path


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ── Logging setup ─────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/pipeline.log", mode="a", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ── Imports  ──────────────────────────
from ingest import load_raw_data
from validate import validate_schema
from transform import engineer_features
from aggregate import aggregate_daily
from quality_checks import check_null_rates, check_latency_outliers, run_great_expectations_suite
from score import MLConfig, create_target, train_model, score_dataframe, save_artifacts, compute_shap_values
from monitoring.baseline import compute_baseline, save_baseline, load_baseline
from monitoring.drift import detect_data_drift, save_data_drift
from monitoring.run_report import save_run_report

import pandas as pd

# ── Config  ───────────────────────────
RAW_PATH = "data/raw/product_logs.csv"
OUTPUT_PATH = "data/processed/feature_metrics.csv"


def log_data_profile(df):
    """Your original function — kept exactly the same."""
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
        logger.info(f"Pipeline started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # ── STEP 1: Ingestion ─────────────────────────────────
        
        logger.info("---------- STEP 1: INGESTION ----------")
        df = load_raw_data(RAW_PATH)
        df = validate_schema(df, stage="raw")
        logger.info("Raw data loaded successfully")
        log_data_profile(df)

        # ── STEP 2: Baseline + Drift Detection ───────────────
        
        logger.info("---------- STEP 2: DRIFT DETECTION ----------")
        baseline = load_baseline()
        current_stats = compute_baseline(df)

        if baseline is None:
            logger.info("No baseline found. Creating baseline_stats.json (first run only).")
            save_baseline(current_stats)
            drift_report = {"alerts": [], "alert_count": 0, "overall_drift_detected": False}
        else:
            drift_report = detect_data_drift(df, baseline, threshold=0.20)
            save_data_drift(drift_report)

            if drift_report["alerts"]:
                logger.warning("DATA DRIFT ALERTS:")
                for a in drift_report["alerts"]:
                    logger.warning(f"  → {a}")
            else:
                logger.info("No significant data drift detected.")

        # ── STEP 3: Data Quality Checks ──────────────────────
        
        logger.info("---------- STEP 3: DATA QUALITY ----------")
        quality_report = run_great_expectations_suite(df)
        logger.info(
            f"Quality suite | score={quality_report['score_pct']}% | "
            f"passed={quality_report['passed']}/{quality_report['total_checks']}"
        )

        
        check_null_rates(df)
        logger.info("Null rate validation passed")

        check_latency_outliers(df)
        logger.info("Latency threshold validation passed")

        # ── STEP 4: Feature Engineering ──────────────────────
        
        logger.info("---------- STEP 4: FEATURE ENGINEERING ----------")
        df = engineer_features(df)
        logger.info("Feature engineering completed")

        # ── STEP 5: Aggregation ───────────────────────────────
        
        logger.info("---------- STEP 5: AGGREGATION ----------")
        df = aggregate_daily(df)
        logger.info("Daily aggregation completed")

        df = validate_schema(df, stage="processed")
        logger.info("Processed schema validation passed")

        # ── STEP 6: ML Risk Scoring ───────────────────────────
        
        logger.info("---------- STEP 6: ML RISK SCORING ----------")
        config = MLConfig()
        df = create_target(df, config)
        model, metrics = train_model(df, config)
        df = score_dataframe(df, model, config)
        logger.info("ML layer completed")
        logger.info(
            f"Metrics | AUC={metrics.get('roc_auc', 'N/A')} | "
            f"ACC={metrics.get('accuracy', 'N/A')}"
        )

        # ── STEP 7: Save Artifacts ────────────────────────────
    
        logger.info("---------- STEP 7: SAVE ARTIFACTS ----------")
        fi = pd.DataFrame({
            "feature": list(config.feature_cols),
            "importance": model.feature_importances_,
        }).sort_values("importance", ascending=False)

        save_artifacts(model, metrics, fi)
        logger.info("Artifacts saved to /artifacts")

        # ── STEP 8: SHAP Explainability ───────────────────────
        
        logger.info("---------- STEP 8: SHAP EXPLAINABILITY ----------")
        shap_df = compute_shap_values(model, df, config)
        if shap_df is not None:
            logger.info(f"SHAP complete | top feature: {shap_df.iloc[0]['feature']}")
        else:
            logger.info("SHAP skipped (run: pip install shap to enable)")

        # ── STEP 9: Save Output ───────────────────────────────
        
        logger.info("---------- STEP 9: SAVE OUTPUT ----------")
        os.makedirs("data/processed", exist_ok=True)
        df.to_csv(OUTPUT_PATH, index=False)
        logger.info(f"Processed data saved to {OUTPUT_PATH}")

        runtime = round(time.time() - start_time, 2)
        logger.info(f"Pipeline runtime: {runtime} seconds")
        logger.info("========== PIPELINE COMPLETED SUCCESSFULLY ==========")

        # ── STEP 10: Run Report ───────────────────────────────
        
        save_run_report({
            "status": "success",
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
            "rows_processed": int(len(df)),
            "output_path": OUTPUT_PATH,
            "runtime_seconds": runtime,
            "ml_auc": metrics.get("roc_auc"),
            "ml_accuracy": metrics.get("accuracy"),
            "quality_score_pct": quality_report["score_pct"],
            "quality_checks_passed": quality_report["passed"],
            "drift_alerts": drift_report.get("alert_count", 0),
            "drift_detected": drift_report.get("overall_drift_detected", False),
        })

       

    except Exception as e:
        logger.info(f"Pipeline finished in {runtime:.2f}s — {rows_processed} rows processed")
        logger.error(f"Failure reason: {str(e)}", exc_info=True)

        save_run_report({
            "status": "failed",
            "error": str(e),
        })
        raise


if __name__ == "__main__":
    run()