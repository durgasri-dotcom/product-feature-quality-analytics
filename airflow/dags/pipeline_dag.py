from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# ─────────────────────────────────────────────
# DEFAULT ARGS — applied to every task in the DAG
# ─────────────────────────────────────────────
default_args = {
    "owner": "abhigna",                    # Your name
    "depends_on_past": False,              # Each run is independent
    "email_on_failure": False,             # Set to True + add email for alerts
    "email_on_retry": False,
    "retries": 2,                          # Retry failed tasks 2 times
    "retry_delay": timedelta(minutes=5),   # Wait 5 minutes between retries
    "start_date": days_ago(1),
}

# ─────────────────────────────────────────────
# DAG DEFINITION
# schedule_interval="0 6 * * *" means: run every day at 6:00 AM UTC
# This is cron syntax: minute hour day month weekday
# ─────────────────────────────────────────────
dag = DAG(
    dag_id="feature_quality_analytics_pipeline",
    default_args=default_args,
    description="Daily Feature Quality Analytics Pipeline — FAANG-level orchestration",
    schedule_interval="0 6 * * *",    # Every day at 6 AM UTC
    catchup=False,                     # Don't run missed historical runs
    tags=["analytics", "ml", "monitoring", "data-engineering"],
    doc_md="""
    ## Feature Quality Analytics Pipeline
    
    This DAG orchestrates the complete daily analytics pipeline:
    
    1. **Kafka Ingest** — Consume latest telemetry events from Kafka
    2. **Data Quality** — Run Great Expectations validation suite  
    3. **Transform** — Feature engineering (Bronze → Silver)
    4. **ML Scoring** — Risk scoring with RandomForest + SHAP
    5. **Drift Detection** — Statistical drift analysis (KS + PSI)
    6. **dbt Models** — Build Gold layer analytical models
    7. **Run Report** — Generate pipeline summary report
    
    **Owner**: Abhigna  
    **SLA**: Must complete within 30 minutes of schedule
    """,
)


# ─────────────────────────────────────────────
# TASK FUNCTIONS
# Each function is one step in the pipeline.
# Airflow runs them in order, handles retries if they fail.
# ─────────────────────────────────────────────

def task_ingest_data(**context):
    """
    Task 1: Ingest data from Kafka (or CSV fallback).
    The 'context' parameter gives access to Airflow run metadata.
    """
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    from kafka.consumer import consume_from_kafka, consume_from_csv_fallback

    events = consume_from_kafka(max_messages=1000, timeout_ms=10000)
    if not events:
        print("Kafka not available — using CSV fallback")
        events = consume_from_csv_fallback()

    # XCom = Airflow's way to pass data between tasks
    context["ti"].xcom_push(key="event_count", value=len(events))
    print(f"✅ Ingested {len(events)} events")
    return len(events)


def task_data_quality(**context):
    """Task 2: Run data quality checks with Great Expectations suite."""
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    from pipeline.ingest import load_raw_data
    from pipeline.quality_checks import run_great_expectations_suite

    df = load_raw_data("data/raw/product_logs.csv")
    report = run_great_expectations_suite(df)

    if not report["overall_passed"]:
        failed_checks = report["failed"]
        raise ValueError(
            f"Data quality FAILED: {len(failed_checks)} checks failed. "
            f"Failures: {[f['expectation'] for f in failed_checks]}"
        )

    context["ti"].xcom_push(key="quality_score", value=report["score_pct"])
    print(f"✅ Data quality passed: {report['score_pct']}%")


def task_transform(**context):
    """Task 3: Feature engineering and aggregation (Bronze → Silver)."""
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    from pipeline.ingest import load_raw_data
    from pipeline.transform import engineer_features
    from pipeline.aggregate import aggregate_daily

    df = load_raw_data("data/raw/product_logs.csv")
    df = engineer_features(df)
    df_agg = aggregate_daily(df)

    context["ti"].xcom_push(key="row_count_after_transform", value=len(df_agg))
    print(f"✅ Transform complete: {len(df_agg)} feature-day rows")


def task_ml_scoring(**context):
    """Task 4: Train model and score features with risk probabilities."""
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    from pipeline.ingest import load_raw_data
    from pipeline.transform import engineer_features
    from pipeline.aggregate import aggregate_daily
    from pipeline.score import MLConfig, create_target, train_model, score_dataframe, save_artifacts
    import pandas as pd

    df = load_raw_data("data/raw/product_logs.csv")
    df = engineer_features(df)
    df_agg = aggregate_daily(df)

    config = MLConfig()
    df_agg = create_target(df_agg, config)
    model, metrics = train_model(df_agg, config)
    df_scored = score_dataframe(df_agg, model, config)

    fi = pd.DataFrame({
        "feature": list(config.feature_cols),
        "importance": model.feature_importances_
    }).sort_values("importance", ascending=False)
    save_artifacts(model, metrics, fi)

    context["ti"].xcom_push(key="model_auc", value=metrics.get("roc_auc"))
    print(f"✅ ML scoring complete | AUC={metrics.get('roc_auc', 'N/A'):.4f}")


def task_drift_detection(**context):
    """Task 5: Run statistical drift detection (KS + PSI + % change)."""
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    from pipeline.ingest import load_raw_data
    from pipeline.monitoring.baseline import compute_baseline, load_baseline, save_baseline
    from pipeline.monitoring.drift import detect_data_drift, save_data_drift

    df = load_raw_data("data/raw/product_logs.csv")
    baseline = load_baseline()
    current_stats = compute_baseline(df)

    if baseline is None:
        save_baseline(current_stats)
        print("✅ First run — baseline created")
        return

    report = detect_data_drift(df, baseline, threshold=0.20, baseline_df=None)
    save_data_drift(report)

    alert_count = report.get("alert_count", 0)
    context["ti"].xcom_push(key="drift_alerts", value=alert_count)
    print(f"✅ Drift detection complete | Alerts: {alert_count}")


def task_run_report(**context):
    """Task 7: Generate final pipeline run report."""
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    from pipeline.monitoring.run_report import save_run_report

    # Gather XCom values from previous tasks
    ti = context["ti"]
    report_data = {
        "event_count": ti.xcom_pull(task_ids="ingest_data", key="event_count"),
        "quality_score": ti.xcom_pull(task_ids="data_quality", key="quality_score"),
        "model_auc": ti.xcom_pull(task_ids="ml_scoring", key="model_auc"),
        "drift_alerts": ti.xcom_pull(task_ids="drift_detection", key="drift_alerts"),
        "dag_run_id": context["run_id"],
        "execution_date": str(context["execution_date"]),
    }

    save_run_report(report_data)
    print(f"✅ Run report saved: {report_data}")


# ─────────────────────────────────────────────
# DEFINE TASKS (connect functions to Airflow)
# ─────────────────────────────────────────────

t1_ingest = PythonOperator(
    task_id="ingest_data",
    python_callable=task_ingest_data,
    dag=dag,
)

t2_quality = PythonOperator(
    task_id="data_quality",
    python_callable=task_data_quality,
    dag=dag,
)

t3_transform = PythonOperator(
    task_id="transform",
    python_callable=task_transform,
    dag=dag,
)

t4_ml_scoring = PythonOperator(
    task_id="ml_scoring",
    python_callable=task_ml_scoring,
    dag=dag,
)

t5_drift = PythonOperator(
    task_id="drift_detection",
    python_callable=task_drift_detection,
    dag=dag,
)

# dbt runs as a bash command (standard practice)
t6_dbt = BashOperator(
    task_id="dbt_models",
    bash_command="cd dbt_project && dbt run --profiles-dir . && dbt test --profiles-dir .",
    dag=dag,
)

t7_report = PythonOperator(
    task_id="run_report",
    python_callable=task_run_report,
    dag=dag,
)

# ─────────────────────────────────────────────
# TASK ORDER (this is the pipeline flow)
# >> means "then run"
# ─────────────────────────────────────────────
t1_ingest >> t2_quality >> t3_transform >> t4_ml_scoring >> t5_drift >> t6_dbt >> t7_report
