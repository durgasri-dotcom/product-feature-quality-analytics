# Logs to ./mlflow_runs by default.
# Set MLFLOW_TRACKING_URI env variable to point to a remote server.
import logging
from pathlib import Path
from typing import Any, Dict, Optional

import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.ensemble import RandomForestClassifier

logger = logging.getLogger(__name__)

# Where MLflow stores all experiment data
MLFLOW_TRACKING_URI = "mlflow_runs"  # Local folder 
EXPERIMENT_NAME = "feature-quality-risk-scoring"


def setup_mlflow() -> None:
    """
    Initialize MLflow with local file storage.
    
    This creates a folder called 'mlflow_runs' in your project.
    You can view it by running: mlflow ui
    Then open your browser to: http://localhost:5000
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)
    logger.info(f"MLflow configured → tracking URI: {MLFLOW_TRACKING_URI}")


def log_training_run(
    model: RandomForestClassifier,
    metrics: Dict[str, Any],
    config: Any,
    feature_importance_df: Optional[pd.DataFrame] = None,
    run_name: Optional[str] = None,
) -> str:
    """
    Log a complete model training run to MLflow.
    
    This records everything about the training run so you can
    reproduce it or compare it with future runs.
    
    Args:
        model: The trained RandomForest model
        metrics: Dict of evaluation metrics (accuracy, auc, etc.)
        config: MLConfig dataclass with hyperparameters
        feature_importance_df: DataFrame of feature importances
        run_name: Optional name for this run (e.g., "baseline_v1")
    
    Returns:
        MLflow run_id string (unique ID for this run)
    """
    setup_mlflow()

    with mlflow.start_run(run_name=run_name or "training_run") as run:
        run_id = run.info.run_id

        # ── LOG HYPERPARAMETERS ───────────────────────────────
        # Settings used to train the model
        mlflow.log_params({
            "n_estimators": config.n_estimators,
            "max_depth": str(config.max_depth),
            "test_size": config.test_size,
            "random_state": config.random_state,
            "feature_cols": str(list(config.feature_cols)),
            "label_col": config.label_col,
        })

        # ── LOG METRICS ───────────────────────────────────────
        # Performance scores of the model
        metric_map = {
            "accuracy": metrics.get("accuracy"),
            "roc_auc": metrics.get("roc_auc"),
            "train_rows": metrics.get("train_rows"),
            "test_rows": metrics.get("test_rows"),
        }
        # Filter out None values
        metric_map = {k: v for k, v in metric_map.items() if v is not None}
        mlflow.log_metrics(metric_map)

        # ── LOG CONFUSION MATRIX ──────────────────────────────
        if "confusion_matrix" in metrics:
            cm = metrics["confusion_matrix"]
            mlflow.log_metrics({
                "true_positives": int(cm[1][1]),
                "true_negatives": int(cm[0][0]),
                "false_positives": int(cm[0][1]),
                "false_negatives": int(cm[1][0]),
            })

        # ── LOG FEATURE IMPORTANCE ────────────────────────────
        if feature_importance_df is not None:
            fi_path = "artifacts/reports/feature_importance.csv"
            Path("artifacts/reports").mkdir(parents=True, exist_ok=True)
            feature_importance_df.to_csv(fi_path, index=False)
            mlflow.log_artifact(fi_path, artifact_path="feature_importance")

        # ── LOG THE MODEL ITSELF ──────────────────────────────
        
        mlflow.sklearn.log_model(
            model,
            artifact_path="risk_model",
            registered_model_name="FeatureRiskScorer",  # Registers in Model Registry
        )

        # ── LOG TAGS (searchable labels) ─────────────────────
        mlflow.set_tags({
            "model_type": "RandomForestClassifier",
            "project": "product-feature-quality-analytics",
            "data_layer": "silver",
        })

        logger.info(f" MLflow run logged | run_id={run_id} | "
                   f"AUC={metric_map.get('roc_auc', 'N/A'):.4f}" 
                   if 'roc_auc' in metric_map else
                   f" MLflow run logged | run_id={run_id}")

    return run_id


def load_best_model() -> Optional[RandomForestClassifier]:
    """
    Load the best model from MLflow Model Registry.
    
    Returns the model with the highest AUC score from all runs.
    Returns None if no runs exist yet.
    """
    setup_mlflow()

    try:
        client = mlflow.tracking.MlflowClient()
        experiment = client.get_experiment_by_name(EXPERIMENT_NAME)

        if experiment is None:
            logger.warning("No MLflow experiment found yet — run training first")
            return None

        # Get all runs, sorted by AUC (best first)
        runs = client.search_runs(
            experiment_ids=[experiment.experiment_id],
            order_by=["metrics.roc_auc DESC"],
            max_results=1,
        )

        if not runs:
            logger.warning("No training runs found in MLflow")
            return None

        best_run = runs[0]
        best_auc = best_run.data.metrics.get("roc_auc", "N/A")
        logger.info(f"Loading best model | run_id={best_run.info.run_id} | AUC={best_auc}")

        model = mlflow.sklearn.load_model(f"runs:/{best_run.info.run_id}/risk_model")
        return model

    except Exception as e:
        logger.error(f"Could not load model from MLflow: {e}")
        return None
