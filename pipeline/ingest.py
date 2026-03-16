import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import structlog

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)
logger = structlog.get_logger(__name__)

# Data layer paths (Medallion Architecture)
BRONZE_PATH = Path("data/bronze")
SILVER_PATH = Path("data/silver")
GOLD_PATH = Path("data/gold")


def load_raw_data(path: str) -> pd.DataFrame:
    """
    Load raw telemetry data from CSV (existing format).

    Args:
        path: Path to your CSV file (e.g., "data/raw/product_logs.csv")
    
    Returns:
        DataFrame with raw telemetry data
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {path}")

    df = pd.read_csv(path)

    logger.info("raw_data_loaded",
                source=str(path),
                rows=len(df),
                columns=list(df.columns))

    # Auto-save to Bronze layer every time we load
    _save_to_bronze(df, source=str(path))

    return df


def load_bronze_data(date: Optional[str] = None) -> pd.DataFrame:
    """
    Load data from the Bronze layer (Parquet files).
    
    WHY USE THIS?
    - Much faster than reading CSV every time
    - Parquet files are 5-10x smaller than CSV
    - Supports reading just one day's data (partitioning)
    
    Args:
        date: Optional date string like "2024-01-15"
              If None, loads all available Bronze data
    
    Returns:
        DataFrame with Bronze layer data
    """
    if date:
        partition_path = BRONZE_PATH / f"date={date}"
        if not partition_path.exists():
            logger.warning("bronze_partition_not_found", date=date)
            return pd.DataFrame()
        parquet_files = list(partition_path.glob("*.parquet"))
    else:
        parquet_files = list(BRONZE_PATH.rglob("*.parquet"))

    if not parquet_files:
        logger.warning("no_bronze_files_found", path=str(BRONZE_PATH))
        return pd.DataFrame()

    # Read all Parquet files and combine
    dfs = [pd.read_parquet(f) for f in parquet_files]
    df = pd.concat(dfs, ignore_index=True)

    logger.info("bronze_data_loaded",
                files_read=len(parquet_files),
                total_rows=len(df))

    return df


def _save_to_bronze(df: pd.DataFrame, source: str = "csv") -> Path:
    """
    Internal: Save a DataFrame to the Bronze layer as Parquet.

    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    partition_path = BRONZE_PATH / f"date={today}"
    partition_path.mkdir(parents=True, exist_ok=True)

    # Adding ingestion metadata 
    df_bronze = df.copy()
    df_bronze["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    df_bronze["_source"] = source
    df_bronze["_layer"] = "bronze"

    ts = datetime.now(timezone.utc).strftime("%H%M%S")
    output_path = partition_path / f"raw_events_{ts}.parquet"
    df_bronze.to_parquet(output_path, index=False, engine="pyarrow")

    logger.info("bronze_layer_saved",
                path=str(output_path),
                rows=len(df_bronze))

    return output_path


def get_pipeline_metadata() -> dict:
    """
    Return metadata about available data layers.

    """
    bronze_files = list(BRONZE_PATH.rglob("*.parquet"))
    silver_files = list(SILVER_PATH.rglob("*.parquet"))
    gold_files = list(GOLD_PATH.rglob("*.parquet"))

    return {
        "bronze_files": len(bronze_files),
        "silver_files": len(silver_files),
        "gold_files": len(gold_files),
        "bronze_size_mb": sum(f.stat().st_size for f in bronze_files) / 1e6,
        "last_ingested": datetime.now(timezone.utc).isoformat(),
    }
