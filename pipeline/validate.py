import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ── Raw data schema ───────────────────────────────────────────
RAW_REQUIRED_COLUMNS = [
    "user_id",
    "feature_name",
    "session_duration",
    "latency_ms",
    "crash_flag",
    "error_count",
    "feedback_score",
    "timestamp",
]

# ── Processed/aggregated data schema ─────────────────────────
PROCESSED_REQUIRED_COLUMNS = [
    "feature_name",
    "date",
    "crash_rate",
    "usage_count",
]

# ── Expected data types for each column ──────────────────────
COLUMN_DTYPE_EXPECTATIONS = {
    "latency_ms": "numeric",
    "crash_flag": "numeric",
    "feedback_score": "numeric",
    "error_count": "numeric",
    "session_duration": "numeric",
}

# ── Valid value ranges ────────────────────────────────────────
COLUMN_RANGE_EXPECTATIONS = {
    "latency_ms":     (0, 10_000),
    "crash_flag":     (0, 1),
    "feedback_score": (1, 5),
    "error_count":    (0, 1_000),
}


def validate_schema(df: pd.DataFrame, stage: str = "raw") -> pd.DataFrame:
    """
    Validates DataFrame schema and data quality.

    Args:
        df: DataFrame to validate
        stage: "raw" or "processed"

    Returns:
        The original DataFrame (unchanged) if all checks pass.
        Raises ValueError if critical checks fail.
    """
    if stage == "raw":
        required_columns = RAW_REQUIRED_COLUMNS
    elif stage == "processed":
        required_columns = PROCESSED_REQUIRED_COLUMNS
    else:
        raise ValueError(f"Invalid stage '{stage}'. Use 'raw' or 'processed'.")

    # ── CHECK 1: Required columns exist ──────────────────────
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"validate_schema [{stage}]: Missing required columns: {missing}")

    logger.info(f"validate_schema [{stage}]: All {len(required_columns)} required columns present")

    # ── CHECK 2: DataFrame is not empty ──────────────────────
    if df.empty:
        raise ValueError(f"validate_schema [{stage}]: DataFrame is empty — no rows to process")

    if len(df) < 10:
        logger.warning(f"validate_schema [{stage}]: Very few rows ({len(df)}) — results may be unreliable")

    # ── CHECK 3: Numeric columns have correct types ───────────
    if stage == "raw":
        for col, expected_type in COLUMN_DTYPE_EXPECTATIONS.items():
            if col not in df.columns:
                continue
            if expected_type == "numeric":
                if not pd.api.types.is_numeric_dtype(df[col]):
                    # Try to coerce instead of failing hard
                    try:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                        coerced_nulls = df[col].isnull().sum()
                        logger.warning(
                            f"validate_schema: '{col}' coerced to numeric — "
                            f"{coerced_nulls} values became NaN"
                        )
                    except Exception:
                        raise ValueError(
                            f"validate_schema: Column '{col}' cannot be converted to numeric"
                        )

    # ── CHECK 4: Values within expected ranges ────────────────
    if stage == "raw":
        range_violations = []
        for col, (min_val, max_val) in COLUMN_RANGE_EXPECTATIONS.items():
            if col not in df.columns:
                continue
            col_data = pd.to_numeric(df[col], errors="coerce").dropna()
            out_of_range = ((col_data < min_val) | (col_data > max_val)).sum()
            if out_of_range > 0:
                pct = out_of_range / len(col_data) * 100
                msg = (
                    f"'{col}': {out_of_range} values ({pct:.1f}%) "
                    f"outside range [{min_val}, {max_val}]"
                )
                range_violations.append(msg)
                logger.warning(f"validate_schema range warning: {msg}")

        # Only fail if MORE than 20% of values are out of range
        critical_violations = []
        for col, (min_val, max_val) in COLUMN_RANGE_EXPECTATIONS.items():
            if col not in df.columns:
                continue
            col_data = pd.to_numeric(df[col], errors="coerce").dropna()
            out_of_range = ((col_data < min_val) | (col_data > max_val)).sum()
            if len(col_data) > 0 and (out_of_range / len(col_data)) > 0.20:
                critical_violations.append(col)

        if critical_violations:
            raise ValueError(
                f"validate_schema: Critical range violations (>20% bad values) "
                f"in columns: {critical_violations}"
            )

    # ── CHECK 5: No fully null columns ───────────────────────
    fully_null = [col for col in required_columns if df[col].isnull().all()]
    if fully_null:
        raise ValueError(
            f"validate_schema [{stage}]: These columns are entirely null: {fully_null}"
        )

    logger.info(
        f"validate_schema [{stage}]: PASSED | "
        f"rows={len(df)} | columns={len(df.columns)}"
    )

    return df