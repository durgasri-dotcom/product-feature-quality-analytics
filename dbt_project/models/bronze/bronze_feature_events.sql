{{
  config(
    materialized='view',
    description='Raw feature telemetry events — Bronze layer. No transformations, just type safety.'
  )
}}

SELECT
    -- Identity columns
    CAST(user_id AS VARCHAR)                         AS user_id,
    CAST(feature_name AS VARCHAR)                    AS feature_name,

    -- Performance metrics (cast to correct types)
    CAST(latency_ms AS DOUBLE)                       AS latency_ms,
    CAST(crash_flag AS INTEGER)                      AS crash_flag,
    CAST(feedback_score AS DOUBLE)                   AS feedback_score,
    CAST(error_count AS INTEGER)                     AS error_count,
    CAST(session_duration AS DOUBLE)                 AS session_duration,

    -- Timestamp handling
    CAST(timestamp AS TIMESTAMP)                     AS event_timestamp,
    CAST(timestamp AS DATE)                          AS event_date,

    -- Ingestion metadata (traceability)
    CURRENT_TIMESTAMP                                AS _bronze_loaded_at,
    'data/raw/product_logs.csv'                      AS _source_file,
    'bronze'                                         AS _layer

FROM read_csv_auto('data/raw/product_logs.csv', header=true)

-- Filter out completely null rows
WHERE user_id IS NOT NULL
  AND feature_name IS NOT NULL
