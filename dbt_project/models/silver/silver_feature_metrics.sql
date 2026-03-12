{{
  config(
    materialized='table',
    description='Daily feature-level metrics — Silver layer. Cleaned and aggregated.'
  )
}}

WITH daily_aggregated AS (
    -- Step 1: Aggregate raw events to feature-day grain
    -- (one row per feature per day)
    SELECT
        feature_name,
        event_date,

        -- Core reliability metrics
        AVG(latency_ms)                              AS avg_latency,
        AVG(crash_flag)                              AS crash_rate,
        AVG(feedback_score)                          AS avg_feedback,
        AVG(error_count)                             AS avg_error_count,
        COUNT(*)                                     AS usage_count,
        SUM(crash_flag)                              AS total_crashes,

        -- Session quality
        AVG(session_duration)                        AS avg_session_duration,

        -- Data completeness
        COUNT(CASE WHEN latency_ms IS NULL THEN 1 END) AS null_latency_count,

        CURRENT_TIMESTAMP                            AS _silver_loaded_at

    FROM {{ ref('bronze_feature_events') }}   -- ref() is how dbt links models
    GROUP BY feature_name, event_date
),

with_quality_score AS (
    -- Step 2: Compute composite quality score (0 to 1, higher = better)
    SELECT
        *,
        ROUND(
            (
                -- Normalize latency: lower is better
                (1.0 / (1.0 + avg_latency / 1000.0))
                -- Invert crash rate: lower crashes = higher score
                + (1.0 - crash_rate)
                -- Normalize feedback: already 1-5, map to 0-1
                + (avg_feedback / 5.0)
            ) / 3.0,
            4
        )                                            AS quality_score

    FROM daily_aggregated
),

with_rolling_stats AS (
    -- Step 3: Add 7-day rolling statistics
    -- This shows TRENDS, not just current values
    SELECT
        *,

        -- 7-day rolling average latency
        AVG(avg_latency) OVER (
            PARTITION BY feature_name
            ORDER BY event_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )                                            AS rolling_7d_avg_latency,

        -- 7-day rolling crash rate
        AVG(crash_rate) OVER (
            PARTITION BY feature_name
            ORDER BY event_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )                                            AS rolling_7d_crash_rate,

        -- Day-over-day latency change
        avg_latency - LAG(avg_latency, 1) OVER (
            PARTITION BY feature_name
            ORDER BY event_date
        )                                            AS latency_dod_change,

        -- Row number for ranking
        ROW_NUMBER() OVER (
            PARTITION BY feature_name ORDER BY event_date DESC
        )                                            AS recency_rank

    FROM with_quality_score
)

SELECT
    feature_name,
    event_date,
    avg_latency,
    crash_rate,
    avg_feedback,
    avg_error_count,
    usage_count,
    total_crashes,
    avg_session_duration,
    quality_score,
    rolling_7d_avg_latency,
    rolling_7d_crash_rate,
    latency_dod_change,
    recency_rank,
    _silver_loaded_at,
    'silver'                                         AS _layer
FROM with_rolling_stats
