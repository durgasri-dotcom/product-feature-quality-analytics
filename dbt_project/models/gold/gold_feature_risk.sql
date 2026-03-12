{{
  config(
    materialized='table',
    description='Feature risk rankings and executive analytics — Gold layer.'
  )
}}

WITH latest_metrics AS (
    -- Get the most recent day's metrics for each feature
    SELECT *
    FROM {{ ref('silver_feature_metrics') }}
    WHERE recency_rank = 1
),

feature_risk_ranked AS (
    SELECT
        feature_name,
        event_date                                   AS latest_date,

        -- Core risk signals
        avg_latency,
        crash_rate,
        avg_feedback,
        usage_count,
        quality_score,

        -- Rolling trends
        rolling_7d_avg_latency,
        rolling_7d_crash_rate,
        latency_dod_change,

        -- Risk classification (rule-based, interpretable)
        CASE
            WHEN crash_rate > 0.10 OR avg_latency > 1000  THEN 'CRITICAL'
            WHEN crash_rate > 0.05 OR avg_latency > 500   THEN 'HIGH'
            WHEN crash_rate > 0.02 OR avg_latency > 200   THEN 'MEDIUM'
            ELSE 'LOW'
        END                                          AS risk_tier,

        -- Trend flag (is this feature getting worse?)
        CASE
            WHEN latency_dod_change > 100            THEN 'DEGRADING'
            WHEN latency_dod_change < -100           THEN 'IMPROVING'
            ELSE 'STABLE'
        END                                          AS trend_direction,

        -- Risk score 0-100 for easy ranking
        ROUND(
            (crash_rate * 40)                        -- Crash rate = 40% of score
            + (LEAST(avg_latency / 2000.0, 1) * 35) -- Latency = 35% of score
            + ((5 - avg_feedback) / 4.0 * 25),      -- Low feedback = 25% of score
            1
        )                                            AS composite_risk_score,

        -- Risk rank (1 = most at risk)
        RANK() OVER (
            ORDER BY
                crash_rate DESC,
                avg_latency DESC,
                avg_feedback ASC
        )                                            AS risk_rank,

        CURRENT_TIMESTAMP                            AS _gold_loaded_at

    FROM latest_metrics
),

with_slo_status AS (
    -- SLO = Service Level Objective
    -- Define what "acceptable" performance looks like
    -- This mirrors how SRE teams at Google track reliability
    SELECT
        *,
        CASE
            WHEN avg_latency <= 200 AND crash_rate <= 0.01 THEN TRUE
            ELSE FALSE
        END                                          AS meeting_slo,

        CASE
            WHEN avg_latency <= 200 AND crash_rate <= 0.01
            THEN 'Within SLO targets'
            WHEN avg_latency > 200 AND crash_rate <= 0.01
            THEN 'Latency SLO breach'
            WHEN avg_latency <= 200 AND crash_rate > 0.01
            THEN 'Crash rate SLO breach'
            ELSE 'Multiple SLO breaches'
        END                                          AS slo_status

    FROM feature_risk_ranked
)

SELECT
    -- Identity
    risk_rank,
    feature_name,
    latest_date,

    -- Risk summary
    risk_tier,
    composite_risk_score,
    trend_direction,

    -- Raw metrics
    ROUND(avg_latency, 2)                            AS avg_latency_ms,
    ROUND(crash_rate * 100, 3)                       AS crash_rate_pct,
    ROUND(avg_feedback, 2)                           AS avg_feedback_score,
    usage_count,

    -- Trend data
    ROUND(rolling_7d_avg_latency, 2)                 AS rolling_7d_latency,
    ROUND(rolling_7d_crash_rate * 100, 3)            AS rolling_7d_crash_pct,
    ROUND(latency_dod_change, 2)                     AS latency_change_vs_yesterday,

    -- Quality and SLO
    ROUND(quality_score * 100, 1)                    AS quality_score_pct,
    meeting_slo,
    slo_status,

    _gold_loaded_at,
    'gold'                                           AS _layer

FROM with_slo_status
ORDER BY composite_risk_score DESC
