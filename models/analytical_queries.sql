-- 7-Day Rolling Latency

SELECT
    feature_id,
    event_date,
    AVG(latency_ms) OVER (
        PARTITION BY feature_id
        ORDER BY event_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_latency
FROM fact_feature_metrics;


-- Identify High-Risk Features

SELECT
    f.feature_id,
    d.owner_team,
    AVG(risk_score) AS avg_risk
FROM fact_feature_metrics f
JOIN dim_feature_metadata d
    ON f.feature_id = d.feature_id
GROUP BY 1,2
HAVING AVG(risk_score) > 0.75;
