-- Fact Table: Feature-Level Telemetry Metrics
-- Grain: One row per feature per day

CREATE TABLE fact_feature_metrics (
    feature_id STRING,
    event_date DATE,
    latency_ms FLOAT,
    crash_rate FLOAT,
    feedback_score FLOAT,
    active_users INT,
    risk_score FLOAT,
    anomaly_flag BOOLEAN,
    PRIMARY KEY (feature_id, event_date)
);
