-- Dimension Table: Feature Metadata

CREATE TABLE dim_feature_metadata (
    feature_id STRING PRIMARY KEY,
    feature_name STRING,
    owner_team STRING,
    release_date DATE,
    lifecycle_stage STRING
);
