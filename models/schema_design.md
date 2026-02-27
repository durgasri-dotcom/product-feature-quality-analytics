# Data Modeling Overview

This project follows a star schema design to support scalable analytics.

## Fact Table

fact_feature_metrics

- Grain: One row per feature per day
- Stores operational telemetry and computed risk signals

## Dimension Table

dim_feature_metadata

- Describes feature attributes
- Enables slicing metrics by owner, lifecycle stage, etc.

This modeling approach enables:

- Time-series analysis
- Risk trend detection
- Aggregations at team or lifecycle level
- Efficient analytical querying
