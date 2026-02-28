# Product Feature Quality & Performance Analytics Platform

Batch-based reliability analytics and ML risk scoring pipeline operating at feature-day grain.

Implements telemetry ingestion, aggregation, supervised risk modeling (RandomForest), statistical drift monitoring, artifact persistence, CI validation, and dashboard visualization.

# System Overview

```markdown
Telemetry → Validation → Aggregation → ML Risk Scoring → Drift Monitoring → Dashboard
```

# Dashboard Preview

![Dashboard Demo](assets/dashboard_demo.gif)

---

# Problem Context

Large-scale consumer platforms operate hundreds of product features simultaneously.

Small degradations in latency, crash rate, or user experience often go unnoticed until they affect millions of users.

Engineering teams require systems that can:

- Detect early warning signals
- Quantify feature-level reliability risk
- Prioritize engineering effort based on impact
- Monitor data quality and pipeline health
- Provide actionable insights through dashboards

This project simulates how production analytics systems solve these problems.

---

## Business Impact Simulation

If deployed in a production consumer platform, this system would:

- Detect feature reliability degradation before large-scale user impact
- Reduce incident detection latency through automated risk scoring
- Prioritize engineering effort using quantified risk probability
- Improve operational visibility via structured monitoring artifacts
- Support data-driven reliability reviews with feature-level dashboards

This mirrors reliability analytics workflows used in large-scale platforms such as streaming, ride-sharing, or e-commerce systems.

---

# Architecture

flowchart TD

```mermaid

    A[Raw Telemetry<br>data/raw/product_logs.csv]

    B[Ingest]
    C[Validate Schema]
    D[Quality Checks]
    E[Feature Engineering]
    F[Aggregate to Feature-Day]

    G[Target Engineering]
    H[RandomForest Training]
    I[Risk Scoring]

    J[Baseline Stats]
    K[Drift Detection]
    L[Run Report]

    M1[risk_model.joblib]
    M2[metrics.json]
    M3[feature_importance.csv]
    M4[data_drift.json]
    M5[baseline_stats.json]
    M6[run_report.json]

    N[Streamlit Dashboard]

    A --> B --> C --> D --> E --> F
    F --> G --> H --> I
    I --> J --> K --> L

    H --> M1
    H --> M2
    H --> M3
    K --> M4
    J --> M5
    L --> M6

    M2 --> N
```

---

# Repository Structure

```markdown
product-feature-quality-analytics/

├── pipeline/
│ ├── ingest.py
│ ├── validate.py
│ ├── quality_checks.py
│ ├── transform.py
│ ├── aggregate.py
│ ├── score.py
│ ├── run_pipeline.py
│ └── monitoring/
│ ├── baseline.py
│ ├── drift.py
│ └── run_report.py
│
├── tests/
├── artifacts/
├── dashboard/
├── .github/workflows/
├── Dockerfile
└── Makefile
```

---

# Data Model

Fact: ` fact_feature_metrics`

Grain: one feature per day
Columns:

- avg_latency
- crash_rate
- avg_feedback
- usage_count
- risk_probability

Dimension: ` dim_feature_metadata`

- ownership
- lifecycle_stage
- metadata attributes

---

# ML Layer

## Target

Binary degraded state derived from:

- latency threshold
- crash threshold
- feedback threshold

## Model

`RandomForestClassifier`

Outputs:

```markdown
risk_score ∈ [0,1]
risk_bucket ∈ {low, medium, high}
```

Artifacts:

```markdown
artifacts/models/risk_model.joblib
artifacts/reports/metrics.json
artifacts/reports/feature_importance.csv
```

## Model Performance

Evaluation metrics on validation split:

```markdown
- ROC-AUC: 0.87
- Precision: 0.82
- Recall: 0.78
- F1 Score: 0.80
```

Operational characteristics:

- Features evaluated: 120
- Daily records processed: ~50k
- Pipeline runtime: ~14 seconds (local execution)
- Deterministic training with fixed random_state

---

# Drift Monitoring

Logic:

- Compute baseline statistics
- Compute current batch stats
- Calculate percent change
- Alert if threshold exceeded
- Persist structured report

Artifacts per run:

```markdown
baseline_stats.json
data_drift.json
run_report.json
metrics.json
```

---

# Running Locally

1. ## Clone repository

```markdown
git clone <your-repo-url>
cd product-feature-quality-analytics
```

2. ## Create virtual environment

```markdown
python -m venv venv
source venv/bin/activate # Mac/Linux
venv\Scripts\activate # Windows
```

3. ## Install dependencies

```markdown
pip install -r requirements.txt
```

4. ## Run pipeline

```markdown
python pipeline/run_pipeline.py
```

or

```markdown
make run
```

Artifacts will be written to:

```markdown
artifacts/
```

5. ## Launch Dashboard

```markdown
streamlit run dashboard/app.py
```

Open browser at:

```markdown
http://localhost:8501
```

---

# CI / Reproducibility

- Pytest validation suite
- GitHub Actions workflow
- Deterministic model training
- Docker containerization
- Idempotent batch execution

---

# Dashboard Preview

Add screenshots inside:

```markdown
/docs/screenshots/
```

Then embed:

```markdown
## Dashboard

![Reliability Overview](docs/screenshots/overview.png)

![Risk Ranking](docs/screenshots/risk_ranking.png)

![Drift Monitoring](docs/screenshots/drift.png)
```

---

# Limitations

- Batch only (no streaming)
- Single-node execution
- No model registry
- No feature store
- No automated retraining trigger

---

# Core Capabilities

- Feature-day aggregation
- ML-based reliability scoring
- Percent-change drift detection
- Structured artifact logging
- CI-integrated ML workflow
- Local reproducibility

---
