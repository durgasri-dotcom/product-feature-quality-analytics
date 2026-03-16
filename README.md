# Product Feature Quality & Performance Analytics

![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-1.x-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?style=flat-square&logo=docker&logoColor=white)
![MLflow](https://img.shields.io/badge/MLflow-Tracking-0194E2?style=flat-square&logo=mlflow&logoColor=white)
![scikit-learn](https://img.shields.io/badge/scikit--learn-RF_Classifier-F7931E?style=flat-square&logo=scikit-learn&logoColor=white)
![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub_Actions-2088FF?style=flat-square&logo=github-actions&logoColor=white)

A data engineering and ML platform that monitors product feature reliability, scores risk, and surfaces performance regressions before they reach users.

---

## What it does

The pipeline ingests raw telemetry from 5 product features (Login, Payments, VideoPlayback, Recommendations, Search), runs quality checks, detects statistical drift, scores each feature with a trained Random Forest model, and writes results to a layered data store. A Streamlit dashboard with Plotly visualizations sits on top of the whole thing.

The stack is intentionally close to what you'd find in a real ML platform team — Kafka for ingestion, Airflow for orchestration, dbt for transformation, MLflow for experiment tracking, and Prometheus/Grafana for system metrics.

---

## Architecture

```
Raw Logs / Kafka
       │
       ▼
  Bronze Layer  ──── Parquet, partitioned by date, never modified
       │
       ▼
  Validation + Data Quality
  (Great Expectations, 12 checks)
       │
       ▼
  Silver Layer  ──── Cleaned, feature-engineered, anomaly-flagged
       │
       ├──── Drift Detection (PSI · KS Test · Δ% change)
       │
       ├──── ML Risk Scoring (Random Forest · MLflow tracking)
       │
       └──── dbt models → Gold Layer (aggregations, SLO tracking)
                │
                ▼
         Streamlit Dashboard (5 pages · Plotly · dark theme)
         MLflow UI · Prometheus · Grafana
```

---

## Tech stack

|                     |                        |
| ------------------- | ---------------------- |
| Data processing     | Pandas, NumPy, PyArrow |
| ML                  | scikit-learn, SHAP     |
| Experiment tracking | MLflow                 |
| Data quality        | Great Expectations     |
| Transformations     | dbt-core + DuckDB      |
| Streaming           | Apache Kafka           |
| Orchestration       | Airflow                |
| Visualization       | Streamlit, Plotly      |
| Monitoring          | Prometheus, Grafana    |
| Infra               | Docker, Docker Compose |
| CI/CD               | GitHub Actions         |

---

## Getting started

```bash
git clone https://github.com/YOUR_USERNAME/product-feature-quality-analytics.git
cd product-feature-quality-analytics

python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

pip install -r requirements.txt

python pipeline/run_pipeline.py
streamlit run dashboard/app.py
```

Or with Docker:

```bash
docker-compose up --build
# Dashboard  → http://localhost:8501
# MLflow     → http://localhost:5000
# Grafana    → http://localhost:3000
```

---

## Makefile

```bash
make run          # full pipeline
make dashboard    # Streamlit
make test-cov     # pytest + coverage
make lint         # ruff
make mlflow-ui    # experiment tracker
make dbt-run      # Bronze → Silver → Gold
make docker-up    # full stack
```

---

## Pipeline steps

```
1  Ingestion          5,026 rows loaded, Bronze Parquet saved (partitioned by date)
2  Drift Detection    PSI + KS Test + Δ% — alerts if any threshold exceeded
3  Data Quality       12 Great Expectations checks, JSON report saved
4  Feature Eng        12 derived features, saved to Silver layer
5  Aggregation        Rolled up to feature-day grain (150 rows)
6  ML Scoring         Random Forest ·AUC=0.96 ·CV-AUC=0.93±0.04 ·auto-logged to MLflow
7  Artifacts          model.pkl, metrics.json, feature_importance.csv
8  SHAP               Per-feature attribution via TreeExplainer
9  Export             feature_metrics_with_risk.csv, run_report.json
```

---

## Dashboard pages

| Page               | Content                                                |
| ------------------ | ------------------------------------------------------ |
| Overview           | KPI cards, risk ranking, latency and feedback trends   |
| Feature Deep Dive  | Per-feature risk score, latency chart, recommendations |
| Model Intelligence | AUC gauge, feature importance, confusion matrix        |
| Drift Monitor      | PSI scores, KS results, % change from baseline         |
| Pipeline Status    | Step health, Bronze/Silver/Gold file counts, last run  |

---

## Drift detection

| Method    | Alert threshold     |
| --------- | ------------------- |
| PSI       | > 0.2               |
| KS Test   | p-value < 0.05      |
| Δ% change | > 20% from baseline |

---

## Project structure

```
├── pipeline/
│   ├── ingest.py            # ingestion + Bronze layer
│   ├── validate.py          # schema + range checks
│   ├── quality_checks.py    # Great Expectations suite
│   ├── transform.py         # feature engineering → Silver
│   ├── aggregate.py         # feature-day rollup
│   ├── score.py             # RF model + MLflow + SHAP
│   ├── run_pipeline.py      # 9-step orchestrator
│   └── monitoring/
│       ├── baseline.py
│       ├── drift.py         # PSI + KS + Δ%
│       └── run_report.py
├── dashboard/app.py         # Streamlit (5 pages, Plotly)
├── kafka/                   # producer + consumer
├── dbt_project/             # Bronze/Silver/Gold SQL models
├── airflow/dags/            # daily DAG, 7 tasks
├── mlflow_tracking/         # experiment logging utils
├── data/
│   ├── raw/                 # source CSV
│   ├── bronze/              # raw Parquet
│   ├── silver/              # transformed Parquet
│   └── gold/                # dbt output
├── artifacts/reports/       # metrics, drift, feature importance
├── tests/                   # 5 test files
├── docker-compose.yml
├── Makefile
└── .github/workflows/ci.yml
```

---

## Tests

```bash
pytest tests/ -v
pytest tests/ --cov=pipeline --cov-fail-under=70
```

| File                            | Covers                       |
| ------------------------------- | ---------------------------- |
| test_validate_schema.py         | schema checks, type coercion |
| test_quality_checks.py          | null rates, GE suite         |
| test_drift_detection.py         | PSI, KS test, alert logic    |
| test_model_training.py          | training, AUC threshold      |
| test_aggregate_output_schema.py | aggregation grain, columns   |

---

## CI/CD

Every push to `main` runs:

1. `ruff` lint check
2. `pytest` with 70% coverage gate
3. Docker image build
4. `dbt compile` validation

---

## Author

**Sri Durga Abhigna Tanguturi**

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?style=flat-square&logo=linkedin)](https://www.linkedin.com/in/durgasritanguturi)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-181717?style=flat-square&logo=github)](https://github.com/durgasri-dotcom)
