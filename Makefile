
.PHONY: help run test test-cov lint format install \
        kafka-produce kafka-consume \
        airflow-init airflow-up \
        dbt-run dbt-test dbt-docs \
        mlflow-ui \
        docker-up docker-down docker-build \
        clean dirs

# Default: show help when you just type 'make'
help:
	@echo ""
	@echo "╔══════════════════════════════════════════════════════╗"
	@echo "║  Feature Quality Analytics Platform"
	@echo "╚══════════════════════════════════════════════════════╝"
	@echo ""
	@echo "── PIPELINE ──────────────────────────────────────────"
	@echo "  make run           Run full pipeline (CSV → Dashboard)"
	@echo "  make dirs          Create all required data directories"
	@echo ""
	@echo "── TESTING ───────────────────────────────────────────"
	@echo "  make test          Run all tests"
	@echo "  make test-cov      Run tests with coverage report"
	@echo "  make lint          Check code quality with Ruff"
	@echo "  make format        Auto-format code with Ruff"
	@echo ""
	@echo "── KAFKA STREAMING ───────────────────────────────────"
	@echo "  make kafka-produce  Simulate 200 telemetry events"
	@echo "  make kafka-consume  Consume events → Bronze layer"
	@echo ""
	@echo "── DBT TRANSFORMATIONS ───────────────────────────────"
	@echo "  make dbt-run       Build Bronze/Silver/Gold models"
	@echo "  make dbt-test      Run dbt data tests"
	@echo "  make dbt-docs      Generate dbt documentation site"
	@echo ""
	@echo "── MLFLOW ────────────────────────────────────────────"
	@echo "  make mlflow-ui     Open MLflow UI (localhost:5000)"
	@echo ""
	@echo "── DOCKER ────────────────────────────────────────────"
	@echo "  make docker-up     Start ALL services (Kafka, MLflow, Grafana)"
	@echo "  make docker-down   Stop all services"
	@echo "  make docker-build  Rebuild Docker image"
	@echo ""
	@echo "── DASHBOARD ─────────────────────────────────────────"
	@echo "  make dashboard     Launch Streamlit dashboard"
	@echo ""

# ── SETUP ──────────────────────────────────────────────────
install:
	pip install -r requirements.txt
	@echo "✅ All dependencies installed"

dirs:
	mkdir -p data/raw data/bronze data/silver data/gold \
	         artifacts/reports/data_quality \
	         models mlflow_runs logs \
	         kafka airflow/dags dbt_project/models/{bronze,silver,gold} \
	         mlflow_tracking infrastructure/prometheus
	@echo "✅ All directories created"

# ── PIPELINE ───────────────────────────────────────────────
run: dirs
	cd pipeline && python run_pipeline.py
	@echo "✅ Pipeline complete"

# ── TESTING ────────────────────────────────────────────────
test:
	cd pipeline && python -m pytest ../tests/ -v
	@echo "✅ All tests passed"

test-cov:
	cd pipeline && python -m pytest ../tests/ \
		--cov=. \
		--cov-report=term-missing \
		--cov-fail-under=70 \
		-v
	@echo "✅ Coverage report complete"

lint:
	ruff check pipeline/ tests/ kafka/ mlflow_tracking/ --ignore E501
	@echo "✅ Lint check passed"

format:
	ruff format pipeline/ tests/ kafka/ mlflow_tracking/
	@echo "✅ Code formatted"

# ── KAFKA ──────────────────────────────────────────────────
kafka-produce:
	python kafka/producer.py
	@echo "✅ Events produced"

kafka-consume:
	python kafka/consumer.py
	@echo "✅ Events consumed to Bronze layer"

# ── DBT ────────────────────────────────────────────────────
dbt-run:
	cd dbt_project && dbt run --profiles-dir .
	@echo "✅ dbt models built (Bronze → Silver → Gold)"

dbt-test:
	cd dbt_project && dbt test --profiles-dir .
	@echo "✅ dbt tests passed"

dbt-docs:
	cd dbt_project && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .
	@echo " dbt docs at http://localhost:8080"

# ── MLFLOW ─────────────────────────────────────────────────
mlflow-ui:
	mlflow ui --backend-store-uri mlflow_runs --port 5000
	@echo " MLflow UI at http://localhost:5000"

# ── DOCKER ─────────────────────────────────────────────────
docker-build:
	docker build -t pfqpa:latest .
	@echo "✅ Docker image built"

docker-up:
	docker-compose up -d
	@echo ""
	@echo "✅ All services started:"
	@echo "   Streamlit Dashboard  → http://localhost:8501"
	@echo "   MLflow UI            → http://localhost:5000"
	@echo "   Prometheus           → http://localhost:9090"
	@echo "   Grafana              → http://localhost:3000  (admin/admin)"
	@echo ""

docker-down:
	docker-compose down
	@echo " All services stopped"

# ── DASHBOARD ──────────────────────────────────────────────
dashboard:
	streamlit run dashboard/app.py
	@echo " Dashboard at http://localhost:8501"

# ── CLEAN ──────────────────────────────────────────────────
clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -name "*.egg-info" -exec rm -rf {} +
	@echo "✅ Cleaned up cache files"
