.PHONY: run test dashboard docker-build docker-run docker-test docker-dashboard

run:
	python pipeline/run_pipeline.py

test:
	pytest -q

dashboard:
	streamlit run dashboard/app.py

docker-build:
	docker build -t pfqpa .

docker-run:
	docker run --rm pfqpa

docker-test:
	docker run --rm pfqpa python -m pytest -q

docker-dashboard:
	docker run --rm -p 8501:8501 pfqpa streamlit run dashboard/app.py --server.address=0.0.0.0 --server.port=8501	