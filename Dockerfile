FROM python:3.11-slim

WORKDIR /app

COPY requirements-core.txt .
RUN pip install --no-cache-dir -r requirements-core.txt

COPY . .

CMD ["python", "pipeline/run_pipeline.py"]