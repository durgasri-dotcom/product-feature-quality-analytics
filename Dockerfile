FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --use-deprecated=legacy-resolver -r requirements.txt

COPY . .

# Default: run pipeline
CMD ["python", "pipeline/run_pipeline.py"]