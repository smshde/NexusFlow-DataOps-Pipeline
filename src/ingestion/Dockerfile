# ============================================================
# NexusFlow — Ingestion Service Dockerfile
# File: src/ingestion/Dockerfile
# ============================================================
FROM python:3.11-slim AS base

LABEL maintainer="data-engineering@nexusflow.io" \
      project="nexusflow" \
      service="ingestion"

ARG BUILD_SHA=local
ENV BUILD_SHA=${BUILD_SHA} \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# System deps — librdkafka for confluent-kafka
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Non-root user
RUN useradd --uid 1001 --no-create-home --shell /bin/false nexusflow
USER nexusflow

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD python3 -c "import os,time; hb='/tmp/consumer_alive'; \
    exit(0 if os.path.exists(hb) and time.time()-os.path.getmtime(hb)<120 else 1)"

ENTRYPOINT ["python"]
CMD ["kafka_consumer.py", \
     "--bootstrap-servers", "localhost:9092", \
     "--s3-bucket", "nexusflow-dev-lakehouse"]
