# ============================================================
# NexusFlow — Data Generator Service Dockerfile
# ============================================================

FROM python:3.11-slim AS base

# Maintainer info
LABEL maintainer="data-engineering@nexusflow.io" \
      project="nexusflow" \
      service="datagen"

# Build arguments
ARG BUILD_SHA=local
ARG BUILD_DATE=unknown

# Environment variables
ENV BUILD_SHA=${BUILD_SHA} \
    BUILD_DATE=${BUILD_DATE} \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# System dependencies — librdkafka required for confluent-kafka
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Python dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Run as non-root user for security
RUN useradd --uid 1001 --no-create-home --shell /bin/false nexusflow
USER nexusflow

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import os; exit(0 if os.path.exists('/tmp/producer_alive') else 1)"

ENTRYPOINT ["python"]
CMD ["kafka_producer.py", "--mode=live", "--bootstrap-servers=localhost:9092"]