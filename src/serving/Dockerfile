FROM python:3.11-slim

LABEL maintainer="data-engineering@nexusflow.io" \
      project="nexusflow" \
      service="serving"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN useradd --uid 1001 --no-create-home --shell /bin/false nexusflow
USER nexusflow

EXPOSE 8000

ENTRYPOINT ["uvicorn"]
CMD ["main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
