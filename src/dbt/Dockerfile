# FILE: src/dbt/Dockerfile

FROM python:3.11-slim 
    # base image # for production use: python:3.11-slim-bookworm

LABEL maintainer="data-engineering@nexusflow.io" \
      project="nexusflow" \
      service="dbt"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir \
    dbt-redshift==1.8.0 \
    dbt-core==1.8.0

COPY dbt_project/ ./dbt_project/

# Bake dbt package deps (dbt_utils) into the image. KubernetesPodOperator
# launches a fresh pod per task, so a `dbt deps` run in one task's pod is
# invisible to the next — installing here means every pod already ships
# dbt_packages/. Without this, gold models fail: "'dbt_utils' is undefined".
# deps needs no DB connection or profile, just the project dir + packages.yml.
RUN dbt deps --project-dir ./dbt_project

ENTRYPOINT ["dbt"]
CMD ["run", "--profiles-dir", "/app/dbt_project", "--project-dir", "/app/dbt_project"]