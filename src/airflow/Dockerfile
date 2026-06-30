FROM apache/airflow:2.10.5

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow-providers-amazon==7.4.0" \
    "apache-airflow-providers-slack==7.3.0" \
    "great_expectations==0.18.19" \
    "sqlalchemy-redshift==0.8.14" \
    "redshift_connector==2.1.1"

# GE FileDataContext at /app/great_expectations — the DAG task
# (great_expectations_validate) calls gx.get_context(context_root_dir=
# "/app/great_expectations"). KPO it is NOT — this runs in the scheduler
# pod (LocalExecutor), so it must ship inside the scheduler image.
COPY --chown=airflow:root great_expectations/ /app/great_expectations/
