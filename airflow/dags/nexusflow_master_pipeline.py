"""
NexusFlow — Master Orchestration DAG
======================================
Orchestrates the full ELT pipeline:
  1. Validate bronze data availability
  2. Trigger Spark bronze→silver jobs on EMR Serverless
  3. Run dbt silver→gold transformations
  4. Validate data quality (Great Expectations)
  5. Refresh ML feature store
  6. Notify on success/failure

Schedule: Daily at 03:00 UTC
Backfill: Supported (catchup=True)
"""

import logging
from datetime import datetime, timedelta

from airflow.decorators import task, task_group
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessStartJobOperator,
)
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow import DAG

logger = logging.getLogger(__name__)

# ── CONSTANTS ─────────────────────────────────────────────
ENV = Variable.get("nexusflow_env", default_var="dev")
BRONZE_BUCKET = Variable.get(
    "nexusflow_bronze_bucket", default_var="nexusflow-dev-lakehouse"
)
SILVER_BUCKET = Variable.get(
    "nexusflow_silver_bucket", default_var="nexusflow-dev-lakehouse"
)
GOLD_BUCKET = Variable.get(
    "nexusflow_gold_bucket", default_var="nexusflow-dev-lakehouse"
)
EMR_APP_ID = Variable.get("nexusflow_emr_app_id", default_var="")
EMR_ROLE_ARN = Variable.get("nexusflow_emr_role_arn", default_var="")
DBT_IMAGE = Variable.get("nexusflow_dbt_image", default_var="nexusflow-dbt:latest")
SLACK_CONN = "slack_webhook_nexusflow"
AWS_CONN = "aws_default"

# Spark job S3 paths
ARTIFACTS_BUCKET = Variable.get(
    "nexusflow_artifacts_bucket", default_var="nexusflow-dev-artifacts"
)
SPARK_SCRIPT_PATH = f"s3://{ARTIFACTS_BUCKET}/spark-scripts"

# ── DEFAULT ARGS ──────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


# ── HELPER FUNCTIONS ──────────────────────────────────────


def get_date_partition(execution_date: datetime) -> str:
    """Get date partition string from execution date."""
    return execution_date.strftime("%Y-%m-%d")


def check_bronze_completeness(**context) -> str:
    """Check if bronze data files exist for the execution date."""
    date_partition = get_date_partition(context["logical_date"])
    s3_hook = S3Hook(aws_conn_id=AWS_CONN)

    required_prefixes = [
        f"bronze/orders/date={date_partition}/",
        f"bronze/clickstream/date={date_partition}/",
    ]

    all_present = all(
        len(s3_hook.list_keys(bucket_name=BRONZE_BUCKET, prefix=p) or []) > 0
        for p in required_prefixes
    )

    return (
        "validate_bronze.bronze_complete"
        if all_present
        else "validate_bronze.bronze_incomplete"
    )


# ── DAG DEFINITION ────────────────────────────────────────
with DAG(
    dag_id="nexusflow_master_pipeline",
    description="NexusFlow end-to-end ELT pipeline — Bronze → Silver → Gold → ML Features",
    schedule_interval="0 3 * * *",  # Daily 03:00 UTC
    start_date=datetime(2026, 6, 15),
    catchup=False,
    max_active_runs=3,
    default_args=default_args,
    tags=["nexusflow", "production", "elt", "daily"],
    doc_md=__doc__,
    render_template_as_native_obj=True,
) as dag:

    # ── START ──────────────────────────────────────────────
    pipeline_start = EmptyOperator(task_id="pipeline_start")

    # ── VALIDATE BRONZE AVAILABILITY ───────────────────────
    @task_group(group_id="validate_bronze")
    def validate_bronze_group():

        check_completeness = BranchPythonOperator(
            task_id="check_bronze_completeness",
            python_callable=check_bronze_completeness,
        )

        bronze_complete = EmptyOperator(task_id="bronze_complete")

        bronze_incomplete = SlackWebhookOperator(
            task_id="bronze_incomplete",
            slack_webhook_conn_id=SLACK_CONN,
            message=(
                "⚠️ NexusFlow: Bronze data incomplete for " "{{ ds }} — pipeline paused"
            ),
        )

        check_completeness >> [bronze_complete, bronze_incomplete]

        # Group-to-group ">>" wires every leaf of this group (both
        # bronze_complete AND bronze_incomplete) as upstream of the next
        # group. Since BranchPythonOperator skips whichever branch it
        # doesn't pick, and downstream tasks default to ALL_SUCCESS,
        # that skip poisons the entire rest of the pipeline regardless
        # of which branch fired. Return only bronze_complete so the
        # chain below gates on it alone.
        return bronze_complete

    validate_bronze = validate_bronze_group()

    # ── SPARK PROCESSING: BRONZE → SILVER ─────────────────
    @task_group(group_id="bronze_to_silver")
    def bronze_to_silver_group():

        def make_spark_job(entity: str) -> EmrServerlessStartJobOperator:
            return EmrServerlessStartJobOperator(
                task_id=f"spark_{entity}",
                application_id=EMR_APP_ID,
                execution_role_arn=EMR_ROLE_ARN,
                job_driver={
                    "sparkSubmit": {
                        "entryPoint": f"{SPARK_SCRIPT_PATH}/bronze_to_silver.py",
                        "entryPointArguments": [
                            "--env",
                            ENV,
                            "--bronze-bucket",
                            BRONZE_BUCKET,
                            "--silver-bucket",
                            SILVER_BUCKET,
                            "--date-partition",
                            "{{ ds }}",
                            "--entities",
                            entity,
                            "--aws-region",
                            "ca-central-1",
                        ],
                        # Sized for the EMR app's demo cap (16 vCPU / 64 GB total,
                        # terraform/modules/emr/main.tf) shared across 4 parallel
                        # entity jobs. Old cores=4/mem=16g/maxExecutors=20 let a
                        # single job alone request 80 vCPU/320GB — instantly hit
                        # ApplicationMaxCapacityExceededException even before the
                        # other 3 jobs asked for anything.
                        "sparkSubmitParameters": (
                            "--conf spark.executor.cores=1 "
                            "--conf spark.executor.memory=2g "
                            "--conf spark.driver.memory=2g "
                            "--conf spark.executor.instances=1 "
                            "--conf spark.dynamicAllocation.enabled=true "
                            "--conf spark.dynamicAllocation.minExecutors=1 "
                            "--conf spark.dynamicAllocation.maxExecutors=2 "
                            "--conf spark.sql.adaptive.enabled=true"
                        ),
                    }
                },
                configuration_overrides={
                    "monitoringConfiguration": {
                        "s3MonitoringConfiguration": {
                            "logUri": f"s3://{ARTIFACTS_BUCKET}/emr-logs/"
                        }
                    }
                },
                aws_conn_id=AWS_CONN,
            )

        # Parallel Spark jobs per entity
        spark_orders = make_spark_job("orders")
        spark_clickstream = make_spark_job("clickstream")
        spark_inventory = make_spark_job("inventory")
        spark_customers = make_spark_job("customers")

        spark_reviews = EmrServerlessStartJobOperator(
            task_id="spark_reviews",
            application_id=EMR_APP_ID,
            execution_role_arn=EMR_ROLE_ARN,
            job_driver={
                "sparkSubmit": {
                    "entryPoint": f"{SPARK_SCRIPT_PATH}/bronze_to_silver.py",
                    "entryPointArguments": [
                        "--env",
                        ENV,
                        "--bronze-bucket",
                        BRONZE_BUCKET,
                        "--silver-bucket",
                        SILVER_BUCKET,
                        "--date-partition",
                        "{{ ds }}",
                        "--entities",
                        "reviews",
                        "--aws-region",
                        "ca-central-1",
                    ],
                    "sparkSubmitParameters": (
                        "--packages com.databricks:spark-xml_2.12:0.18.0 "
                        "--conf spark.executor.cores=1 "
                        "--conf spark.executor.memory=2g "
                        "--conf spark.driver.memory=2g "
                        "--conf spark.executor.instances=1 "
                        "--conf spark.dynamicAllocation.enabled=true "
                        "--conf spark.dynamicAllocation.minExecutors=1 "
                        "--conf spark.dynamicAllocation.maxExecutors=2 "
                        "--conf spark.sql.adaptive.enabled=true"
                    ),
                }
            },
            configuration_overrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": f"s3://{ARTIFACTS_BUCKET}/emr-logs/"
                    }
                }
            },
            aws_conn_id=AWS_CONN,
        )

        # All in parallel
        [
            spark_orders,
            spark_clickstream,
            spark_inventory,
            spark_customers,
            spark_reviews,
        ]

    silver_processing = bronze_to_silver_group()

    # ── GLUE CRAWLERS: UPDATE CATALOG ─────────────────────
    @task_group(group_id="update_catalog")
    def update_catalog_group():
        # terraform provisions exactly one crawler per layer (bronze/
        # silver/gold), each pointed at the whole layer's S3 prefix —
        # it picks up every entity under silver/ in one pass. There's
        # no per-entity crawler to target, and AWS rejects a second
        # concurrent StartCrawler call against the same crawler name
        # (CrawlerRunningException), so this can't be two parallel
        # tasks. GlueJobOperator was also the wrong operator entirely —
        # this targets a Crawler resource, not a Job.
        GlueCrawlerOperator(
            task_id="crawl_silver",
            config={"Name": f"nexusflow-{ENV}-silver-crawler"},
            aws_conn_id=AWS_CONN,
        )

    update_catalog = update_catalog_group()

    # ── DBT TRANSFORMATIONS: SILVER → GOLD ────────────────
    @task_group(group_id="dbt_transformations")
    def dbt_group():
        # The scheduler image (src/airflow/Dockerfile) has no dbt binary
        # and no dbt_project/ — subprocess.run(cwd="/app/dbt_project")
        # used to throw FileNotFoundError. dbt has its own purpose-built
        # image+ServiceAccount+IRSA (kubernetes/dbt/cronjob.yml) that
        # were already provisioned but unused by this DAG. Launching that
        # image via KPO instead of running dbt inside the scheduler.
        # Requires kubernetes/dbt/scheduler-pod-launcher-rbac.yml (grants
        # airflow-scheduler SA pod-launch rights in the nexusflow
        # namespace — multiNamespaceMode is off, so this is namespace-
        # scoped rather than cluster-wide).
        from kubernetes.client import models as k8s

        env_from = [
            k8s.V1EnvFromSource(
                config_map_ref=k8s.V1ConfigMapEnvSource(name="nexusflow-config")
            ),
            k8s.V1EnvFromSource(
                secret_ref=k8s.V1SecretEnvSource(name="nexusflow-secrets")
            ),
        ]

        def make_dbt_task(task_id: str, command: str) -> KubernetesPodOperator:
            return KubernetesPodOperator(
                task_id=task_id,
                namespace="nexusflow",
                name=f"nexusflow-{task_id.replace('_', '-')}",
                image=DBT_IMAGE,
                service_account_name="nexusflow-dbt-sa",
                env_from=env_from,
                cmds=["/bin/bash", "-c"],
                arguments=[command],
                get_logs=True,
                is_delete_operator_pod=True,
                in_cluster=True,
                startup_timeout_seconds=120,
            )

        # dbt has no concept of a "cwd default" for project lookup — it
        # always needs --project-dir explicitly. --profiles-dir alone
        # left dbt looking for dbt_project.yml in the container's WORKDIR
        # (/app), not /app/dbt_project where it actually lives, throwing
        # "No dbt_project.yml found at expected path /app/dbt_project.yml".
        # kubernetes/dbt/cronjob.yml's args have this same gap (relies on
        # the image's CMD default, which never fires once args override
        # it) — never caught before since nothing exercised that path.
        DBT_PROJECT_DIR = "/app/dbt_project"

        run_silver = make_dbt_task(
            "dbt_run_silver",
            f"set -e; dbt deps --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --target {ENV} && "
            f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --target {ENV} "
            f'--select tag:silver --vars \'{{"execution_date": "{{{{ ds }}}}"}}\' && '
            # dbt run never executes snapshots regardless of --select —
            # they need the separate `dbt snapshot` command. Runs after
            # silver models since snapshot_customers reads silver_customers.
            f"dbt snapshot --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --target {ENV}",
        )
        run_gold = make_dbt_task(
            "dbt_run_gold",
            f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --target {ENV} "
            f'--select tag:gold --vars \'{{"execution_date": "{{{{ ds }}}}"}}\'',
        )
        run_tests = make_dbt_task(
            "dbt_test",
            f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --target {ENV} "
            f"--select tag:gold --store-failures",
        )

        run_silver >> run_gold >> run_tests

    dbt_transforms = dbt_group()

    # ── DATA QUALITY: GREAT EXPECTATIONS ──────────────────
    @task(task_id="great_expectations_validate")
    def run_great_expectations(**context) -> dict:
        """Run Great Expectations checkpoints on gold tables in Redshift."""
        import json as _json
        import os

        import boto3

        import great_expectations as gx

        # GE's connection_string + data connector use ${...} env
        # substitution (check great_expectations/great_expectations.yml).
        # The scheduler pod gets REDSHIFT_HOST + REDSHIFT_SECRET_ARN from
        # the airflow-dynamic-config ConfigMap; the admin password is
        # never an env var — fetching it from Secrets Manager at runtime.
        region = os.environ.get("AWS_REGION", "ca-central-1")
        secret_arn = os.environ["REDSHIFT_SECRET_ARN"]
        sm = boto3.client("secretsmanager", region_name=region)
        secret = _json.loads(sm.get_secret_value(SecretId=secret_arn)["SecretString"])
        os.environ["REDSHIFT_PASSWORD"] = secret["password"]
        os.environ["GE_GOLD_SCHEMA"] = f"dbt_{ENV}_gold"
        os.environ["GE_ML_SCHEMA"] = f"dbt_{ENV}_ml_features"

        context_gx = gx.get_context(context_root_dir="/app/great_expectations")

        checkpoints = [
            "fact_orders_checkpoint",
            "dim_customers_checkpoint",
            "customer_ml_features_checkpoint",
        ]

        results = {}
        all_passed = True

        for checkpoint_name in checkpoints:
            result = context_gx.run_checkpoint(checkpoint_name=checkpoint_name)
            passed = result.success
            results[checkpoint_name] = {"success": passed}
            if not passed:
                all_passed = False
                logger.error(f"❌ Checkpoint failed: {checkpoint_name}")

        if not all_passed:
            raise ValueError(f"Data quality checks failed: {results}")

        logger.info("✅ All Great Expectations checkpoints passed")
        return results

    dq_validation = run_great_expectations()

    # ── ML FEATURE STORE SYNC ─────────────────────────────
    @task(task_id="sync_feature_store")
    def sync_feature_store(**context) -> dict:
        """No-op placeholder for SageMaker Feature Store sync.

        The real ingest (Redshift read -> wr.feature_store.ingest) is
        intentionally not wired: there is no SageMaker Feature Group in
        terraform and no awswrangler in the scheduler image, so importing
        it unconditionally used to crash this task and poison the DAG.
        To productionize: add a SageMaker module + feature group, install
        awswrangler in src/airflow/Dockerfile, grant sagemaker:* on the
        airflow IRSA role, then implement the ingest below.
        """
        date = context["ds"]
        logger.info(f"[stub] feature store sync skipped for date: {date}")
        return {"status": "skipped", "date": date}

    feature_store_sync = sync_feature_store()

    # ── SUCCESS NOTIFICATION ───────────────────────────────
    notify_success = SlackWebhookOperator(
        task_id="notify_success",
        slack_webhook_conn_id=SLACK_CONN,
        message=(
            "✅ *NexusFlow Pipeline Complete* \n"
            "Date: `{{ ds }}` | Env: `" + ENV + "`\n"
            "Duration: `{{ macros.datetime.now() }}`"
        ),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    notify_failure = SlackWebhookOperator(
        task_id="notify_failure",
        slack_webhook_conn_id=SLACK_CONN,
        message=(
            "🚨 *NexusFlow Pipeline FAILED* \n"
            "Date: `{{ ds }}` | Env: `" + ENV + "`\n"
            "Check Airflow: https://airflow.nexusflow.io"
        ),
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    pipeline_end = EmptyOperator(
        task_id="pipeline_end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── DEPENDENCY GRAPH ───────────────────────────────────
    (
        pipeline_start
        >> validate_bronze
        >> silver_processing
        >> update_catalog
        >> dbt_transforms
        >> dq_validation
        >> feature_store_sync
        >> [notify_success, notify_failure]
        >> pipeline_end
    )
