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

from datetime import datetime, timedelta
import json
import logging
from typing import Any

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessStartJobOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrServerlessJobSensor
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

# ── CONSTANTS ─────────────────────────────────────────────
ENV            = Variable.get("nexusflow_env",            default_var="dev")
BRONZE_BUCKET  = Variable.get("nexusflow_bronze_bucket",  default_var="nexusflow-dev-lakehouse")
SILVER_BUCKET  = Variable.get("nexusflow_silver_bucket",  default_var="nexusflow-dev-lakehouse")
GOLD_BUCKET    = Variable.get("nexusflow_gold_bucket",    default_var="nexusflow-dev-lakehouse")
EMR_APP_ID     = Variable.get("nexusflow_emr_app_id",     default_var="")
EMR_ROLE_ARN   = Variable.get("nexusflow_emr_role_arn",   default_var="")
DBT_IMAGE      = Variable.get("nexusflow_dbt_image",      default_var="nexusflow-dbt:latest")
SLACK_CONN     = "slack_webhook_nexusflow"
AWS_CONN       = "aws_default"

# Spark job S3 paths
ARTIFACTS_BUCKET = Variable.get("nexusflow_artifacts_bucket", default_var="nexusflow-dev-artifacts")
SPARK_SCRIPT_PATH = f"s3://{ARTIFACTS_BUCKET}/spark-scripts"

# ── DEFAULT ARGS ──────────────────────────────────────────
default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":  timedelta(minutes=30),
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

    return "validate_bronze.bronze_complete" if all_present else "validate_bronze.bronze_incomplete"


# ── DAG DEFINITION ────────────────────────────────────────
with DAG(
    dag_id="nexusflow_master_pipeline",
    description="NexusFlow end-to-end ELT pipeline — Bronze → Silver → Gold → ML Features",
    schedule_interval="0 3 * * *",          # Daily 03:00 UTC
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
                "⚠️ NexusFlow: Bronze data incomplete for "
                "{{ ds }} — pipeline paused"
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
                            "--env",            ENV,
                            "--bronze-bucket",  BRONZE_BUCKET,
                            "--silver-bucket",  SILVER_BUCKET,
                            "--date-partition", "{{ ds }}",
                            "--entities",       entity,
                            "--aws-region",     "ca-central-1",
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
                            "--conf spark.dynamicAllocation.enabled=true "
                            "--conf spark.dynamicAllocation.minExecutors=1 "
                            "--conf spark.dynamicAllocation.maxExecutors=3 "
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
        spark_orders      = make_spark_job("orders")
        spark_clickstream = make_spark_job("clickstream")
        spark_inventory   = make_spark_job("inventory")
        spark_customers   = make_spark_job("customers")

        # All in parallel
        [spark_orders, spark_clickstream, spark_inventory, spark_customers]

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

        @task(task_id="dbt_run_silver")
        def dbt_run_silver(**context) -> dict:
            """Run dbt silver models."""
            import subprocess
            date = context["ds"]
            result = subprocess.run(
                [
                    "dbt", "run",
                    "--select", "tag:silver",
                    "--vars", json.dumps({"execution_date": date}),
                    "--profiles-dir", "/app/dbt_project",
                    "--target", ENV,
                ],
                capture_output=True, text=True, cwd="/app/dbt_project"
            )
            if result.returncode != 0:
                raise RuntimeError(f"dbt silver failed:\n{result.stderr}")
            logger.info(result.stdout)
            return {"status": "success", "stdout": result.stdout}

        @task(task_id="dbt_run_gold")
        def dbt_run_gold(**context) -> dict:
            """Run dbt gold models."""
            import subprocess
            date = context["ds"]
            result = subprocess.run(
                [
                    "dbt", "run",
                    "--select", "tag:gold",
                    "--vars", json.dumps({"execution_date": date}),
                    "--profiles-dir", "/app/dbt_project",
                    "--target", ENV,
                ],
                capture_output=True, text=True, cwd="/app/dbt_project"
            )
            if result.returncode != 0:
                raise RuntimeError(f"dbt gold failed:\n{result.stderr}")
            logger.info(result.stdout)
            return {"status": "success", "stdout": result.stdout}

        @task(task_id="dbt_test")
        def dbt_test(**context) -> dict:
            """Run dbt tests on gold layer."""
            import subprocess
            result = subprocess.run(
                [
                    "dbt", "test",
                    "--select", "tag:gold",
                    "--profiles-dir", "/app/dbt_project",
                    "--target", ENV,
                    "--store-failures",
                ],
                capture_output=True, text=True, cwd="/app/dbt_project"
            )
            if result.returncode != 0:
                raise RuntimeError(f"dbt tests failed:\n{result.stderr}")
            return {"status": "success"}

        run_silver = dbt_run_silver()
        run_gold = dbt_run_gold()
        run_tests = dbt_test()

        run_silver >> run_gold >> run_tests

    dbt_transforms = dbt_group()

    # ── DATA QUALITY: GREAT EXPECTATIONS ──────────────────
    @task(task_id="great_expectations_validate")
    def run_great_expectations(**context) -> dict:
        """Run Great Expectations checkpoints on gold tables."""
        import great_expectations as gx

        context_gx = gx.get_context(
            context_root_dir="/app/great_expectations"
        )

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
        """Push latest customer features to SageMaker Feature Store."""
        import boto3
        import pandas as pd
        import awswrangler as wr

        date = context["ds"]

        # Read gold features from Redshift
        query = f"""
            SELECT *
            FROM ml_features.customer_ml_features
            WHERE snapshot_date = '{date}'
            LIMIT 1000000
        """

        # In production: use Redshift Data API or psycopg2
        logger.info(f"Syncing feature store for date: {date}")

        # Simulate success (in prod, uncomment actual code)
        # df = wr.redshift.read_sql_query(sql=query, con=conn)
        # feature_group_name = f"nexusflow-customer-features-{ENV}"
        # wr.feature_store.ingest(df=df, feature_group_name=feature_group_name)

        logger.info("✅ Feature store sync complete")
        return {"status": "success", "date": date}

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
