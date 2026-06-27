#!/usr/bin/env bash
# ============================================================
# NexusFlow — Collect Environment Values
# Run after terraform apply
# Usage: bash scripts/validate/collect_env_values.sh
# ============================================================
set -euo pipefail

REGION="ca-central-1"

echo "Collecting AWS values..."

# ── COLLECT ALL VALUES ────────────────────────────────────
ACCOUNT=$(aws sts get-caller-identity \
  --query Account --output text)

ECR=$(aws ecr describe-repositories \
  --region $REGION \
  --query 'repositories[0].repositoryUri' \
  --output text | cut -d'/' -f1)

EKS=$(aws eks list-clusters \
  --region $REGION \
  --query 'clusters[0]' \
  --output text)

CLUSTER_ARN=$(aws kafka list-clusters-v2 \
  --region $REGION \
  --query 'ClusterInfoList[0].ClusterArn' \
  --output text)

MSK=$(aws kafka get-bootstrap-brokers \
  --cluster-arn $CLUSTER_ARN \
  --region $REGION \
  --query 'BootstrapBrokerStringSaslIam' \
  --output text)

EMR=$(aws emr-serverless list-applications \
  --region $REGION \
  --query 'applications[0].id' \
  --output text)

EMR_ROLE=$(aws iam get-role \
  --role-name nexusflow-dev-emr-execution \
  --query 'Role.Arn' \
  --output text)

REDSHIFT=$(aws redshift-serverless list-workgroups \
  --region $REGION \
  --query 'workgroups[0].endpoint.address' \
  --output text)

# nexusflow-serving (FastAPI) reads this to fetch the Redshift admin
# password at startup — never hardcoded, never set as a plain env var.
REDSHIFT_SECRET_ARN=$(aws secretsmanager describe-secret \
  --secret-id "redshift!nexusflow-dev-ns-nexusflow_admin" \
  --region $REGION \
  --query 'ARN' \
  --output text)

# Needed by dbt's on-run-start CREATE EXTERNAL SCHEMA hook (Spectrum
# over Spark's silver/ output) — the role Redshift assumes to read Glue
# + S3, not an IRSA/OIDC role.
REDSHIFT_SPECTRUM_ROLE_ARN=$(aws iam get-role \
  --role-name nexusflow-dev-wg-role \
  --query 'Role.Arn' \
  --output text)

# Generate Fernet key for Airflow
FERNET=$(python3 -c \
  "from cryptography.fernet import Fernet; \
  print(Fernet.generate_key().decode())" 2>/dev/null \
  || echo "GENERATE_MANUALLY")

# ── WRITE .env FILE ───────────────────────────────────────
cat > .env << ENVEOF
AWS_ACCOUNT_ID=$ACCOUNT
AWS_REGION=$REGION
REGION=$REGION
ECR_REGISTRY=$ECR
EKS_CLUSTER_NAME=$EKS
MSK_BOOTSTRAP_SERVERS=$MSK
EMR_APPLICATION_ID=$EMR
EMR_EXECUTION_ROLE_ARN=$EMR_ROLE
REDSHIFT_HOST=$REDSHIFT
REDSHIFT_PORT=5439
REDSHIFT_DB=nexusflow
REDSHIFT_USER=nexusflow_admin
REDSHIFT_SPECTRUM_ROLE_ARN=$REDSHIFT_SPECTRUM_ROLE_ARN
REDSHIFT_SECRET_ARN=$REDSHIFT_SECRET_ARN
S3_BRONZE_BUCKET=nexusflow-dev-lakehouse
S3_SILVER_BUCKET=nexusflow-dev-lakehouse
S3_GOLD_BUCKET=nexusflow-dev-lakehouse
S3_ARTIFACTS_BUCKET=nexusflow-dev-artifacts
S3_LOGS_BUCKET=nexusflow-dev-logs
NEXUSFLOW_ENV=dev
NUM_CUSTOMERS=10000
ORDERS_PER_SECOND=5
CLICKS_PER_SECOND=50
GENERATOR_SEED=42
AIRFLOW_FERNET_KEY=$FERNET
SLACK_WEBHOOK_URL=${SLACK_WEBHOOK_URL:-}
ENVEOF

# ── PRINT SUMMARY ─────────────────────────────────────────
echo ""
echo ".env file created"
echo ""
echo "AWS Account:   $ACCOUNT"
echo "ECR Registry:  $ECR"
echo "EKS Cluster:   $EKS"
echo "MSK Brokers:   $MSK"
echo "EMR App ID:    $EMR"
echo "Redshift:      $REDSHIFT"
echo ""
echo "Next: bash scripts/deploy/full_deploy.sh"