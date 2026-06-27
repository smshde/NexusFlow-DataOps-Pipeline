#!/usr/bin/env bash
# NexusFlow Full Stack Deploy Script
# Usage: bash scripts/deploy/full_deploy.sh [--from STEP]
#   --from STEP   Resume from a given step label (e.g. --from 8.55, --from 9).
#                 Steps 1-3 (env/kubectl/ecr login) always run first since
#                 later steps depend on their exported vars/context.
#   --list        Print step labels in execution order and exit.
set -euo pipefail
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"

REGION="ca-central-1"
GREEN='\033[0;32m'
CYAN='\033[0;36m'
NC='\033[0m'

info() { echo "[INFO] $*"; }
pass() { echo "[OK]   $*"; }

# ── STEP LABELS IN EXECUTION ORDER ──────────────────────
STEP_LABELS=(4 5 6 7 7.5 8 8.55 8.5 8.6 8.7 8.8 8.9 8.10 9 9.5 10 10.5 11 12 13)

FROM_STEP=""
if [ "${1:-}" = "--list" ]; then
  echo "Steps 1-3 always run (env/kubectl/ecr login), then in order:"
  printf '%s\n' "${STEP_LABELS[@]}"
  exit 0
fi
if [ "${1:-}" = "--from" ]; then
  FROM_STEP="${2:-}"
fi

# index of FROM_STEP within STEP_LABELS; empty FROM_STEP -> run everything (idx 0)
START_IDX=0
if [ -n "$FROM_STEP" ]; then
  found=0
  for i in "${!STEP_LABELS[@]}"; do
    if [ "${STEP_LABELS[$i]}" = "$FROM_STEP" ]; then
      START_IDX=$i
      found=1
      break
    fi
  done
  if [ "$found" -eq 0 ]; then
    echo "Unknown --from step '$FROM_STEP'. Use --list to see valid labels." >&2
    exit 1
  fi
fi

# run_step LABEL: true if LABEL's index >= START_IDX
run_step() {
  local label="$1"
  for i in "${!STEP_LABELS[@]}"; do
    if [ "${STEP_LABELS[$i]}" = "$label" ]; then
      [ "$i" -ge "$START_IDX" ] && return 0 || return 1
    fi
  done
  return 1
}

echo "[INFO] NexusFlow Full Stack Deploy Starting...${FROM_STEP:+ (resuming from step $FROM_STEP)}"

# ── STEP 1: COLLECT ENV VALUES (always runs) ────────────
info "Step 1: Collecting environment values..."
bash scripts/validate/collect_env_values.sh
source .env
pass "Environment ready"

# ── STEP 2: CONNECT KUBECTL (always runs) ───────────────
info "Step 2: Connecting kubectl to EKS..."
aws eks update-kubeconfig \
  --region $REGION \
  --name $EKS_CLUSTER_NAME
pass "kubectl connected"

# ── STEP 3: ECR LOGIN (always runs) ─────────────────────
info "Step 3: Logging into ECR..."
aws ecr get-login-password --region $REGION | \
  docker login --username AWS \
  --password-stdin $ECR_REGISTRY
pass "ECR login successful"

# CLUSTER_ARN is computed in step 8.7 but consumed by step 8.9 — fetch it
# unconditionally (cheap describe call) so --from 8.9 alone still works.
CLUSTER_ARN=$(aws kafka list-clusters-v2 \
  --region $REGION \
  --query 'ClusterInfoList[0].ClusterArn' \
  --output text)

if run_step 4; then
# ── STEP 4: BUILD AND PUSH ALL IMAGES ───────────────────
info "Step 4: Building and pushing Docker images..."
for REPO in datagen ingestion serving dbt airflow; do
  aws ecr create-repository \
    --repository-name nexusflow-$REPO \
    --region $REGION 2>/dev/null || true
done

for SERVICE in datagen ingestion serving; do
  info "  Building nexusflow-$SERVICE..."
  docker buildx build \
    --platform linux/amd64 \
    -t $ECR_REGISTRY/nexusflow-$SERVICE:latest \
    src/$SERVICE/ \
    --push
  pass "  nexusflow-$SERVICE pushed"
done

info "  Building nexusflow-dbt..."
docker buildx build \
  --platform linux/amd64 \
  -t $ECR_REGISTRY/nexusflow-dbt:latest \
  -f src/dbt/Dockerfile \
  . \
  --push
pass "  nexusflow-dbt pushed"

info "  Building nexusflow-airflow..."
docker buildx build \
  --platform linux/amd64 \
  -t $ECR_REGISTRY/nexusflow-airflow:2.10.5 \
  src/airflow/ \
  --push
pass "  nexusflow-airflow pushed"
pass "All images in ECR"
fi

if run_step 5; then
# ── STEP 5: UPLOAD SPARK SCRIPTS ────────────────────────
info "Step 5: Uploading Spark scripts to S3..."
aws s3 cp src/processing/bronze_to_silver.py \
  s3://$S3_ARTIFACTS_BUCKET/spark-scripts/bronze_to_silver.py \
  --region $REGION
pass "Spark scripts uploaded"
fi

if run_step 6; then
# ── STEP 6: REPLACE PLACEHOLDERS IN MANIFESTS ───────────
info "Step 6: Updating manifests with real values..."
find kubernetes/ -name "*.yml" -exec \
  sed -i '' "s|ECR_REGISTRY|$ECR_REGISTRY|g" {} \; 2>/dev/null || true
find kubernetes/ -name "*.yml" -exec \
  sed -i '' "s|IMAGE_SHA|latest|g" {} \; 2>/dev/null || true
find kubernetes/ -name "*.yml" -exec \
  sed -i '' "s|ACCOUNT_ID|$AWS_ACCOUNT_ID|g" {} \; 2>/dev/null || true
pass "Manifests updated"
fi

if run_step 7; then
# ── STEP 7: CREATE NAMESPACES ───────────────────────────
info "Step 7: Creating namespaces..."
kubectl create namespace nexusflow 2>/dev/null || true
kubectl create namespace airflow 2>/dev/null || true
kubectl apply -f kubernetes/kafka/namespace.yml
kubectl apply -f kubernetes/monitoring/namespace.yml

for NS in nexusflow kafka airflow; do
  kubectl delete configmap nexusflow-config -n $NS 2>/dev/null || true
  kubectl create configmap nexusflow-config \
    --namespace $NS \
    --from-literal=NEXUSFLOW_ENV=dev \
    --from-literal=AWS_REGION=ca-central-1 \
    --from-literal=S3_BRONZE_BUCKET=nexusflow-dev-lakehouse
done
pass "Namespaces ready"

kubectl delete namespace airflow 2>/dev/null || true
sleep 20
kubectl create namespace airflow
fi

if run_step 7.5; then
# ── STEP 7.5: DEPLOY STANDALONE POSTGRES FOR AIRFLOW ────
info "Step 7.5: Deploying standalone PostgreSQL for Airflow..."
kubectl apply -f kubernetes/airflow/postgres.yml
kubectl wait --for=condition=ready pod \
  -l app=airflow-postgres \
  -n airflow \
  --timeout=120s
sleep 15
pass "Standalone PostgreSQL ready"
fi

if run_step 8; then
# ── STEP 8: CREATE SECRETS ──────────────────────────────
info "Step 8: Creating Kubernetes secrets..."
for NS in nexusflow kafka airflow; do
  kubectl delete secret nexusflow-secrets -n $NS 2>/dev/null || true
  kubectl create secret generic nexusflow-secrets \
    --namespace $NS \
    --from-literal=MSK_BOOTSTRAP_SERVERS="$MSK_BOOTSTRAP_SERVERS" \
    --from-literal=REDSHIFT_HOST="$REDSHIFT_HOST" \
    --from-literal=AWS_REGION="$REGION" \
    --from-literal=REDSHIFT_USER="nexusflow_admin" \
    --from-literal=REDSHIFT_SPECTRUM_ROLE_ARN="$REDSHIFT_SPECTRUM_ROLE_ARN" \
    --from-literal=REDSHIFT_SECRET_ARN="$REDSHIFT_SECRET_ARN"
done

kubectl delete secret alertmanager-slack -n monitoring 2>/dev/null || true
kubectl create secret generic alertmanager-slack \
  --namespace monitoring \
  --from-literal=slack_api_url="$SLACK_WEBHOOK_URL"

pip install cryptography --quiet 2>/dev/null || true
FERNET=$(python3 -c \
  "from cryptography.fernet import Fernet; \
  print(Fernet.generate_key().decode())")
kubectl delete secret airflow-fernet-secret -n airflow 2>/dev/null || true
kubectl create secret generic airflow-fernet-secret \
  --namespace airflow \
  --from-literal=fernet-key="$FERNET"
pass "Secrets created"
fi

if run_step 8.55; then
# ── STEP 8.55: BOOTSTRAP REDSHIFT PERMISSIONS FOR DBT ────
# dbt connects via IAM role federation (nexusflow-dev-dbt-irsa), which
# Redshift maps to a database user literally named "IAMR:nexusflow-dev-
# dbt-irsa". That role can't grant privileges to itself, so this must
# run once as the real admin (master password from Secrets Manager,
# created automatically by Redshift Serverless's managed-admin-password
# feature). CREATE on the database is enough — it covers both dbt's own
# schema creation (silver/gold/snapshots/etc, all "schema: x" configs in
# dbt_project.yml use CREATE SCHEMA IF NOT EXISTS) and the on-run-start
# CREATE EXTERNAL SCHEMA hook for Spectrum. Idempotent — safe to re-run.
info "Step 8.55: Bootstrapping Redshift permissions for dbt..."
# Redshift Serverless is publicly_accessible=false (VPC-only, see
# terraform/modules/redshift/main.tf) — this laptop/CI runner is outside
# the VPC and can't reach port 5439 directly (was hanging on TCP connect
# until redshift_connector's 60s timeout). Run the bootstrap inside the
# cluster instead, using the already-pushed dbt image (has redshift_connector
# via the dbt-redshift adapter) on a throwaway pod in the same VPC as Redshift.
REDSHIFT_ADMIN_PW=$(aws secretsmanager get-secret-value \
  --secret-id "redshift!nexusflow-${NEXUSFLOW_ENV}-ns-nexusflow_admin" \
  --region $REGION --query SecretString --output text \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['password'])")
kubectl delete secret redshift-bootstrap-pw -n nexusflow 2>/dev/null || true
kubectl create secret generic redshift-bootstrap-pw \
  --namespace nexusflow \
  --from-literal=password="$REDSHIFT_ADMIN_PW"
unset REDSHIFT_ADMIN_PW

kubectl delete configmap redshift-bootstrap-script -n nexusflow 2>/dev/null || true
kubectl create configmap redshift-bootstrap-script -n nexusflow --from-literal=bootstrap.py="
import os, redshift_connector
conn = redshift_connector.connect(
    host=os.environ['REDSHIFT_HOST'], port=5439, database='nexusflow',
    user='nexusflow_admin', password=os.environ['PW'],
)
cur = conn.cursor()
try:
    cur.execute('CREATE USER \"IAMR:nexusflow-dev-dbt-irsa\" PASSWORD DISABLE;')
    conn.commit()
    print('Created user IAMR:nexusflow-dev-dbt-irsa')
except Exception as e:
    conn.rollback()
    if '42710' not in str(e) and 'already exists' not in str(e):
        raise
    print('User IAMR:nexusflow-dev-dbt-irsa already exists, skipping create')
cur.execute('GRANT CREATE, TEMP ON DATABASE nexusflow TO \"IAMR:nexusflow-dev-dbt-irsa\";')
conn.commit()
print('Granted CREATE, TEMP on database nexusflow to IAMR:nexusflow-dev-dbt-irsa')
# CREATE EXTERNAL SCHEMA (on-run-start hook) succeeds for any user, but
# querying the resulting Spectrum views fails with UnauthorizedException
# unless the connecting user is explicitly granted ASSUMEROLE on that IAM
# role — CREATE/TEMP on the database alone doesn't cover Spectrum reads.
try:
    cur.execute('REVOKE ASSUMEROLE ON ALL FROM PUBLIC FOR ALL;')
    conn.commit()
    print('Switched ASSUMEROLE to access-control mode (revoked PUBLIC default)')
except Exception as e:
    conn.rollback()
    print('REVOKE ASSUMEROLE ON ALL FROM PUBLIC already applied or no-op:', e)
cur.execute('GRANT ASSUMEROLE ON \'' + os.environ['SPECTRUM_ROLE_ARN'] + '\' TO \"IAMR:nexusflow-dev-dbt-irsa\" FOR ALL;')
conn.commit()
print('Granted ASSUMEROLE on Spectrum IAM role to IAMR:nexusflow-dev-dbt-irsa')
"

kubectl delete pod redshift-bootstrap -n nexusflow --ignore-not-found=true
kubectl wait --for=delete pod/redshift-bootstrap -n nexusflow --timeout=30s 2>/dev/null || true
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: redshift-bootstrap
  namespace: nexusflow
spec:
  restartPolicy: Never
  containers:
    - name: redshift-bootstrap
      image: "$ECR_REGISTRY/nexusflow-dbt:latest"
      command: ["python3", "/scripts/bootstrap.py"]
      env:
        - name: REDSHIFT_HOST
          value: "$REDSHIFT_HOST"
        - name: SPECTRUM_ROLE_ARN
          value: "$REDSHIFT_SPECTRUM_ROLE_ARN"
        - name: PW
          valueFrom:
            secretKeyRef:
              name: redshift-bootstrap-pw
              key: password
      volumeMounts:
        - name: script
          mountPath: /scripts
  volumes:
    - name: script
      configMap:
        name: redshift-bootstrap-script
EOF
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded pod/redshift-bootstrap -n nexusflow --timeout=90s
kubectl logs redshift-bootstrap -n nexusflow
kubectl delete pod redshift-bootstrap -n nexusflow --ignore-not-found=true
kubectl delete secret redshift-bootstrap-pw -n nexusflow --ignore-not-found=true
kubectl delete configmap redshift-bootstrap-script -n nexusflow --ignore-not-found=true
pass "Redshift dbt permissions bootstrapped"
fi

if run_step 8.5; then
# ── STEP 8.5: IRSA TRUST POLICIES ───────────────────────
info "Step 8.5: Configuring IRSA trust policies..."
OIDC_ID=$(aws eks describe-cluster \
  --name $EKS_CLUSTER_NAME \
  --region $REGION \
  --query 'cluster.identity.oidc.issuer' \
  --output text | cut -d'/' -f5)

aws iam update-assume-role-policy \
  --role-name nexusflow-dev-app-irsa \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [{
      \"Effect\": \"Allow\",
      \"Principal\": {
        \"Federated\": \"arn:aws:iam::${AWS_ACCOUNT_ID}:oidc-provider/oidc.eks.ca-central-1.amazonaws.com/id/${OIDC_ID}\"
      },
      \"Action\": \"sts:AssumeRoleWithWebIdentity\",
      \"Condition\": {
        \"StringLike\": {
          \"oidc.eks.ca-central-1.amazonaws.com/id/${OIDC_ID}:sub\": \"system:serviceaccount:*:nexusflow-*\"
        }
      }
    }]
  }"

kubectl annotate serviceaccount nexusflow-datagen-sa \
  -n nexusflow \
  eks.amazonaws.com/role-arn=arn:aws:iam::${AWS_ACCOUNT_ID}:role/nexusflow-dev-app-irsa \
  --overwrite 2>/dev/null || true

kubectl annotate serviceaccount nexusflow-ingestion-sa \
  -n kafka \
  eks.amazonaws.com/role-arn=arn:aws:iam::${AWS_ACCOUNT_ID}:role/nexusflow-dev-app-irsa \
  --overwrite 2>/dev/null || true
pass "IRSA trust policies configured"
fi

if run_step 8.6; then
# ── STEP 8.6: MSK SECURITY GROUP ────────────────────────
info "Step 8.6: Configuring MSK security group..."
EKS_SG=$(aws eks describe-cluster \
  --name $EKS_CLUSTER_NAME \
  --region $REGION \
  --query 'cluster.resourcesVpcConfig.clusterSecurityGroupId' \
  --output text)

MSK_SG=$(aws ec2 describe-security-groups \
  --filters "Name=tag:Name,Values=*msk*" \
  --region $REGION \
  --query 'SecurityGroups[0].GroupId' \
  --output text)

aws ec2 authorize-security-group-ingress \
  --group-id $MSK_SG \
  --protocol tcp --port 9098 \
  --source-group $EKS_SG \
  --region $REGION 2>/dev/null || true

aws ec2 authorize-security-group-ingress \
  --group-id $MSK_SG \
  --protocol tcp --port 9094 \
  --source-group $EKS_SG \
  --region $REGION 2>/dev/null || true
pass "MSK security group configured"
fi

if run_step 8.7; then
# ── STEP 8.7: CREATE KAFKA TOPICS ───────────────────────
info "Step 8.7: Creating Kafka topics..."
for TOPIC in orders clickstream inventory-events product-reviews user-sessions dlq-orders; do
  aws kafka create-topic \
    --cluster-arn $CLUSTER_ARN \
    --topic-name $TOPIC \
    --replication-factor 2 \
    --partition-count 6 \
    --region $REGION 2>/dev/null \
    && echo "  Created: $TOPIC" \
    || echo "  Exists: $TOPIC"
done
pass "Kafka topics created"
fi

if run_step 8.8; then
# ── STEP 8.8: S3 IAM POLICY ─────────────────────────────
info "Step 8.8: Adding S3 IAM policy..."
aws iam put-role-policy \
  --role-name nexusflow-dev-app-irsa \
  --policy-name nexusflow-s3-policy \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": [
        "s3:PutObject","s3:GetObject",
        "s3:DeleteObject","s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::nexusflow-dev-*",
        "arn:aws:s3:::nexusflow-dev-*/*"
      ]
    }]
  }' 2>/dev/null || true
pass "S3 IAM policy added"

# nexusflow-serving (FastAPI) fetches the Redshift admin password from
# Secrets Manager at startup (fastapi_main.py's get_db_password()) instead
# of a plain-text env var — needs explicit read access to that one secret.
aws iam put-role-policy \
  --role-name nexusflow-dev-app-irsa \
  --policy-name nexusflow-secrets-policy \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [{
      \"Effect\": \"Allow\",
      \"Action\": [\"secretsmanager:GetSecretValue\"],
      \"Resource\": \"arn:aws:secretsmanager:${REGION}:${AWS_ACCOUNT_ID}:secret:redshift!nexusflow-${NEXUSFLOW_ENV}-ns-nexusflow_admin-*\"
    }]
  }" 2>/dev/null || true
pass "Secrets Manager IAM policy added"
fi

if run_step 8.9; then
# ── STEP 8.9: KAFKA CONSUMER IAM POLICY ─────────────────
info "Step 8.9: Adding Kafka consumer IAM policy..."
aws iam put-role-policy \
  --role-name nexusflow-dev-app-irsa \
  --policy-name nexusflow-kafka-consumer-policy \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [
      {
        \"Effect\": \"Allow\",
        \"Action\": [
          \"kafka-cluster:Connect\",
          \"kafka-cluster:AlterCluster\",
          \"kafka-cluster:DescribeCluster\"
        ],
        \"Resource\": \"$CLUSTER_ARN\"
      },
      {
        \"Effect\": \"Allow\",
        \"Action\": [
          \"kafka-cluster:ReadData\",
          \"kafka-cluster:WriteData\",
          \"kafka-cluster:DescribeTopic\",
          \"kafka-cluster:CreateTopic\"
        ],
        \"Resource\": \"arn:aws:kafka:ca-central-1:${AWS_ACCOUNT_ID}:topic/nexusflow-dev-kafka/*\"
      },
      {
        \"Effect\": \"Allow\",
        \"Action\": [
          \"kafka-cluster:AlterGroup\",
          \"kafka-cluster:DescribeGroup\"
        ],
        \"Resource\": \"arn:aws:kafka:ca-central-1:${AWS_ACCOUNT_ID}:group/nexusflow-dev-kafka/*\"
      }
    ]
  }" 2>/dev/null || true
pass "Kafka consumer IAM policy added"
fi

if run_step 8.10; then
# ── STEP 8.10: REDSHIFT IAM POLICY FOR DBT ──────────────
info "Step 8.10: Adding Redshift IAM policy for dbt..."
aws iam put-role-policy \
  --role-name nexusflow-dev-dbt-irsa \
  --policy-name nexusflow-redshift-policy \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": [
        "redshift:GetClusterCredentials",
        "redshift:DescribeClusters",
        "redshift-serverless:GetCredentials",
        "redshift-serverless:GetWorkgroup"
      ],
      "Resource": "*"
    }]
  }' 2>/dev/null || true
pass "Redshift IAM policy added"
fi

if run_step 9; then
# ── STEP 9: DEPLOY MONITORING ───────────────────────────
info "Step 9: Deploying Prometheus + Grafana..."
helm repo add prometheus-community \
  https://prometheus-community.github.io/helm-charts 2>/dev/null || true
helm repo update
helm upgrade --install kube-prometheus \
  prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --values kubernetes/monitoring/prometheus-values.yml \
  --wait --timeout 10m
pass "Monitoring deployed"
fi

if run_step 9.5; then
# ── STEP 9.5: CREATE AIRFLOW DYNAMIC CONFIG ─────────────
info "Step 9.5: Creating Airflow dynamic ConfigMap..."
kubectl delete configmap airflow-dynamic-config -n airflow 2>/dev/null || true
kubectl create configmap airflow-dynamic-config \
  --namespace airflow \
  --from-literal=AIRFLOW_VAR_NEXUSFLOW_EMR_APP_ID="$EMR_APPLICATION_ID" \
  --from-literal=AIRFLOW_VAR_NEXUSFLOW_EMR_ROLE_ARN="$EMR_EXECUTION_ROLE_ARN" \
  --from-literal=AIRFLOW_VAR_NEXUSFLOW_ARTIFACTS_BUCKET="$S3_ARTIFACTS_BUCKET" \
  --from-literal=AIRFLOW_VAR_NEXUSFLOW_BRONZE_BUCKET="$S3_BRONZE_BUCKET" \
  --from-literal=AIRFLOW_VAR_NEXUSFLOW_SILVER_BUCKET="$S3_SILVER_BUCKET" \
  --from-literal=AIRFLOW_VAR_NEXUSFLOW_GOLD_BUCKET="$S3_GOLD_BUCKET" \
  --from-literal=AIRFLOW_VAR_NEXUSFLOW_ENV="dev" \
  --from-literal=AIRFLOW_VAR_NEXUSFLOW_DBT_IMAGE="$ECR_REGISTRY/nexusflow-dbt:latest" \
  --from-literal=AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER="s3://$S3_LOGS_BUCKET/airflow-logs"
pass "Airflow dynamic ConfigMap created"
fi

if run_step 10; then
# ── STEP 10: DEPLOY AIRFLOW ──────────────────────────────
# No --wait here: this chart's DB-migration Job is a post-upgrade hook,
# and Helm only runs post-upgrade hooks after --wait confirms the main
# Deployments are Ready. Scheduler/webserver init containers block on
# migrations, which can't run until the hook fires — --wait deadlocks
# against its own hook. Wait on rollout explicitly instead, after the
# hook has already had a chance to run.
info "Step 10: Deploying Apache Airflow..."
helm repo add apache-airflow \
  https://airflow.apache.org 2>/dev/null || true
helm repo update
helm upgrade --install airflow apache-airflow/airflow \
  --version 1.16.0 \
  --namespace airflow \
  --values kubernetes/airflow/values.yml \
  --timeout 10m
kubectl rollout status deployment/airflow-scheduler -n airflow --timeout=10m
kubectl rollout status deployment/airflow-webserver -n airflow --timeout=10m
pass "Airflow deployed"
fi

if run_step 10.5; then
# ── STEP 10.5: CREATE SLACK CONNECTION ───────────────────
# DAG's SlackWebhookOperator tasks (notify_success, notify_failure,
# bronze_incomplete) use conn_id "slack_webhook_nexusflow" — this was
# never created, so those tasks throw "connection not found" whenever
# reached. Create it here if a webhook URL was provided.
if [ -n "${SLACK_WEBHOOK_URL:-}" ]; then
  info "Step 10.5: Creating Airflow Slack connection..."
  kubectl exec -n airflow deploy/airflow-scheduler -c scheduler -- \
    airflow connections add slack_webhook_nexusflow \
    --conn-type slackwebhook \
    --conn-host "$SLACK_WEBHOOK_URL" || true
  pass "Slack connection created"
else
  info "Step 10.5: SLACK_WEBHOOK_URL not set — skipping Slack connection (notify tasks will fail if reached)"
fi
fi

if run_step 11; then
# ── STEP 11: DEPLOY APPLICATION PODS ────────────────────
info "Step 11: Deploying application pods..."
kubectl apply -f kubernetes/datagen/deployment.yml
kubectl apply -f kubernetes/kafka/kafka-connect-deployment.yml
kubectl apply -f kubernetes/dbt/cronjob.yml
# DAG's dbt_transformations group launches KubernetesPodOperator pods in
# this namespace — airflow-scheduler SA (namespace airflow) needs explicit
# cross-namespace pod-launch rights since multiNamespaceMode is off.
kubectl apply -f kubernetes/dbt/scheduler-pod-launcher-rbac.yml
kubectl apply -f kubernetes/dashboard/deployment.yml 2>/dev/null || true
pass "Application pods deployed"
fi

if run_step 12; then
# ── STEP 12: WAIT FOR PODS ──────────────────────────────
info "Step 12: Checking pod status..."
sleep 30
kubectl get pods -n nexusflow
kubectl get pods -n kafka
kubectl get pods -n airflow
kubectl get pods -n monitoring
pass "Pod status shown above"
fi

if run_step 13; then
# ── STEP 13: VERIFY S3 LANDING ──────────────────────────
info "Step 13: Waiting 3 minutes for S3 data landing..."
sleep 180
echo "S3 Bronze layer:"
aws s3 ls s3://nexusflow-dev-lakehouse/bronze/ \
  --recursive --region $REGION | head -20

AIRFLOW_URL=$(kubectl get svc airflow-webserver \
  -n airflow \
  -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' \
  2>/dev/null || echo "pending")
GRAFANA_URL=$(kubectl get svc kube-prometheus-grafana \
  -n monitoring \
  -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' \
  2>/dev/null || echo "pending")

echo ""
echo "NexusFlow Full Stack Running"
echo "Airflow:  http://$AIRFLOW_URL:8080"
echo "Grafana:  http://$GRAFANA_URL:80"
echo "Creds:    admin / nexusflow2026"
echo "Logs:     kubectl logs -n nexusflow -l app=datagen -f"
echo "Done:     cd terraform/environments/dev && terraform destroy"
echo ""

if [ -n "${SLACK_WEBHOOK_URL:-}" ]; then
  curl -s -X POST \
    -H 'Content-type: application/json' \
    --data "{\"text\":\"DONE: NexusFlow deployed. Airflow: http://$AIRFLOW_URL:8080 Grafana: http://$GRAFANA_URL:80\"}" \
    "$SLACK_WEBHOOK_URL"
fi
fi
