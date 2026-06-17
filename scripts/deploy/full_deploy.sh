#!/usr/bin/env bash
# NexusFlow Full Stack Deploy Script
# Usage: bash scripts/deploy/full_deploy.sh
set -euo pipefail

REGION="ca-central-1"
GREEN='\033[0;32m'
CYAN='\033[0;36m'
NC='\033[0m'

info() { echo "[INFO] $*"; }
pass() { echo "[OK]   $*"; }

echo "[INFO] NexusFlow Full Stack Deploy Starting..."

# ── STEP 1: COLLECT ENV VALUES ───────────────────────────
info "Step 1: Collecting environment values..."
bash scripts/validate/collect_env_values.sh
source .env
pass "Environment ready"

# ── STEP 2: CONNECT KUBECTL ──────────────────────────────
info "Step 2: Connecting kubectl to EKS..."
aws eks update-kubeconfig \
  --region $REGION \
  --name $EKS_CLUSTER_NAME
pass "kubectl connected"

# ── STEP 3: ECR LOGIN ────────────────────────────────────
info "Step 3: Logging into ECR..."
aws ecr get-login-password --region $REGION | \
  docker login --username AWS \
  --password-stdin $ECR_REGISTRY
pass "ECR login successful"

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

# ── STEP 5: UPLOAD SPARK SCRIPTS ────────────────────────
info "Step 5: Uploading Spark scripts to S3..."
aws s3 cp src/processing/bronze_to_silver.py \
  s3://$S3_ARTIFACTS_BUCKET/spark-scripts/bronze_to_silver.py \
  --region $REGION
pass "Spark scripts uploaded"

# ── STEP 6: REPLACE PLACEHOLDERS IN MANIFESTS ───────────
info "Step 6: Updating manifests with real values..."
find kubernetes/ -name "*.yml" -exec \
  sed -i '' "s|ECR_REGISTRY|$ECR_REGISTRY|g" {} \; 2>/dev/null || true
find kubernetes/ -name "*.yml" -exec \
  sed -i '' "s|IMAGE_SHA|latest|g" {} \; 2>/dev/null || true
find kubernetes/ -name "*.yml" -exec \
  sed -i '' "s|ACCOUNT_ID|$AWS_ACCOUNT_ID|g" {} \; 2>/dev/null || true
pass "Manifests updated"

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

# ── STEP 7.5: DEPLOY STANDALONE POSTGRES FOR AIRFLOW ────
info "Step 7.5: Deploying standalone PostgreSQL for Airflow..."
kubectl apply -f kubernetes/airflow/postgres.yml
kubectl wait --for=condition=ready pod \
  -l app=airflow-postgres \
  -n airflow \
  --timeout=120s
sleep 15
pass "Standalone PostgreSQL ready"

# ── STEP 8: CREATE SECRETS ──────────────────────────────
info "Step 8: Creating Kubernetes secrets..."
for NS in nexusflow kafka airflow; do
  kubectl delete secret nexusflow-secrets -n $NS 2>/dev/null || true
  kubectl create secret generic nexusflow-secrets \
    --namespace $NS \
    --from-literal=MSK_BOOTSTRAP_SERVERS="$MSK_BOOTSTRAP_SERVERS" \
    --from-literal=REDSHIFT_HOST="$REDSHIFT_HOST" \
    --from-literal=AWS_REGION="$REGION" \
    --from-literal=REDSHIFT_USER="nexusflow_admin"
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

# ── STEP 8.7: CREATE KAFKA TOPICS ───────────────────────
info "Step 8.7: Creating Kafka topics..."
CLUSTER_ARN=$(aws kafka list-clusters-v2 \
  --region $REGION \
  --query 'ClusterInfoList[0].ClusterArn' \
  --output text)

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

# ── STEP 9.5: CREATE AIRFLOW DYNAMIC CONFIG ─────────────
info "Step 9.5: Creating Airflow dynamic ConfigMap..."
kubectl delete configmap airflow-dynamic-config -n airflow 2>/dev/null || true
kubectl create configmap airflow-dynamic-config \
  --namespace airflow \
  --from-literal=AIRFLOW_VAR_NEXUSFLOW_EMR_APP_ID="$EMR" \
  --from-literal=AIRFLOW_VAR_NEXUSFLOW_EMR_ROLE_ARN="$EMR_ROLE" \
  --from-literal=AIRFLOW_VAR_NEXUSFLOW_ARTIFACTS_BUCKET="$S3_ARTIFACTS_BUCKET" \
  --from-literal=AIRFLOW_VAR_NEXUSFLOW_BRONZE_BUCKET="$S3_BRONZE_BUCKET" \
  --from-literal=AIRFLOW_VAR_NEXUSFLOW_SILVER_BUCKET="$S3_SILVER_BUCKET" \
  --from-literal=AIRFLOW_VAR_NEXUSFLOW_GOLD_BUCKET="$S3_GOLD_BUCKET" \
  --from-literal=AIRFLOW_VAR_NEXUSFLOW_ENV="dev"
pass "Airflow dynamic ConfigMap created"

# ── STEP 10: DEPLOY AIRFLOW ──────────────────────────────
info "Step 10: Deploying Apache Airflow..."
helm repo add apache-airflow \
  https://airflow.apache.org 2>/dev/null || true
helm repo update
helm upgrade --install airflow apache-airflow/airflow \
  --version 1.16.0 \
  --namespace airflow \
  --values kubernetes/airflow/values.yml \
  --timeout 30m \
  --wait
pass "Airflow deployed"

# ── STEP 11: DEPLOY APPLICATION PODS ────────────────────
info "Step 11: Deploying application pods..."
kubectl apply -f kubernetes/datagen/deployment.yml
kubectl apply -f kubernetes/kafka/kafka-connect-deployment.yml
kubectl apply -f kubernetes/dbt/cronjob.yml
kubectl apply -f kubernetes/dashboard/deployment.yml 2>/dev/null || true
pass "Application pods deployed"

# ── STEP 12: WAIT FOR PODS ──────────────────────────────
info "Step 12: Checking pod status..."
sleep 30
kubectl get pods -n nexusflow
kubectl get pods -n kafka
kubectl get pods -n airflow
kubectl get pods -n monitoring
pass "Pod status shown above"

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
echo "Airflow:  http://$AIRFLOW_URL"
echo "Grafana:  http://$GRAFANA_URL"
echo "Creds:    admin / nexusflow2026"
echo "Logs:     kubectl logs -n nexusflow -l app=datagen -f"
echo "Done:     cd terraform/environments/dev && terraform destroy"
echo ""

if [ -n "${SLACK_WEBHOOK_URL:-}" ]; then
  curl -s -X POST \
    -H 'Content-type: application/json' \
    --data "{\"text\":\"DONE: NexusFlow deployed. Airflow: http://$AIRFLOW_URL Grafana: http://$GRAFANA_URL\"}" \
    "$SLACK_WEBHOOK_URL"
fi
