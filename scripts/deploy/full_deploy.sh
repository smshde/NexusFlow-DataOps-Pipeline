#!/usr/bin/env bash
# ============================================================
# NexusFlow — Full Stack Deploy Script
# Deploys all sprints after terraform apply
# Usage: bash scripts/deploy/full_deploy.sh
# ============================================================
set -euo pipefail

REGION="ca-central-1"
GREEN='\033[0;32m'; CYAN='\033[0;36m'; NC='\033[0m'
info() { echo -e "${CYAN}[INFO]${NC} $*"; }
pass() { echo -e "${GREEN}[OK]${NC}   $*"; }

echo -e "${CYAN}"
echo "╔══════════════════════════════════════════════╗"
echo "║   NexusFlow — Full Stack Deploy              ║"
echo "╚══════════════════════════════════════════════╝"
echo -e "${NC}"

# ── STEP 1: COLLECT ENV VALUES ───────────────────────────
info "Step 1: Collecting environment values..."
bash scripts/validate/collect_env_values.sh
source .env

# ── TEST SLACK ────────────────────────────────────────────
if [ -n "${SLACK_WEBHOOK_URL:-}" ]; then
  curl -s -X POST \
    -H 'Content-type: application/json' \
    --data "{\"text\":\"🚀 NexusFlow deploy started — ENV: $NEXUSFLOW_ENV\"}" \
    "$SLACK_WEBHOOK_URL"
  pass "Slack notification sent"
fi

pass "Environment ready"

# ── STEP 2: CONNECT KUBECTL ──────────────────────────────
info "Step 2: Connecting kubectl to EKS..."
aws eks update-kubeconfig \
  --region $REGION \
  --name $EKS_CLUSTER_NAME
pass "kubectl connected — $(kubectl get nodes --no-headers | wc -l | tr -d ' ') nodes ready"

# ── STEP 3: ECR LOGIN ────────────────────────────────────
info "Step 3: Logging into ECR..."
aws ecr get-login-password --region $REGION | \
  docker login --username AWS --password-stdin $ECR_REGISTRY
pass "ECR login successful"

# ── STEP 4: BUILD AND PUSH ALL IMAGES ───────────────────
info "Step 4: Building and pushing Docker images..."
for SERVICE in datagen ingestion serving; do
  info "  Building nexusflow-$SERVICE..."
  docker buildx build \
    --platform linux/amd64 \
    -t $ECR_REGISTRY/nexusflow-$SERVICE:latest \
    src/$SERVICE/ \
    --push
  pass "  nexusflow-$SERVICE pushed"
done
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
  sed -i '' \
  "s|ECR_REGISTRY|$ECR_REGISTRY|g" {} \; 2>/dev/null || true
find kubernetes/ -name "*.yml" -exec \
  sed -i '' \
  "s|ACCOUNT_ID|$AWS_ACCOUNT_ID|g" {} \; 2>/dev/null || true
pass "Manifests updated"

# ── STEP 7: CREATE NAMESPACES ───────────────────────────
info "Step 7: Creating namespaces..."
kubectl create namespace nexusflow  2>/dev/null || true
kubectl create namespace airflow    2>/dev/null || true
kubectl apply -f kubernetes/kafka/namespace.yml
kubectl apply -f kubernetes/monitoring/namespace.yml
pass "Namespaces ready"

# ── STEP 7.5: DEPLOY STANDALONE POSTGRES FOR AIRFLOW ────
info "Step 7.5: Deploying standalone PostgreSQL for Airflow..."

kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-postgres
  namespace: airflow
spec:
  replicas: 1
  selector:
    matchLabels:
      app: airflow-postgres
  template:
    metadata:
      labels:
        app: airflow-postgres
    spec:
      containers:
        - name: postgres
          image: postgres:13
          env:
            - name: POSTGRES_USER
              value: airflow
            - name: POSTGRES_PASSWORD
              value: airflow
            - name: POSTGRES_DB
              value: airflow
          ports:
            - containerPort: 5432
---
apiVersion: v1
kind: Service
metadata:
  name: airflow-postgres
  namespace: airflow
spec:
  selector:
    app: airflow-postgres
  ports:
    - port: 5432
      targetPort: 5432
EOF

kubectl wait --for=condition=ready pod \
  -l app=airflow-postgres \
  -n airflow \
  --timeout=120s

pass "Standalone PostgreSQL ready"

# ── STEP 8: CREATE SECRETS ──────────────────────────────
info "Step 8: Creating Kubernetes secrets..."
for NS in nexusflow kafka airflow; do
  kubectl delete secret nexusflow-secrets -n $NS 2>/dev/null || true
  kubectl create secret generic nexusflow-secrets \
    --namespace $NS \
    --from-literal=MSK_BOOTSTRAP_SERVERS="$MSK_BOOTSTRAP_SERVERS" \
    --from-literal=REDSHIFT_HOST="$REDSHIFT_HOST" \
    --from-literal=AWS_REGION="$REGION"
done
# Create dedicated AlertManager secret
kubectl delete secret alertmanager-slack \
  -n monitoring 2>/dev/null || true
kubectl create secret generic alertmanager-slack \
  --namespace monitoring \
  --from-literal=slack_api_url="$SLACK_WEBHOOK_URL"

pass "Secrets created"

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

# ── STEP 10: DEPLOY AIRFLOW ──────────────────────────────
info "Step 10: Deploying Airflow..."
helm repo add apache-airflow \
  https://airflow.apache.org 2>/dev/null || true
helm repo update
helm upgrade --install airflow apache-airflow/airflow \
  --version 1.16.0 \
  --namespace airflow \
  --values kubernetes/airflow/values.yml \
  --timeout 25m \
  --wait
pass "Airflow deployed"

# ── STEP 11: DEPLOY APPLICATION PODS ────────────────────
info "Step 11: Deploying application pods..."
kubectl apply -f kubernetes/datagen/deployment.yml
kubectl apply -f kubernetes/kafka/kafka-connect-deployment.yml
kubectl apply -f kubernetes/dbt/cronjob.yml
kubectl apply -f kubernetes/dashboard/ 2>/dev/null || true
pass "Application pods deployed"

# ── STEP 12: WAIT FOR PODS ──────────────────────────────
info "Step 12: Waiting for pods..."
sleep 30
kubectl get pods -n nexusflow
kubectl get pods -n kafka
kubectl get pods -n airflow
pass "Pods status shown above"

# ── STEP 13: VERIFY S3 LANDING ──────────────────────────
info "Step 13: Waiting 3 minutes for data to land in S3..."
sleep 180
echo "S3 Bronze layer contents:"
aws s3 ls s3://$S3_BRONZE_BUCKET/bronze/ \
  --recursive --region $REGION | head -20

# ── SUMMARY ─────────────────────────────────────────────
AIRFLOW_URL=$(kubectl get svc airflow-webserver \
  -n airflow \
  -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' \
  2>/dev/null || echo "pending")
GRAFANA_URL=$(kubectl get svc kube-prometheus-grafana \
  -n monitoring \
  -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' \
  2>/dev/null || echo "pending")

# ── SUCCESS NOTIFICATION ──────────────────────────────────
if [ -n "${SLACK_WEBHOOK_URL:-}" ]; then
  curl -s -X POST \
    -H 'Content-type: application/json' \
    --data "{\"text\":\"✅ NexusFlow full stack deployed\nAirflow: http://$AIRFLOW_URL\nGrafana: http://$GRAFANA_URL\nENV: $NEXUSFLOW_ENV\"}" \
    "$SLACK_WEBHOOK_URL"
fi


echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║   NexusFlow Full Stack Running ✅                ║${NC}"
echo -e "${GREEN}╠══════════════════════════════════════════════════╣${NC}"
echo -e "${GREEN}║  Airflow:  http://$AIRFLOW_URL                   ║${NC}"
echo -e "${GREEN}║  Grafana:  http://$GRAFANA_URL                   ║${NC}"
echo -e "${GREEN}║  Creds:    admin / nexusflow2026                 ║${NC}"
echo -e "${GREEN}╠══════════════════════════════════════════════════╣${NC}"
echo -e "${GREEN}║  View logs:                                      ║${NC}"
echo -e "${GREEN}║  kubectl logs -n nexusflow -l app=datagen -f     ║${NC}"
echo -e "${GREEN}║  kubectl logs -n kafka -l app=kafka-consumer -f  ║${NC}"
echo -e "${GREEN}╠══════════════════════════════════════════════════╣${NC}"
echo -e "${GREEN}║  When done: cd terraform/environments/dev        ║${NC}"
echo -e "${GREEN}║             terraform destroy                    ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════╝${NC}"
