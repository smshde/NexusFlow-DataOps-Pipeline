#!/usr/bin/env bash
# ============================================================
# NexusFlow — Teardown Script
# Usage: bash scripts/teardown/destroy.sh
# Must run BEFORE terraform destroy
# ============================================================
set -euo pipefail

REGION="ca-central-1"
RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; NC='\033[0m'

echo -e "${RED}WARNING: This will destroy all NexusFlow resources${NC}"
read -rp "Type 'yes' to confirm: " CONFIRM
[ "$CONFIRM" = "yes" ] || { echo "Cancelled."; exit 0; }

# ── EMPTY S3 BUCKETS ─────────────────────────────────────
echo -e "${CYAN}Emptying S3 buckets...${NC}"
for SUFFIX in lakehouse artifacts logs; do
  BUCKET="nexusflow-dev-$SUFFIX"
  if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
    python3 - << EOF
import boto3
s3 = boto3.resource('s3', region_name='$REGION')
bucket = s3.Bucket('$BUCKET')
bucket.object_versions.delete()
print(f'  ✅ {bucket.name} emptied')
EOF
  fi
done

# ── DELETE LOADBALANCER SERVICES + WAIT FOR ELB/ENI RELEASE ──
# Namespace delete alone GC's these eventually, but doesn't wait —
# leftover Classic ELBs (airflow webserver, grafana) hold ENIs in
# every subnet and block terraform destroy's VPC deletion later
# (DependencyViolation) if EKS gets torn down before the cloud-
# controller-manager finishes deprovisioning them. Deleting explicitly
# and wait here, while EKS/cloud-controller-manager still exist.
echo -e "${CYAN}Deleting LoadBalancer-type Services and waiting for ELB teardown...${NC}"
LB_SVCS=$(kubectl get svc -A -o json 2>/dev/null | python3 -c "
import json, sys
data = json.load(sys.stdin)
for item in data.get('items', []):
    if item.get('spec', {}).get('type') == 'LoadBalancer':
        print(f\"{item['metadata']['namespace']} {item['metadata']['name']}\")
" || true)
if [ -n "$LB_SVCS" ]; then
  echo "$LB_SVCS" | while read -r NS NAME; do
    [ -n "$NS" ] && kubectl delete svc "$NAME" -n "$NS" --ignore-not-found=true
  done
  echo "  Waiting for k8s-elb-* Classic ELBs to disappear..."
  for i in $(seq 1 30); do
    REMAINING=$(aws elb describe-load-balancers --region "$REGION" \
      --query "LoadBalancerDescriptions[?starts_with(LoadBalancerName,'k8s-elb')].LoadBalancerName" \
      --output text 2>/dev/null)
    [ -z "$REMAINING" ] && break
    sleep 10
  done
  echo "  Waiting for k8s-elb-* ENIs/security groups to release..."
  for i in $(seq 1 30); do
    SG_IDS=$(aws ec2 describe-security-groups --region "$REGION" \
      --filters "Name=group-name,Values=k8s-elb-*" \
      --query 'SecurityGroups[*].GroupId' --output text 2>/dev/null)
    [ -z "$SG_IDS" ] && break
    sleep 10
  done
  if [ -n "${SG_IDS:-}" ]; then
    for SG in $SG_IDS; do
      aws ec2 delete-security-group --group-id "$SG" --region "$REGION" 2>/dev/null \
        && echo "  ✅ Deleted leftover SG $SG" || true
    done
  fi
  echo -e "${GREEN}✅ LoadBalancer cleanup complete${NC}"
fi

# ── REMOVE HELM RELEASES ─────────────────────────────────
echo -e "${CYAN}Removing Helm releases...${NC}"
helm uninstall airflow         -n airflow    2>/dev/null || true
helm uninstall kube-prometheus -n monitoring 2>/dev/null || true

# ── DELETE NAMESPACES ────────────────────────────────────
echo -e "${CYAN}Removing namespaces...${NC}"
for NS in nexusflow kafka airflow monitoring; do
  kubectl delete namespace $NS --ignore-not-found=true
done

# ── DELETE ECR IMAGES - Persistant: incurs hidden cost ────────────
echo "Deleting ECR images..."
for REPO in datagen ingestion serving dbt processing dashboard; do
  IMAGES=$(aws ecr list-images \
    --repository-name nexusflow-$REPO \
    --region ca-central-1 \
    --query 'imageIds[*]' \
    --output json 2>/dev/null)
  if [ "$IMAGES" != "[]" ] && [ -n "$IMAGES" ]; then
    aws ecr batch-delete-image \
      --repository-name nexusflow-$REPO \
      --image-ids "$IMAGES" \
      --region ca-central-1 2>/dev/null || true
    echo "  ✅ Cleared: nexusflow-$REPO"
  fi
done

# ── DELETE CLOUDWATCH LOG GROUPS - Persistant: incurs hidden cost ─────
echo "Deleting CloudWatch log groups..."
for LG in \
  "/aws/redshift/nexusflow-dev-ns/connectionlog" \
  "/aws/redshift/nexusflow-dev-ns/useractivitylog" \
  "/aws/redshift/nexusflow-dev-ns/userlog" \
  "/aws/eks/nexusflow-dev-cluster/cluster" \
  "/aws/msk/nexusflow-dev-kafka" \
  "/aws/emr-serverless/nexusflow-dev-spark/spark"; do
  aws logs delete-log-group \
    --log-group-name "$LG" \
    --region ca-central-1 2>/dev/null \
    && echo "  ✅ Deleted: $LG" || true
done

# ── STOP EMR SERVERLESS APPLICATION ──────────────────────
# terraform destroy fails with ValidationException if the app is
# STARTED (e.g. a Spark job was running/submitted) — DeleteApplication
# only accepts [CREATED, STOPPED]. Stop it first, idempotent.
echo -e "${CYAN}Stopping EMR Serverless application...${NC}"
EMR_APP_ID=$(aws emr-serverless list-applications \
  --region "$REGION" \
  --query "applications[?name=='nexusflow-dev-spark'].id" \
  --output text 2>/dev/null)
if [ -n "$EMR_APP_ID" ] && [ "$EMR_APP_ID" != "None" ]; then
  aws emr-serverless stop-application --application-id "$EMR_APP_ID" --region "$REGION" 2>/dev/null || true
  for i in $(seq 1 20); do
    APP_STATE=$(aws emr-serverless get-application --application-id "$EMR_APP_ID" --region "$REGION" --query 'application.state' --output text 2>/dev/null)
    [ "$APP_STATE" = "STOPPED" ] || [ "$APP_STATE" = "CREATED" ] && break
    sleep 10
  done
  echo "  ✅ EMR app $EMR_APP_ID state: ${APP_STATE:-unknown}"
fi

echo -e "${GREEN}✅ Pre-destroy cleanup complete${NC}"
echo ""
echo "Now run:"
echo "  cd terraform/environments/dev"
echo "  terraform destroy"
