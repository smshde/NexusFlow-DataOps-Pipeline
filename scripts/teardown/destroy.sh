#!/usr/bin/env bash
# ============================================================
# NexusFlow — Teardown Script
# Usage: bash scripts/teardown/destroy.sh
# Run BEFORE terraform destroy
# ============================================================
set -euo pipefail

REGION="ca-central-1"
RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; NC='\033[0m'

echo -e "${RED}WARNING: This will destroy all NexusFlow resources${NC}"
read -rp "Type 'yes' to confirm: " CONFIRM
[ "$CONFIRM" = "yes" ] || { echo "Cancelled."; exit 0; }

# ── EMPTY S3 BUCKETS ─────────────────────────────────────
echo -e "${CYAN}Emptying S3 buckets...${NC}"
for SUFFIX in lakehouse artifacts logs athena-results; do
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

# ── REMOVE HELM RELEASES ─────────────────────────────────
echo -e "${CYAN}Removing Helm releases...${NC}"
helm uninstall airflow         -n airflow    2>/dev/null || true
helm uninstall kube-prometheus -n monitoring 2>/dev/null || true

# ── DELETE NAMESPACES ────────────────────────────────────
echo -e "${CYAN}Removing namespaces...${NC}"
for NS in nexusflow kafka airflow monitoring; do
  kubectl delete namespace $NS --ignore-not-found=true
done

echo -e "${GREEN}✅ Pre-destroy cleanup complete${NC}"
echo ""
echo "Now run:"
echo "  cd terraform/environments/dev"
echo "  terraform destroy"
