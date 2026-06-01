#!/usr/bin/env bash
# ============================================================
# NexusFlow — Sprint 2 Local Test Script
# File: scripts/validate/test_datagen_local.sh
#
# Tests data generation locally BEFORE deploying to AWS.
# Run this on your local machineto verify everything works.
#
# Usage:
#   bash scripts/validate/test_datagen_local.sh
# ============================================================
set -euo pipefail

# REPLACE WITH THIS
PYTHON=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3.11

# Verify Python version
$PYTHON --version | grep "3.11" || {
  echo "❌ Python 3.11 not found at expected path"bash scripts/validate/test_datagen_local.sh
  exit 1
}
# ── COLORS ────────────────────────────────────────────────
GREEN='\033[0;32m'; RED='\033[0;31m'; CYAN='\033[0;36m'
YELLOW='\033[1;33m'; NC='\033[0m'

pass() { echo -e "${GREEN}  ✅ PASS${NC}: $*"; }
fail() { echo -e "${RED}  ❌ FAIL${NC}: $*"; exit 1; }
info() { echo -e "${CYAN}[INFO]${NC}  $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }

OUTPUT_DIR="/tmp/nexusflow-local-test"
PASS_COUNT=0
FAIL_COUNT=0

echo -e "${CYAN}"
echo "╔══════════════════════════════════════════════════╗"
echo "║   NexusFlow Sprint 2 — Local Data Gen Test       ║"
echo "╚══════════════════════════════════════════════════╝"
echo -e "${NC}"

# ── STEP 1: PREREQUISITES ─────────────────────────────────
info "Step 1: Checking prerequisites..."

$PYTHON --version | grep "3.11" || fail "Python 3.11 not found"
pass "Python found: $($PYTHON --version)"

command -v pip3 &>/dev/null || fail "pip3 not found"
pass "pip3 found"

command -v docker &>/dev/null || fail "docker not found"
pass "docker found: $(docker --version | head -1)"

command -v aws &>/dev/null || fail "aws CLI not found"
pass "aws CLI found: $(aws --version | head -1)"

# ── STEP 2: PYTHON DEPENDENCIES ───────────────────────────
info "Step 2: Installing Python dependencies..."

pip3 install faker confluent-kafka boto3 awswrangler \
  --quiet --no-warn-script-location

$PYTHON -c "import faker; import boto3" || \
  fail "Python dependencies not installed correctly"

pass "Python dependencies installed"

# ── STEP 3: RUN BATCH GENERATOR (LOCAL MODE) ──────────────
info "Step 3: Running batch generator in local mode..."

rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

$PYTHON src/datagen/ecommerce_generator.py \
  --mode batch \
  --output-dir "$OUTPUT_DIR" \
  --num-customers 100 \
  --num-orders 1000 \
  2>&1 | tail -10

# Verify output files exist
CUSTOMERS_FILE="$OUTPUT_DIR/customers.jsonl"
ORDERS_FILE="$OUTPUT_DIR/orders.jsonl"
INVENTORY_FILE="$OUTPUT_DIR/inventory.csv"
REVIEWS_FILE="$OUTPUT_DIR/reviews.xml"

[ -f "$CUSTOMERS_FILE" ] && pass "customers.jsonl created" \
  || fail "customers.jsonl missing"
[ -f "$ORDERS_FILE"    ] && pass "orders.jsonl created" \
  || fail "orders.jsonl missing"
[ -f "$INVENTORY_FILE" ] && pass "inventory.csv created" \
  || fail "inventory.csv missing"
[ -f "$REVIEWS_FILE"   ] && pass "reviews.xml created" \
  || fail "reviews.xml missing"

# ── STEP 4: VALIDATE DATA QUALITY ─────────────────────────
info "Step 4: Validating data quality..."

$PYTHON - <<'EOF'
import json, csv, sys
from xml.etree import ElementTree as ET

errors = []

# Check customers JSONL
customers = []
with open("/tmp/nexusflow-local-test/customers.jsonl") as f:
    for line in f:
        c = json.loads(line)
        customers.append(c)
        if not c.get("customer_id"):
            errors.append("customer missing customer_id")
        if len(c.get("email_hash", "")) != 64:
            errors.append("customer email_hash not SHA-256")
        if "email" not in c:
            errors.append("customer missing email field")

print(f"  Customers: {len(customers):,} records")

# Check orders JSONL
orders = []
with open("/tmp/nexusflow-local-test/orders.jsonl") as f:
    for line in f:
        o = json.loads(line)
        orders.append(o)
        if not o.get("order_id"):
            errors.append("order missing order_id")
        if o.get("total_amount", -1) < 0:
            errors.append(f"order has negative total: {o['order_id']}")
        if not o.get("items"):
            errors.append(f"order has no items: {o['order_id']}")

print(f"  Orders: {len(orders):,} records")

# Check inventory CSV
with open("/tmp/nexusflow-local-test/inventory.csv") as f:
    reader = csv.DictReader(f)
    inv_rows = list(reader)
    required = {"snapshot_ts","warehouse_id","product_sku",
                "quantity_on_hand","quantity_available"}
    missing  = required - set(reader.fieldnames or [])
    if missing:
        errors.append(f"inventory missing columns: {missing}")

print(f"  Inventory: {len(inv_rows):,} rows")

# Check reviews XML
tree  = ET.parse("/tmp/nexusflow-local-test/reviews.xml")
root  = tree.getroot()
revs  = root.findall("review")
print(f"  Reviews: {len(revs):,} records")

if errors:
    print(f"\n❌ Data quality errors:")
    for e in errors:
        print(f"   {e}")
    sys.exit(1)
else:
    print(f"\n✅ All data quality checks passed")
EOF

pass "Data quality validation passed"

# ── STEP 5: TEST BATCH GENERATOR SCRIPT ───────────────────
info "Step 5: Testing batch_generator.py..."

$PYTHON src/datagen/batch_generator.py \
  --mode local \
  --output-dir "$OUTPUT_DIR/batch_test" \
  --num-customers 50 \
  --num-orders 500 \
  --num-reviews 100 \
  2>&1 | tail -5

pass "batch_generator.py runs without errors"

# ── STEP 6: DOCKER BUILD TEST ─────────────────────────────
info "Step 6: Building Docker image..."

docker build \
  -t nexusflow-datagen:test \
  --platform linux/amd64 \
  src/datagen/ \
  2>&1 | tail -10

docker image inspect nexusflow-datagen:test \
  --format "{{.Id}}" > /dev/null || \
  fail "Docker image not built"

pass "Docker image built successfully"

# ── STEP 7: TEST DOCKER IMAGE LOCALLY ─────────────────────
info "Step 7: Testing Docker image runs correctly..."

docker run --rm \
  --platform linux/amd64 \
  -v "$OUTPUT_DIR:/tmp/nexusflow-data" \
  nexusflow-datagen:test \
  ecommerce_generator.py \
  --mode batch \
  --output-dir /tmp/nexusflow-data/docker_test \
  --num-customers 10 \
  --num-orders 50 \
  2>&1 | tail -5

pass "Docker image runs correctly"

# ── STEP 8: VERIFY AWS CREDENTIALS ────────────────────────
info "Step 8: Verifying AWS credentials..."

IDENTITY=$(aws sts get-caller-identity --output json 2>/dev/null)
if [ $? -eq 0 ]; then
  ACCOUNT=$(echo "$IDENTITY" | python3 -c "import json,sys; print(json.load(sys.stdin)['Account'])")
  USER=$(echo "$IDENTITY" | python3 -c "import json,sys; print(json.load(sys.stdin)['Arn'])")
  pass "AWS credentials valid — Account: $ACCOUNT"
  info "  Identity: $USER"
else
  warn "AWS credentials not configured or expired"
  warn "Run: aws configure"
  warn "Kafka producer and S3 tests will be skipped"
fi

# ── STEP 9: CHECK DOCKER IMAGE SIZE ───────────────────────
info "Step 9: Checking Docker image size..."

IMAGE_SIZE=$(docker image inspect nexusflow-datagen:test \
  --format "{{.Size}}" | \
  awk '{printf "%.0f MB", $1/1048576}')

pass "Docker image size: $IMAGE_SIZE"

# ── CLEANUP ───────────────────────────────────────────────
info "Cleaning up test Docker image..."
docker rmi nexusflow-datagen:test --force 2>/dev/null || true

# ── SUMMARY ───────────────────────────────────────────────
echo ""
echo -e "${CYAN}══════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  ✅ Sprint 2 local validation PASSED${NC}"
echo -e "${CYAN}══════════════════════════════════════════════════${NC}"
echo ""
echo "Generated test data at: $OUTPUT_DIR"
echo ""
echo "Next steps:"
echo "  1. Run terraform apply (rebuild Sprint 1 infra)"
echo "  2. Get MSK broker: terraform output msk_bootstrap_brokers"
echo "  3. Get ECR registry: terraform output ecr_registry"
echo "  4. Update .env with real values"
echo "  5. Push Docker image to ECR"
echo "  6. Deploy to EKS: kubectl apply -f kubernetes/kafka/"
echo "  7. Verify S3: aws s3 ls s3://nexusflow-dev-lakehouse/bronze/"



