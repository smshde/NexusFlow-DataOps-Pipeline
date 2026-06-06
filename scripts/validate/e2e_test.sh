#!/usr/bin/env bash
# ============================================================
# NexusFlow — End-to-End Pipeline Validation Script
# Usage: bash scripts/validate/e2e_test.sh dev
# ============================================================
set -euo pipefail

ENV="${1:-dev}"
PROJECT="nexusflow"
PASS=0; FAIL=0

GREEN='\033[0;32m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
pass() { echo -e "${GREEN}  ✅ PASS${NC}: $*"; PASS=$((PASS+1)); }
fail() { echo -e "${RED}  ❌ FAIL${NC}: $*"; FAIL=$((FAIL+1)); }
section() { echo -e "\n${CYAN}── $* ──${NC}"; }

echo -e "${CYAN}NexusFlow E2E Validation — ENV: ${ENV}${NC}"

# ── INFRA CHECKS ─────────────────────────────────────────
section "Infrastructure"

# EKS cluster reachable
if kubectl cluster-info &>/dev/null; then
  pass "EKS cluster reachable"
else
  fail "EKS cluster unreachable"
fi

# All pods running
UNHEALTHY=$(kubectl get pods -A --field-selector=status.phase!=Running \
  --no-headers 2>/dev/null | grep -v Completed | grep -v Terminating | wc -l)
if [ "${UNHEALTHY}" -eq 0 ]; then
  pass "All pods healthy"
else
  fail "${UNHEALTHY} pod(s) not in Running state"
  kubectl get pods -A --field-selector=status.phase!=Running --no-headers
fi

# ── KAFKA CHECKS ─────────────────────────────────────────
section "Kafka / MSK"

MSK_BROKERS=$(aws secretsmanager get-secret-value \
  --secret-id "nexusflow/${ENV}/msk" \
  --query SecretString --output text 2>/dev/null | jq -r '.bootstrap_servers' 2>/dev/null || echo "")

if [ -n "${MSK_BROKERS}" ]; then
  pass "MSK bootstrap brokers found"

  # Check topics exist
  for TOPIC in orders clickstream inventory-events product-reviews; do
    TOPIC_COUNT=$(kubectl exec -n nexusflow deploy/nexusflow-datagen -- \
      kafka-topics.sh --bootstrap-server "${MSK_BROKERS}" \
      --list 2>/dev/null | grep -c "^${TOPIC}$" || echo 0)
    if [ "${TOPIC_COUNT}" -gt 0 ]; then
      pass "Kafka topic '${TOPIC}' exists"
    else
      fail "Kafka topic '${TOPIC}' missing"
    fi
  done
else
  fail "Could not retrieve MSK broker address"
fi

# ── S3 CHECKS ────────────────────────────────────────────
section "S3 Lakehouse"

LAKEHOUSE_BUCKET="${PROJECT}-${ENV}-lakehouse"

for LAYER in bronze silver gold; do
  OBJ_COUNT=$(aws s3 ls "s3://${LAKEHOUSE_BUCKET}/${LAYER}/" --recursive \
    --summarize 2>/dev/null | grep "Total Objects" | awk '{print $3}')
  if [ "${OBJ_COUNT:-0}" -gt 0 ]; then
    pass "S3 ${LAYER} layer has ${OBJ_COUNT} objects"
  else
    fail "S3 ${LAYER} layer is empty or inaccessible"
  fi
done

# ── DATA FRESHNESS ────────────────────────────────────────
section "Data Freshness"

LATEST_ORDER_PARTITION=$(aws s3 ls "s3://${LAKEHOUSE_BUCKET}/silver/orders/" \
  --recursive 2>/dev/null | sort | tail -1 | awk '{print $4}' | grep -o 'order_year=[0-9]*/order_month=[0-9]*/order_day=[0-9]*' || echo "none")

if [ "${LATEST_ORDER_PARTITION}" != "none" ]; then
  pass "Silver orders partition found: ${LATEST_ORDER_PARTITION}"
else
  fail "No silver order partitions found"
fi

# ── API CHECKS ────────────────────────────────────────────
section "Analytics API"

API_HOST=$(kubectl get svc nexusflow-api -n nexusflow \
  -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || echo "localhost")
API_URL="http://${API_HOST}"

# Health endpoint
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${API_URL}/health" --max-time 10 || echo 000)
if [ "${HTTP_CODE}" = "200" ]; then
  pass "API /health → 200"
else
  fail "API /health → ${HTTP_CODE} (expected 200)"
fi

# Ready endpoint
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${API_URL}/ready" --max-time 10 || echo 000)
if [ "${HTTP_CODE}" = "200" ]; then
  pass "API /ready → 200"
else
  fail "API /ready → ${HTTP_CODE} (expected 200)"
fi

# Revenue summary (requires data)
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${API_URL}/api/v1/revenue/summary" --max-time 15 || echo 000)
if [ "${HTTP_CODE}" = "200" ]; then
  pass "API /revenue/summary → 200"
else
  fail "API /revenue/summary → ${HTTP_CODE}"
fi

# Pipeline health
HEALTH_JSON=$(curl -s "${API_URL}/api/v1/pipeline/health" --max-time 15 || echo '{}')
PIPELINE_STATUS=$(echo "${HEALTH_JSON}" | jq -r '.status' 2>/dev/null || echo "unknown")
if [ "${PIPELINE_STATUS}" = "healthy" ]; then
  pass "Pipeline health: ${PIPELINE_STATUS}"
elif [ "${PIPELINE_STATUS}" = "degraded" ]; then
  fail "Pipeline health: ${PIPELINE_STATUS} (data may be stale)"
else
  fail "Pipeline health: ${PIPELINE_STATUS}"
fi

# ── AIRFLOW CHECKS ────────────────────────────────────────
section "Airflow"

AIRFLOW_POD=$(kubectl get pods -n airflow -l component=scheduler \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

if [ -n "${AIRFLOW_POD}" ]; then
  pass "Airflow scheduler pod: ${AIRFLOW_POD}"

  # Check DAG is loaded
  DAG_COUNT=$(kubectl exec -n airflow "${AIRFLOW_POD}" -- \
    airflow dags list 2>/dev/null | grep -c "nexusflow_master_pipeline" || echo 0)
  if [ "${DAG_COUNT}" -gt 0 ]; then
    pass "Master pipeline DAG loaded"
  else
    fail "Master pipeline DAG not found in Airflow"
  fi
else
  fail "Airflow scheduler pod not found"
fi

# ── DBT CHECKS ────────────────────────────────────────────
section "dbt Models"

# Run dbt parse in-cluster
if kubectl get cronjob nexusflow-dbt -n nexusflow &>/dev/null; then
  pass "dbt CronJob exists"
else
  fail "dbt CronJob not found"
fi

# ── MONITORING CHECKS ────────────────────────────────────
section "Monitoring"

GRAFANA_POD=$(kubectl get pods -n monitoring -l app.kubernetes.io/name=grafana \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
if [ -n "${GRAFANA_POD}" ]; then
  pass "Grafana pod running: ${GRAFANA_POD}"
else
  fail "Grafana pod not found"
fi

PROMETHEUS_POD=$(kubectl get pods -n monitoring -l app=prometheus \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
if [ -n "${PROMETHEUS_POD}" ]; then
  pass "Prometheus pod running: ${PROMETHEUS_POD}"
else
  fail "Prometheus pod not found"
fi

# ── SUMMARY ──────────────────────────────────────────────
echo ""
echo -e "${CYAN}══════════════════════════════════${NC}"
echo -e "  Passed: ${GREEN}${PASS}${NC}  |  Failed: ${RED}${FAIL}${NC}"
echo -e "${CYAN}══════════════════════════════════${NC}"

if [ "${FAIL}" -gt 0 ]; then
  echo -e "${RED}E2E validation FAILED — ${FAIL} checks failed${NC}"
  exit 1
else
  echo -e "${GREEN}✅ All E2E checks passed!${NC}"
  exit 0
fi
