#!/usr/bin/env bash
set -euo pipefail

if [ -f .env ]; then
  export $(cat .env | grep -v '^#' | xargs)
fi

API_URL="${API_URL:-http://localhost:8001}"
API_KEY="${DISVENT_API_KEY:?DISVENT_API_KEY must be set in environment}"

echo "Waiting for API to become ready..."
max_retries=30
attempt=1

while [ $attempt -le $max_retries ]; do
  if curl -sS "$API_URL/api/v1/health" | grep -q '"status":"ok"'; then
    echo "API is ready!"
    break
  fi
  echo "Attempt $attempt/$max_retries: API not ready yet. Retrying in 2s..."
  sleep 2
  attempt=$((attempt + 1))
done

if [ $attempt -gt $max_retries ]; then
  echo "ERROR: API failed to become ready in time."
  exit 1
fi

headers=(-H "X-API-Key: $API_KEY")

echo "Running Integration Smoke Tests..."
errors=0

echo "1. Testing /api/v1/health"
health_resp=$(curl -fsS "$API_URL/api/v1/health")
echo "  Response: $health_resp"
if ! echo "$health_resp" | grep -q '"status":"ok"'; then
  echo "  [FAIL] Health check did not return ok"
  errors=$((errors + 1))
else
  echo "  [PASS] Health check is ok"
fi

echo "2. Testing /metrics (Prometheus)"
if curl -fsS "$API_URL/metrics" | grep -q "disvent_api_requests_total"; then
  echo "  [PASS] Prometheus metrics found"
else
  echo "  [FAIL] Prometheus metrics missing"
  errors=$((errors + 1))
fi

echo "3. Testing /api/v1/metrics/realtime-throughput"
throughput_resp=$(curl -fsS "${headers[@]}" "$API_URL/api/v1/metrics/realtime-throughput")
echo "  Response: $throughput_resp"
if echo "$throughput_resp" | grep -q "transactions_total"; then
  echo "  [PASS] Throughput metrics retrieved"
else
  echo "  [FAIL] Throughput metrics missing"
  errors=$((errors + 1))
fi

echo "4. Testing Authentication (Missing API Key)"
auth_resp=$(curl -sS -w "\n%{http_code}" "$API_URL/api/v1/metrics/realtime-throughput")
status_code=$(echo "$auth_resp" | tail -n1)
if [ "$status_code" -eq 401 ]; then
  echo "  [PASS] Auth enforcement works (401 returned)"
else
  echo "  [FAIL] Auth enforcement failed (returned $status_code)"
  errors=$((errors + 1))
fi

echo "5. Testing /api/v1/archive/recent"
archive_resp=$(curl -fsS -X POST "${headers[@]}" "$API_URL/api/v1/archive/recent?minutes=60")
echo "  Response: $archive_resp"
if echo "$archive_resp" | grep -q "records"; then
  echo "  [PASS] Archive request successful"
else
  echo "  [FAIL] Archive request failed"
  errors=$((errors + 1))
fi

if [ $errors -gt 0 ]; then
  echo "Smoke tests FAILED with $errors errors."
  exit 1
else
  echo "All smoke tests PASSED!"
  exit 0
fi
