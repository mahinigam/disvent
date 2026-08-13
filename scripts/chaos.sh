#!/usr/bin/env bash
set -eo pipefail

echo "========================================="
echo "   Disvent Chaos Engineering Injector    "
echo "========================================="
echo "This script will randomly kill and restart Redpanda and ClickHouse"
echo "containers to test PySpark checkpointing and pipeline resilience."
echo "Press Ctrl+C to stop."
echo ""

TARGETS=("disvent-redpanda-1" "disvent-clickhouse-1" "disvent-streaming-engine")

while true; do
  SLEEP_TIME=$((RANDOM % 20 + 10))
  echo "[*] Waiting ${SLEEP_TIME} seconds before next fault injection..."
  sleep $SLEEP_TIME

  TARGET=${TARGETS[$RANDOM % ${#TARGETS[@]}]}
  
  echo "[!] INJECTING FAULT: Killing container $TARGET"
  docker kill $TARGET > /dev/null

  DOWN_TIME=$((RANDOM % 10 + 5))
  echo "[*] Container $TARGET is down. Leaving it down for ${DOWN_TIME} seconds..."
  sleep $DOWN_TIME

  echo "[+] RECOVERING: Restarting container $TARGET"
  docker start $TARGET > /dev/null
  
  echo "[*] Recovery command sent. Pipeline should self-heal."
  echo "-----------------------------------------"
done
