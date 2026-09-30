#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/compose.dev.yaml"
K6_SCRIPT="$ROOT_DIR/scripts/k6/events-burst.js"
WORKERS=20
RATE_PER_WORKER=500
DURATION=5s
TEST_RATE_LIMIT=6000
K6_IMAGE="${K6_IMAGE:-grafana/k6}"
RUN_ID="k6-10000-5s-$(date +%s)-$$"
LOG_DIR="$(mktemp -d "${TMPDIR:-/tmp}/k6-10000-5s.XXXXXX")"
PIDS=()
CONTAINERS=()

if ! command -v docker >/dev/null || ! docker compose version >/dev/null 2>&1; then
  echo 'Docker with the Compose plugin is required.' >&2
  exit 1
fi
if ! command -v curl >/dev/null; then
  echo 'curl is required to check API readiness.' >&2
  exit 1
fi
if ! command -v python3 >/dev/null; then
  echo 'python3 is required to summarize k6 results.' >&2
  exit 1
fi

# Preserve the running API's current per-IP limit, including non-default local settings.
API_CONTAINER="$(docker compose -f "$COMPOSE_FILE" ps -q api 2>/dev/null || true)"
if [[ -n "$API_CONTAINER" ]]; then
  ORIGINAL_RATE_LIMIT="$(docker inspect "$API_CONTAINER" --format '{{range .Config.Env}}{{println .}}{{end}}' \
    | sed -n 's/^INGESTION_RATE_LIMIT=//p' | head -n 1)"
fi
ORIGINAL_RATE_LIMIT="${ORIGINAL_RATE_LIMIT:-${INGESTION_RATE_LIMIT:-120}}"

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM
  for container in "${CONTAINERS[@]}"; do
    docker rm -f "$container" >/dev/null 2>&1 || true
  done
  for pid in "${PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
  echo "Restoring API ingestion limit to $ORIGINAL_RATE_LIMIT/min..."
  INGESTION_RATE_LIMIT="$ORIGINAL_RATE_LIMIT" docker compose -f "$COMPOSE_FILE" \
    up -d --force-recreate api >/dev/null || echo 'Warning: failed to restore the API; check Docker Compose.' >&2
  echo "k6 logs: $LOG_DIR"
  exit "$exit_code"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

cd "$ROOT_DIR"
echo "Starting local dev API with temporary limit $TEST_RATE_LIMIT requests/minute per source IP..."
INGESTION_RATE_LIMIT="$TEST_RATE_LIMIT" docker compose -f "$COMPOSE_FILE" \
  up -d --force-recreate api

ready=0
for _ in $(seq 1 60); do
  if curl -fsS http://127.0.0.1:3002/health/ready >/dev/null; then
    ready=1
    break
  fi
  sleep 1
done
if [[ "$ready" != 1 ]]; then
  echo 'API did not become ready at http://127.0.0.1:3002.' >&2
  docker compose -f "$COMPOSE_FILE" logs --tail=80 api >&2 || true
  exit 1
fi

API_CONTAINER="$(docker compose -f "$COMPOSE_FILE" ps -q api)"
NETWORK="$(docker inspect "$API_CONTAINER" --format '{{range $name, $config := .NetworkSettings.Networks}}{{println $name}}{{end}}' | head -n 1)"
if [[ -z "$NETWORK" ]]; then
  echo 'Could not determine the Compose network for the API.' >&2
  exit 1
fi

echo "Running $((WORKERS * RATE_PER_WORKER)) requests/sec for $DURATION from $WORKERS Docker IPs."
echo 'This writes test events to the local development database; the script does not delete them.'
echo "Results will be saved in $LOG_DIR"

result=0
for worker in $(seq 1 "$WORKERS"); do
  name="${RUN_ID}-worker-${worker}"
  CONTAINERS+=("$name")
  docker run --rm --name "$name" --network "$NETWORK" \
    -v "$ROOT_DIR/scripts/k6:/scripts:ro" \
    -e RATE="$RATE_PER_WORKER" -e DURATION="$DURATION" -e WORKER_ID="$worker" \
    "$K6_IMAGE" run /scripts/events-burst.js >"$LOG_DIR/worker-${worker}.log" 2>&1 &
  PIDS+=("$!")
done

for pid in "${PIDS[@]}"; do
  wait "$pid" || result=1
done

python3 - "$LOG_DIR" "$WORKERS" "$RATE_PER_WORKER" <<'PY'
import glob
import re
import sys

log_dir, workers, rate = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
files = glob.glob(f'{log_dir}/worker-*.log')
completed = dropped = failed_responses = 0
p95_ms = []
passed = 0
for path in files:
    text = open(path, encoding='utf-8').read()
    match = re.search(r'http_reqs[.]*:\s+(\d+)', text)
    if match:
        completed += int(match.group(1))
    match = re.search(r'dropped_iterations[.]*:\s+(\d+)', text)
    if match:
        dropped += int(match.group(1))
    match = re.search(r'http_req_failed[.]*:\s+[\d.]+%\s+(\d+)\s+out of', text)
    if match:
        failed_responses += int(match.group(1))
    match = re.search(r'p\(95\)=([\d.]+)(µs|ms|s)', text)
    if match:
        scale = {'µs': 0.001, 'ms': 1, 's': 1000}[match.group(2)]
        p95_ms.append(float(match.group(1)) * scale)
    if "rate>0.99' rate=100.00%" in text and "rate<0.01' rate=0.00%" in text and 'count==0' in text and '✗' not in text:
        passed += 1
print(f'Workers: {len(files)}/{workers}; target: {workers * rate} req/s for 5s')
print(f'Completed: {completed} requests ({completed / 5:.1f} req/s); dropped k6 iterations: {dropped}; failed HTTP responses: {failed_responses}')
if p95_ms:
    print(f'Per-worker p95 latency range: {min(p95_ms):.1f}-{max(p95_ms):.1f} ms')
print(f'Workers passing all thresholds: {passed}/{workers}')
PY

if [[ "$result" != 0 ]]; then
  echo 'The load test missed one or more k6 thresholds; inspect the per-worker logs above.' >&2
  exit 1
fi
