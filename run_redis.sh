#!/usr/bin/env bash
# FastQ - Start a local Redis instance for development
set -euo pipefail

REDIS_PORT="${FASTQ_REDIS_PORT:-6379}"
REDIS_DIR="$(mktemp -d /tmp/fastq-redis.XXXXXX)"

cleanup() {
    echo "[fastq-dev] Stopping Redis..."
    redis-cli -p "$REDIS_PORT" shutdown nosave 2>/dev/null || true
    rm -rf "$REDIS_DIR"
}
trap cleanup EXIT

echo "[fastq-dev] Starting Redis on port $REDIS_PORT (data: $REDIS_DIR)"
redis-server \
    --port "$REDIS_PORT" \
    --dir "$REDIS_DIR" \
    --save "" \
    --appendonly no \
    --loglevel notice \
    --daemonize no
