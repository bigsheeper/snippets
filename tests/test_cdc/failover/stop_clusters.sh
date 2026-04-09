#!/bin/bash
# Stop all Milvus processes for the 2‑cluster failover setup.
# Usage: bash stop_clusters.sh

set -e

echo "Stopping all milvus processes..."
pkill -f "milvus run" || true
sleep 2

REMAINING=$(pgrep -f "milvus run" || true)
if [[ -n "${REMAINING}" ]]; then
  echo "Force-killing remaining processes..."
  pkill -9 -f "milvus run" || true
  sleep 1
fi

echo "All clusters stopped."

COMPOSE_FILE="${HOME}/workspace/snippets/milvus_control/docker-compose-pulsar.yml"
if [[ -f "${COMPOSE_FILE}" ]]; then
  echo "=== Stopping third-party services (etcd/minio/pulsar) ==="
  export MILVUS_INF_VOLUME_DIRECTORY="${MILVUS_VOLUME_DIRECTORY}"
  docker compose -f "${COMPOSE_FILE}" down
  echo "Third-party services stopped."
else
  echo "Compose file not found, skip stopping third-party services: ${COMPOSE_FILE}"
fi
