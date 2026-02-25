#!/bin/bash
# Stop all Milvus processes (all 3 mTLS test clusters).
#
# Usage: ./stop_clusters.sh

set -e

echo "Stopping all milvus processes..."
pkill -f "milvus run" || true
sleep 2

# Verify no lingering processes
REMAINING=$(pgrep -f "milvus run" || true)
if [[ -n "${REMAINING}" ]]; then
    echo "Force-killing remaining processes..."
    pkill -9 -f "milvus run" || true
    sleep 1
fi

echo "All clusters stopped."
