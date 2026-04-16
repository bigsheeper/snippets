#!/bin/bash
# Start Milvus standalone for import commit_timestamp tests.
#
# Usage:
#   export MILVUS_DEV_PATH=~/workspace/milvus/.worktrees/test-import-commit-ts
#   export MILVUS_VOLUME_DIRECTORY=/tmp/milvus-volumes
#   bash start_milvus.sh

set -e

MILVUS_CONTROL="${HOME}/workspace/snippets/milvus_control/milvus_control"

if [[ -z "${MILVUS_DEV_PATH}" ]]; then echo "Error: MILVUS_DEV_PATH is not set"; exit 1; fi
if [[ -z "${MILVUS_VOLUME_DIRECTORY}" ]]; then echo "Error: MILVUS_VOLUME_DIRECTORY is not set"; exit 1; fi

"${MILVUS_CONTROL}" start_milvus_full

echo ""
echo "Waiting for Milvus to be ready..."
for i in $(seq 1 60); do
  if curl -s http://localhost:19530/v2/vectordb/collections/list -X POST -d '{}' > /dev/null 2>&1; then
    echo "Milvus is ready! (took ~${i}s)"
    exit 0
  fi
  sleep 1
done
echo "Error: Milvus failed to start within 60s."
exit 1
