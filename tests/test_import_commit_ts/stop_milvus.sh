#!/bin/bash
# Stop Milvus standalone and third-party services.
#
# Usage:
#   bash stop_milvus.sh              # stop everything
#   bash stop_milvus.sh --keep-infra # stop Milvus only

set -e

MILVUS_CONTROL="${HOME}/workspace/snippets/milvus_control/milvus_control"

if [[ "$1" == "--keep-infra" ]]; then
  "${MILVUS_CONTROL}" -f stop_milvus
else
  "${MILVUS_CONTROL}" -f stop_milvus_full
fi
