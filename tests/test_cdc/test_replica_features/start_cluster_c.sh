#!/bin/bash
# Start only cluster C (by-dev3). Used by restart_all_three_clusters in tests
# that need to scale C independently (e.g. M4's pchannel scale-up phase).
#
# Prerequisites:
#   - MILVUS_DEV_PATH: Milvus source directory with built binaries (bin/milvus)
#   - MILVUS_VOLUME_DIRECTORY: writable base directory for data/logs
#   - third-party services (etcd/minio/pulsar) already running

set -e

if [[ -z "${MILVUS_DEV_PATH}" ]]; then
  echo "MILVUS_DEV_PATH is not set"; exit 1
fi
if [[ -z "${MILVUS_VOLUME_DIRECTORY}" ]]; then
  echo "MILVUS_VOLUME_DIRECTORY is not set"; exit 1
fi

WORKSPACE_TAG=${WORKSPACE_TAG:-$(date +%F-%H-%M-%S)}
VOL="${MILVUS_VOLUME_DIRECTORY}/${WORKSPACE_TAG}-c"
LOG="${VOL}/milvus-logs"
mkdir -p "${LOG}"
echo "Volume: ${VOL}"
echo "Logs:   ${LOG}"

cd "${MILVUS_DEV_PATH}"
source "./scripts/setenv.sh"

export LOG_LEVEL=debug
export MQ_TYPE=pulsar
export MILVUS_STREAMING_SERVICE_ENABLED=1

# ============================================================
# Cluster C (by-dev3)
# ============================================================
echo "=== Starting Cluster C (by-dev3) ==="
C_ENV="msgChannel_CHANNAMEPREFIX_CLUSTER=by-dev3 ETCD_ROOTPATH=by-dev3 MINIO_rootPath=by-dev3"
C_EXT=19532
C_HTTP=18532
C_INTERNAL=19526

env ${C_ENV} ROOTCOORD_PORT=53102 QUERYCOORD_PORT=53112 DATACOORD_PORT=53122 INDEXCOORD_PORT=53132 METRICS_PORT=19300 ./bin/milvus run mixture -rootcoord -querycoord -datacoord -indexcoord \
  1>>"${LOG}/c-mixcoord.stdout.log" 2>>"${LOG}/c-mixcoord.stderr.log" &

env ${C_ENV} PROXY_PORT=${C_EXT} PROXY_HTTP_PORT=${C_HTTP} PROXY_INTERNAL_PORT=${C_INTERNAL} ROOTCOORD_PORT=53102 METRICS_PORT=19301 ./bin/milvus run proxy \
  1>>"${LOG}/c-proxy.stdout.log" 2>>"${LOG}/c-proxy.stderr.log" &

env ${C_ENV} PROXY_INTERNAL_PORT=${C_INTERNAL} ROOTCOORD_PORT=53102 METRICS_PORT=19302 ./bin/milvus run datanode \
  1>>"${LOG}/c-datanode.stdout.log" 2>>"${LOG}/c-datanode.stderr.log" &

env ${C_ENV} PROXY_INTERNAL_PORT=${C_INTERNAL} ROOTCOORD_PORT=53102 METRICS_PORT=19303 ./bin/milvus run indexnode \
  1>>"${LOG}/c-indexnode.stdout.log" 2>>"${LOG}/c-indexnode.stderr.log" &

env ${C_ENV} PROXY_INTERNAL_PORT=${C_INTERNAL} ROOTCOORD_PORT=53102 METRICS_PORT=19304 LOCALSTORAGE_PATH="${VOL}/c-sn1/data/" ./bin/milvus run streamingnode \
  1>>"${LOG}/c-sn1.stdout.log" 2>>"${LOG}/c-sn1.stderr.log" &

env ${C_ENV} PROXY_INTERNAL_PORT=${C_INTERNAL} ROOTCOORD_PORT=53102 METRICS_PORT=19305 LOCALSTORAGE_PATH="${VOL}/c-sn2/data/" ./bin/milvus run streamingnode \
  1>>"${LOG}/c-sn2.stdout.log" 2>>"${LOG}/c-sn2.stderr.log" &

env ${C_ENV} PROXY_INTERNAL_PORT=${C_INTERNAL} ROOTCOORD_PORT=53102 METRICS_PORT=19306 LOCALSTORAGE_PATH="${VOL}/c-sn3/data/" ./bin/milvus run streamingnode \
  1>>"${LOG}/c-sn3.stdout.log" 2>>"${LOG}/c-sn3.stderr.log" &

env ${C_ENV} PROXY_INTERNAL_PORT=${C_INTERNAL} ROOTCOORD_PORT=53102 METRICS_PORT=19307 LOCALSTORAGE_PATH="${VOL}/c-qn1/data/" ./bin/milvus run querynode \
  1>>"${LOG}/c-qn1.stdout.log" 2>>"${LOG}/c-qn1.stderr.log" &

env ${C_ENV} PROXY_INTERNAL_PORT=${C_INTERNAL} ROOTCOORD_PORT=53102 METRICS_PORT=19308 LOCALSTORAGE_PATH="${VOL}/c-qn2/data/" ./bin/milvus run querynode \
  1>>"${LOG}/c-qn2.stdout.log" 2>>"${LOG}/c-qn2.stderr.log" &

env ${C_ENV} PROXY_INTERNAL_PORT=${C_INTERNAL} ROOTCOORD_PORT=53102 METRICS_PORT=19309 LOCALSTORAGE_PATH="${VOL}/c-qn3/data/" ./bin/milvus run querynode \
  1>>"${LOG}/c-qn3.stdout.log" 2>>"${LOG}/c-qn3.stderr.log" &

env ${C_ENV} PROXY_PORT=${C_EXT} PROXY_INTERNAL_PORT=${C_INTERNAL} ROOTCOORD_PORT=53102 METRICS_PORT=19350 ./bin/milvus run cdc \
  >"${LOG}/c-cdc.stdout.log" 2>&1 &

echo "Cluster C started (11 processes)"
