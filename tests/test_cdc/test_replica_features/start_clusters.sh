#!/bin/bash
# Start two Milvus clusters in cluster mode with streaming enabled.
# Each cluster has 3 streaming nodes to support up to 3 replicas.
#
# Cluster A (by-dev1): proxy port 19530
# Cluster B (by-dev2): proxy port 19531, local replica config = 2
#
# Usage: ./start_clusters.sh

set -e

if [[ -z "${MILVUS_DEV_PATH}" ]]; then
    echo "MILVUS_DEV_PATH is not set"
    exit 1
fi
if [[ -z "${MILVUS_VOLUME_DIRECTORY}" ]]; then
    echo "MILVUS_VOLUME_DIRECTORY is not set"
    exit 1
fi

WORKSPACE_TAG=${WORKSPACE_TAG:-$(date +%F-%H-%M-%S)}
VOL="${MILVUS_VOLUME_DIRECTORY}/${WORKSPACE_TAG}"
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
# Cluster A (by-dev1): proxy=19530
# ============================================================
echo "=== Starting Cluster A (by-dev1) ==="

A_ENV="msgChannel_CHANNAMEPREFIX_CLUSTER=by-dev1 ETCD_ROOTPATH=by-dev1 MINIO_rootPath=by-dev1"

env ${A_ENV} METRICS_PORT=19091 ./bin/milvus run mixture -rootcoord -querycoord -datacoord -indexcoord \
    1>>"${LOG}/a-mixcoord.stdout.log" 2>>"${LOG}/a-mixcoord.stderr.log" &

env ${A_ENV} PROXY_PORT=19530 METRICS_PORT=19191 ./bin/milvus run proxy \
    1>>"${LOG}/a-proxy.stdout.log" 2>>"${LOG}/a-proxy.stderr.log" &

env ${A_ENV} METRICS_PORT=19291 ./bin/milvus run datanode \
    1>>"${LOG}/a-datanode.stdout.log" 2>>"${LOG}/a-datanode.stderr.log" &

env ${A_ENV} METRICS_PORT=19391 ./bin/milvus run indexnode \
    1>>"${LOG}/a-indexnode.stdout.log" 2>>"${LOG}/a-indexnode.stderr.log" &

# 3 streaming nodes for cluster A (supports up to 3 replicas)
env ${A_ENV} METRICS_PORT=19491 LOCALSTORAGE_PATH="${VOL}/a-sn1/data/" ./bin/milvus run streamingnode \
    1>>"${LOG}/a-streamingnode1.stdout.log" 2>>"${LOG}/a-streamingnode1.stderr.log" &

env ${A_ENV} METRICS_PORT=19492 LOCALSTORAGE_PATH="${VOL}/a-sn2/data/" ./bin/milvus run streamingnode \
    1>>"${LOG}/a-streamingnode2.stdout.log" 2>>"${LOG}/a-streamingnode2.stderr.log" &

env ${A_ENV} METRICS_PORT=19493 LOCALSTORAGE_PATH="${VOL}/a-sn3/data/" ./bin/milvus run streamingnode \
    1>>"${LOG}/a-streamingnode3.stdout.log" 2>>"${LOG}/a-streamingnode3.stderr.log" &

# CDC for cluster A
env ${A_ENV} PROXY_PORT=19530 METRICS_PORT=29091 ./bin/milvus run cdc \
    >"${LOG}/a-cdc.stdout.log" 2>&1 &

echo "Cluster A started (8 processes)"

# ============================================================
# Cluster B (by-dev2): proxy=19531, local replica=2
# ============================================================
echo "=== Starting Cluster B (by-dev2) ==="

B_ENV="msgChannel_CHANNAMEPREFIX_CLUSTER=by-dev2 ETCD_ROOTPATH=by-dev2 MINIO_rootPath=by-dev2"

env ${B_ENV} METRICS_PORT=19092 QUERYCOORD_CLUSTERLEVELLOADREPLICANUMBER=2 \
    ./bin/milvus run mixture -rootcoord -querycoord -datacoord -indexcoord \
    1>>"${LOG}/b-mixcoord.stdout.log" 2>>"${LOG}/b-mixcoord.stderr.log" &

env ${B_ENV} PROXY_PORT=19531 PROXY_INTERNAL_PORT=19528 ROOTCOORD_PORT=53101 METRICS_PORT=19192 ./bin/milvus run proxy \
    1>>"${LOG}/b-proxy.stdout.log" 2>>"${LOG}/b-proxy.stderr.log" &

env ${B_ENV} METRICS_PORT=19292 ./bin/milvus run datanode \
    1>>"${LOG}/b-datanode.stdout.log" 2>>"${LOG}/b-datanode.stderr.log" &

env ${B_ENV} METRICS_PORT=19392 ./bin/milvus run indexnode \
    1>>"${LOG}/b-indexnode.stdout.log" 2>>"${LOG}/b-indexnode.stderr.log" &

# 3 streaming nodes for cluster B
env ${B_ENV} METRICS_PORT=19591 LOCALSTORAGE_PATH="${VOL}/b-sn1/data/" ./bin/milvus run streamingnode \
    1>>"${LOG}/b-streamingnode1.stdout.log" 2>>"${LOG}/b-streamingnode1.stderr.log" &

env ${B_ENV} METRICS_PORT=19592 LOCALSTORAGE_PATH="${VOL}/b-sn2/data/" ./bin/milvus run streamingnode \
    1>>"${LOG}/b-streamingnode2.stdout.log" 2>>"${LOG}/b-streamingnode2.stderr.log" &

env ${B_ENV} METRICS_PORT=19593 LOCALSTORAGE_PATH="${VOL}/b-sn3/data/" ./bin/milvus run streamingnode \
    1>>"${LOG}/b-streamingnode3.stdout.log" 2>>"${LOG}/b-streamingnode3.stderr.log" &

# CDC for cluster B
env ${B_ENV} PROXY_PORT=19531 PROXY_INTERNAL_PORT=19528 ROOTCOORD_PORT=53101 METRICS_PORT=29092 ./bin/milvus run cdc \
    >"${LOG}/b-cdc.stdout.log" 2>&1 &

echo "Cluster B started (8 processes)"

echo ""
echo "=== All processes started ==="
echo "Cluster A proxy: http://localhost:19530"
echo "Cluster B proxy: http://localhost:19531"
echo "Cluster B local replica config: 2"
echo "Logs: ${LOG}"
echo ""
echo "Wait ~30s for clusters to be ready, then check:"
echo "  curl http://localhost:19191/healthz  # Cluster A proxy"
echo "  curl http://localhost:19192/healthz  # Cluster B proxy"
