#!/bin/bash
# Start 2 Milvus clusters for CDC failover tests (no TLS), aligned with cluster+streaming port layout.
#
# Cluster A (by-dev1): proxy gRPC=19530, proxy-internal=19527, metrics proxy=19101
# Cluster B (by-dev2): proxy gRPC(external)=19531 (tests/default client), proxy-internal=19528 (standby internals), metrics proxy=19201
#
# Prerequisites:
#   - MILVUS_DEV_PATH: Milvus source directory with built binaries (bin/milvus)
#   - MILVUS_VOLUME_DIRECTORY: writable base directory for data/logs
#
# Usage:
#   export MILVUS_DEV_PATH=~/workspace/milvus
#   export MILVUS_VOLUME_DIRECTORY=~/milvus-vol
#   bash start_clusters.sh

set -e

if [[ -z "${MILVUS_DEV_PATH}" ]]; then
  echo "MILVUS_DEV_PATH is not set"; exit 1
fi
if [[ -z "${MILVUS_VOLUME_DIRECTORY}" ]]; then
  echo "MILVUS_VOLUME_DIRECTORY is not set"; exit 1
fi

COMPOSE_FILE="${HOME}/workspace/snippets/milvus_control/docker-compose-pulsar.yml"
if [[ ! -f "${COMPOSE_FILE}" ]]; then
  echo "Compose file not found: ${COMPOSE_FILE}"; exit 1
fi

echo "=== Starting third-party services (etcd/minio/pulsar) ==="
export MILVUS_INF_VOLUME_DIRECTORY="${MILVUS_VOLUME_DIRECTORY}"
docker compose -f "${COMPOSE_FILE}" up -d

echo "Third-party services started."

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
# Cluster A (by-dev1)
# ============================================================
echo "=== Starting Cluster A (by-dev1) ==="
A_ENV="msgChannel_CHANNAMEPREFIX_CLUSTER=by-dev1 ETCD_ROOTPATH=by-dev1 MINIO_rootPath=by-dev1"

# MixCoord
env ${A_ENV} ROOTCOORD_PORT=53100 QUERYCOORD_PORT=53110 DATACOORD_PORT=53120 INDEXCOORD_PORT=53130 METRICS_PORT=19100 ./bin/milvus run mixture -rootcoord -querycoord -datacoord -indexcoord \
  1>>"${LOG}/a-mixcoord.stdout.log" 2>>"${LOG}/a-mixcoord.stderr.log" &

# Proxy (external 19530; internal 19527; metrics 19101)
env ${A_ENV} PROXY_PORT=19530 PROXY_HTTP_PORT=18530 PROXY_INTERNAL_PORT=19527 METRICS_PORT=19101 ./bin/milvus run proxy \
  1>>"${LOG}/a-proxy.stdout.log" 2>>"${LOG}/a-proxy.stderr.log" &

# Data/Index/Streaming/Query nodes — pass PROXY_INTERNAL_PORT so these pods
# don't default to 19529 and accidentally talk to another cluster's proxy.
env ${A_ENV} PROXY_INTERNAL_PORT=19527 METRICS_PORT=19102 ./bin/milvus run datanode \
  1>>"${LOG}/a-datanode.stdout.log" 2>>"${LOG}/a-datanode.stderr.log" &

env ${A_ENV} PROXY_INTERNAL_PORT=19527 METRICS_PORT=19103 ./bin/milvus run indexnode \
  1>>"${LOG}/a-indexnode.stdout.log" 2>>"${LOG}/a-indexnode.stderr.log" &

env ${A_ENV} PROXY_INTERNAL_PORT=19527 METRICS_PORT=19104 LOCALSTORAGE_PATH="${VOL}/a-sn1/data/" ./bin/milvus run streamingnode \
  1>>"${LOG}/a-sn1.stdout.log" 2>>"${LOG}/a-sn1.stderr.log" &

env ${A_ENV} PROXY_INTERNAL_PORT=19527 METRICS_PORT=19105 LOCALSTORAGE_PATH="${VOL}/a-sn2/data/" ./bin/milvus run streamingnode \
  1>>"${LOG}/a-sn2.stdout.log" 2>>"${LOG}/a-sn2.stderr.log" &

env ${A_ENV} PROXY_INTERNAL_PORT=19527 METRICS_PORT=19106 LOCALSTORAGE_PATH="${VOL}/a-sn3/data/" ./bin/milvus run streamingnode \
  1>>"${LOG}/a-sn3.stdout.log" 2>>"${LOG}/a-sn3.stderr.log" &

env ${A_ENV} PROXY_INTERNAL_PORT=19527 METRICS_PORT=19107 LOCALSTORAGE_PATH="${VOL}/a-qn1/data/" ./bin/milvus run querynode \
  1>>"${LOG}/a-qn1.stdout.log" 2>>"${LOG}/a-qn1.stderr.log" &

env ${A_ENV} PROXY_INTERNAL_PORT=19527 METRICS_PORT=19108 LOCALSTORAGE_PATH="${VOL}/a-qn2/data/" ./bin/milvus run querynode \
  1>>"${LOG}/a-qn2.stdout.log" 2>>"${LOG}/a-qn2.stderr.log" &

env ${A_ENV} PROXY_INTERNAL_PORT=19527 METRICS_PORT=19109 LOCALSTORAGE_PATH="${VOL}/a-qn3/data/" ./bin/milvus run querynode \
  1>>"${LOG}/a-qn3.stdout.log" 2>>"${LOG}/a-qn3.stderr.log" &

# CDC for A
env ${A_ENV} PROXY_PORT=19530 PROXY_INTERNAL_PORT=19527 METRICS_PORT=19150 ./bin/milvus run cdc \
  >"${LOG}/a-cdc.stdout.log" 2>&1 &

echo "Cluster A started (11 processes)"

# ============================================================
# Cluster B (by-dev2)
# ============================================================
echo "=== Starting Cluster B (by-dev2) ==="
B_ENV="msgChannel_CHANNAMEPREFIX_CLUSTER=by-dev2 ETCD_ROOTPATH=by-dev2 MINIO_rootPath=by-dev2"
B_EXT=19531
B_HTTP=18531

# MixCoord (separate ports)
env ${B_ENV} ROOTCOORD_PORT=53101 QUERYCOORD_PORT=53111 DATACOORD_PORT=53121 INDEXCOORD_PORT=53131 METRICS_PORT=19200 ./bin/milvus run mixture -rootcoord -querycoord -datacoord -indexcoord \
  1>>"${LOG}/b-mixcoord.stdout.log" 2>>"${LOG}/b-mixcoord.stderr.log" &

# Proxy: external 19531 for tests/default client; internal fixed 19528 for standby internals
env ${B_ENV} PROXY_PORT=${B_EXT} PROXY_HTTP_PORT=${B_HTTP} PROXY_INTERNAL_PORT=19528 ROOTCOORD_PORT=53101 METRICS_PORT=19201 ./bin/milvus run proxy \
  1>>"${LOG}/b-proxy.stdout.log" 2>>"${LOG}/b-proxy.stderr.log" &

# Data/Index/Streaming/Query nodes
env ${B_ENV} PROXY_INTERNAL_PORT=19528 ROOTCOORD_PORT=53101 METRICS_PORT=19202 ./bin/milvus run datanode \
  1>>"${LOG}/b-datanode.stdout.log" 2>>"${LOG}/b-datanode.stderr.log" &

env ${B_ENV} PROXY_INTERNAL_PORT=19528 ROOTCOORD_PORT=53101 METRICS_PORT=19203 ./bin/milvus run indexnode \
  1>>"${LOG}/b-indexnode.stdout.log" 2>>"${LOG}/b-indexnode.stderr.log" &

env ${B_ENV} PROXY_INTERNAL_PORT=19528 ROOTCOORD_PORT=53101 METRICS_PORT=19204 LOCALSTORAGE_PATH="${VOL}/b-sn1/data/" ./bin/milvus run streamingnode \
  1>>"${LOG}/b-sn1.stdout.log" 2>>"${LOG}/b-sn1.stderr.log" &

env ${B_ENV} PROXY_INTERNAL_PORT=19528 ROOTCOORD_PORT=53101 METRICS_PORT=19205 LOCALSTORAGE_PATH="${VOL}/b-sn2/data/" ./bin/milvus run streamingnode \
  1>>"${LOG}/b-sn2.stdout.log" 2>>"${LOG}/b-sn2.stderr.log" &

env ${B_ENV} PROXY_INTERNAL_PORT=19528 ROOTCOORD_PORT=53101 METRICS_PORT=19206 LOCALSTORAGE_PATH="${VOL}/b-sn3/data/" ./bin/milvus run streamingnode \
  1>>"${LOG}/b-sn3.stdout.log" 2>>"${LOG}/b-sn3.stderr.log" &

env ${B_ENV} PROXY_INTERNAL_PORT=19528 ROOTCOORD_PORT=53101 METRICS_PORT=19207 LOCALSTORAGE_PATH="${VOL}/b-qn1/data/" ./bin/milvus run querynode \
  1>>"${LOG}/b-qn1.stdout.log" 2>>"${LOG}/b-qn1.stderr.log" &

env ${B_ENV} PROXY_INTERNAL_PORT=19528 ROOTCOORD_PORT=53101 METRICS_PORT=19208 LOCALSTORAGE_PATH="${VOL}/b-qn2/data/" ./bin/milvus run querynode \
  1>>"${LOG}/b-qn2.stdout.log" 2>>"${LOG}/b-qn2.stderr.log" &

env ${B_ENV} PROXY_INTERNAL_PORT=19528 ROOTCOORD_PORT=53101 METRICS_PORT=19209 LOCALSTORAGE_PATH="${VOL}/b-qn3/data/" ./bin/milvus run querynode \
  1>>"${LOG}/b-qn3.stdout.log" 2>>"${LOG}/b-qn3.stderr.log" &

# CDC for B
env ${B_ENV} PROXY_PORT=${B_EXT} PROXY_INTERNAL_PORT=19528 ROOTCOORD_PORT=53101 METRICS_PORT=19250 ./bin/milvus run cdc \
  >"${LOG}/b-cdc.stdout.log" 2>&1 &

echo "Cluster B started (11 processes)"

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

# Export client endpoints for convenience
ENV_FILE="${VOL}/cluster_env.sh"
cat > "${ENV_FILE}" <<EOF
export CLUSTER_A_ADDR=tcp://localhost:19530
export CLUSTER_B_ADDR=tcp://localhost:19531
export CLUSTER_C_ADDR=tcp://localhost:19532
export MILVUS_TOKEN=root:Milvus
EOF

echo ""
echo "=== All 3 clusters started ==="
echo "Cluster A (by-dev1): external grpc \`tcp://localhost:19530\`, internal grpc \`tcp://localhost:19527\`, metrics=19101"
echo "Cluster B (by-dev2): external grpc \`tcp://localhost:19531\`, internal grpc \`tcp://localhost:19528\`, metrics=19201"
echo "Cluster C (by-dev3): external grpc \`tcp://localhost:19532\`, internal grpc \`tcp://localhost:19526\`, metrics=19301"
echo "Logs:  ${LOG}"
echo "Env file: ${ENV_FILE} (source this to set CLUSTER_*_ADDR)"
echo ""
echo "Wait ~20-30s, then health check:"
echo "  curl -s http://127.0.0.1:19101/healthz; echo"
echo "  curl -s http://127.0.0.1:19201/healthz; echo"
echo "  curl -s http://127.0.0.1:19301/healthz; echo"
