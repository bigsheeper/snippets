#!/bin/bash
# Start 3 Milvus clusters with mTLS enabled (tlsMode=2).
#
# Cluster A (by-dev1): proxy=19530, metrics=19191
# Cluster B (by-dev2): proxy=19531, metrics=19192
# Cluster C (by-dev3): proxy=19532, metrics=19193
#
# Prerequisites:
#   - MILVUS_DEV_PATH set to Milvus source directory (with compiled binary)
#   - MILVUS_VOLUME_DIRECTORY set to volume storage path
#   - Certificates generated in ./certs/ (run gen_certs.sh first)
#
# Usage: ./start_clusters.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CERT_DIR="${SCRIPT_DIR}/certs"

# Generate certs if not present
if [[ ! -f "${CERT_DIR}/ca.pem" ]]; then
    echo "Generating certificates..."
    "${SCRIPT_DIR}/gen_certs.sh" "${CERT_DIR}"
fi

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

# TLS configuration (common to all clusters)
export COMMON_SECURITY_TLSMODE=2
export TLS_CAPEMPATH="${CERT_DIR}/ca.pem"

# ============================================================
# Cluster A (by-dev1): proxy=19530
# ============================================================
echo "=== Starting Cluster A (by-dev1) ==="

A_ENV="msgChannel_CHANNAMEPREFIX_CLUSTER=by-dev1 ETCD_ROOTPATH=by-dev1 MINIO_rootPath=by-dev1"
A_TLS="TLS_SERVERPEMPATH=${CERT_DIR}/cluster-a-server.pem TLS_SERVERKEYPATH=${CERT_DIR}/cluster-a-server.key"

env ${A_ENV} ${A_TLS} METRICS_PORT=19091 ./bin/milvus run mixture -rootcoord -querycoord -datacoord -indexcoord \
    1>>"${LOG}/a-mixcoord.stdout.log" 2>>"${LOG}/a-mixcoord.stderr.log" &

env ${A_ENV} ${A_TLS} PROXY_PORT=19530 METRICS_PORT=19191 ./bin/milvus run proxy \
    1>>"${LOG}/a-proxy.stdout.log" 2>>"${LOG}/a-proxy.stderr.log" &

env ${A_ENV} ${A_TLS} METRICS_PORT=19291 ./bin/milvus run datanode \
    1>>"${LOG}/a-datanode.stdout.log" 2>>"${LOG}/a-datanode.stderr.log" &

env ${A_ENV} ${A_TLS} METRICS_PORT=19391 ./bin/milvus run indexnode \
    1>>"${LOG}/a-indexnode.stdout.log" 2>>"${LOG}/a-indexnode.stderr.log" &

env ${A_ENV} ${A_TLS} METRICS_PORT=19491 LOCALSTORAGE_PATH="${VOL}/a-sn1/data/" ./bin/milvus run streamingnode \
    1>>"${LOG}/a-sn1.stdout.log" 2>>"${LOG}/a-sn1.stderr.log" &

# CDC for cluster A
env ${A_ENV} ${A_TLS} PROXY_PORT=19530 METRICS_PORT=29091 ./bin/milvus run cdc \
    >"${LOG}/a-cdc.stdout.log" 2>&1 &

echo "Cluster A started (6 processes)"

# ============================================================
# Cluster B (by-dev2): proxy=19531
# ============================================================
echo "=== Starting Cluster B (by-dev2) ==="

B_ENV="msgChannel_CHANNAMEPREFIX_CLUSTER=by-dev2 ETCD_ROOTPATH=by-dev2 MINIO_rootPath=by-dev2"
B_TLS="TLS_SERVERPEMPATH=${CERT_DIR}/cluster-b-server.pem TLS_SERVERKEYPATH=${CERT_DIR}/cluster-b-server.key"

env ${B_ENV} ${B_TLS} METRICS_PORT=19092 ./bin/milvus run mixture -rootcoord -querycoord -datacoord -indexcoord \
    1>>"${LOG}/b-mixcoord.stdout.log" 2>>"${LOG}/b-mixcoord.stderr.log" &

env ${B_ENV} ${B_TLS} PROXY_PORT=19531 PROXY_INTERNAL_PORT=19528 ROOTCOORD_PORT=53101 METRICS_PORT=19192 ./bin/milvus run proxy \
    1>>"${LOG}/b-proxy.stdout.log" 2>>"${LOG}/b-proxy.stderr.log" &

env ${B_ENV} ${B_TLS} METRICS_PORT=19292 ./bin/milvus run datanode \
    1>>"${LOG}/b-datanode.stdout.log" 2>>"${LOG}/b-datanode.stderr.log" &

env ${B_ENV} ${B_TLS} METRICS_PORT=19392 ./bin/milvus run indexnode \
    1>>"${LOG}/b-indexnode.stdout.log" 2>>"${LOG}/b-indexnode.stderr.log" &

env ${B_ENV} ${B_TLS} METRICS_PORT=19592 LOCALSTORAGE_PATH="${VOL}/b-sn1/data/" ./bin/milvus run streamingnode \
    1>>"${LOG}/b-sn1.stdout.log" 2>>"${LOG}/b-sn1.stderr.log" &

# CDC for cluster B
env ${B_ENV} ${B_TLS} PROXY_PORT=19531 PROXY_INTERNAL_PORT=19528 ROOTCOORD_PORT=53101 METRICS_PORT=29092 ./bin/milvus run cdc \
    >"${LOG}/b-cdc.stdout.log" 2>&1 &

echo "Cluster B started (6 processes)"

# ============================================================
# Cluster C (by-dev3): proxy=19532
# ============================================================
echo "=== Starting Cluster C (by-dev3) ==="

C_ENV="msgChannel_CHANNAMEPREFIX_CLUSTER=by-dev3 ETCD_ROOTPATH=by-dev3 MINIO_rootPath=by-dev3"
C_TLS="TLS_SERVERPEMPATH=${CERT_DIR}/cluster-c-server.pem TLS_SERVERKEYPATH=${CERT_DIR}/cluster-c-server.key"

env ${C_ENV} ${C_TLS} METRICS_PORT=19093 ./bin/milvus run mixture -rootcoord -querycoord -datacoord -indexcoord \
    1>>"${LOG}/c-mixcoord.stdout.log" 2>>"${LOG}/c-mixcoord.stderr.log" &

env ${C_ENV} ${C_TLS} PROXY_PORT=19532 PROXY_INTERNAL_PORT=19529 ROOTCOORD_PORT=53102 METRICS_PORT=19193 ./bin/milvus run proxy \
    1>>"${LOG}/c-proxy.stdout.log" 2>>"${LOG}/c-proxy.stderr.log" &

env ${C_ENV} ${C_TLS} METRICS_PORT=19293 ./bin/milvus run datanode \
    1>>"${LOG}/c-datanode.stdout.log" 2>>"${LOG}/c-datanode.stderr.log" &

env ${C_ENV} ${C_TLS} METRICS_PORT=19393 ./bin/milvus run indexnode \
    1>>"${LOG}/c-indexnode.stdout.log" 2>>"${LOG}/c-indexnode.stderr.log" &

env ${C_ENV} ${C_TLS} METRICS_PORT=19693 LOCALSTORAGE_PATH="${VOL}/c-sn1/data/" ./bin/milvus run streamingnode \
    1>>"${LOG}/c-sn1.stdout.log" 2>>"${LOG}/c-sn1.stderr.log" &

# CDC for cluster C
env ${C_ENV} ${C_TLS} PROXY_PORT=19532 PROXY_INTERNAL_PORT=19529 ROOTCOORD_PORT=53102 METRICS_PORT=29093 ./bin/milvus run cdc \
    >"${LOG}/c-cdc.stdout.log" 2>&1 &

echo "Cluster C started (6 processes)"

echo ""
echo "=== All 3 clusters started (mTLS enabled, tlsMode=2) ==="
echo "Cluster A (by-dev1): https://localhost:19530  metrics=19191"
echo "Cluster B (by-dev2): https://localhost:19531  metrics=19192"
echo "Cluster C (by-dev3): https://localhost:19532  metrics=19193"
echo "Certs: ${CERT_DIR}"
echo "Logs:  ${LOG}"
echo ""
echo "Wait ~30s for clusters to be ready, then health-check over mTLS:"
echo "  curl --cacert ${CERT_DIR}/ca.pem --cert ${CERT_DIR}/cluster-a-client.pem --key ${CERT_DIR}/cluster-a-client.key https://localhost:19530/healthz"
echo "  curl --cacert ${CERT_DIR}/ca.pem --cert ${CERT_DIR}/cluster-b-client.pem --key ${CERT_DIR}/cluster-b-client.key https://localhost:19531/healthz"
echo "  curl --cacert ${CERT_DIR}/ca.pem --cert ${CERT_DIR}/cluster-c-client.pem --key ${CERT_DIR}/cluster-c-client.key https://localhost:19532/healthz"
