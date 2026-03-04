#!/bin/bash
# Start 3 Milvus clusters with mTLS enabled (tlsMode=2).
#
# Cluster A (by-dev1): proxy=19530, metrics=19191
# Cluster B (by-dev2): proxy=19531, metrics=19192
# Cluster C (by-dev3): proxy=19532, metrics=19193
#
# Each cluster has its own CA, server cert, and client certs.
# CDC reads per-cluster client certs from paramtable via
# tls.clusters.<clusterID>.{caPemPath,clientPemPath,clientKeyPath}.
# Since cluster IDs contain dashes (by-dev1, etc.), these MUST be
# configured in milvus.yaml (env vars can't represent dashes in keys).
# We inject the config via MILVUSCONF pointing to a temp config dir.
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
if [[ ! -f "${CERT_DIR}/ca-dev1.pem" ]]; then
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

MILVUS_DEV_PATH="/home/sheep/workspace/milvus/.worktrees/cdc-per-cluster-tls"

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

# TLS mode (shared by all clusters — server-side per-cluster certs in A_ENV/B_ENV/C_ENV)
export COMMON_SECURITY_TLSMODE=2

# Per-cluster outbound TLS for CDC.
# Cluster IDs contain dashes (by-dev1, by-dev2, by-dev3) which can't be
# represented in env var names, so we inject the config into milvus.yaml
# via MILVUSCONF pointing to a temp config dir.
CDC_CONF_DIR="${VOL}/milvus-conf"
mkdir -p "${CDC_CONF_DIR}"
cp -r "${MILVUS_DEV_PATH}/configs/"* "${CDC_CONF_DIR}/" 2>/dev/null || true

python3 -c "
import re, sys

yaml_path = '${CDC_CONF_DIR}/milvus.yaml'
cert_dir = '${CERT_DIR}'

with open(yaml_path) as f:
    content = f.read()

clusters_block = '''  clusters:
    by-dev1:
      caPemPath: {cd}/ca-dev1.pem
      clientPemPath: {cd}/client-dev1.pem
      clientKeyPath: {cd}/client-dev1.key
    by-dev2:
      caPemPath: {cd}/ca-dev2.pem
      clientPemPath: {cd}/client-dev2.pem
      clientKeyPath: {cd}/client-dev2.key
    by-dev3:
      caPemPath: {cd}/ca-dev3.pem
      clientPemPath: {cd}/client-dev3.pem
      clientKeyPath: {cd}/client-dev3.key'''.format(cd=cert_dir)

# Remove any existing 'clusters:' block under tls: (from previous injection)
content = re.sub(r'\n  clusters:\n(?:    .*\n)*', '\n', content)

# Insert clusters block after the EXTERNAL tls: section's caPemPath line
# (match '# Configure external tls.' followed by 'tls:' block)
content = re.sub(
    r'(# Configure external tls\.\ntls:\n  serverPemPath:.*\n  serverKeyPath:.*\n  caPemPath:.*)',
    r'\1\n' + clusters_block,
    content,
)

with open(yaml_path, 'w') as f:
    f.write(content)

print(f'Per-cluster TLS config injected into {yaml_path}')
"

export MILVUSCONF="${CDC_CONF_DIR}"
echo "MILVUSCONF: ${CDC_CONF_DIR}"

# ============================================================
# Cluster A (by-dev1): proxy=19530
# ============================================================
echo "=== Starting Cluster A (by-dev1) ==="

A_ENV="msgChannel_CHANNAMEPREFIX_CLUSTER=by-dev1 ETCD_ROOTPATH=by-dev1 MINIO_rootPath=by-dev1 TLS_CAPEMPATH=${CERT_DIR}/ca-dev1.pem TLS_SERVERPEMPATH=${CERT_DIR}/server-dev1.pem TLS_SERVERKEYPATH=${CERT_DIR}/server-dev1.key"

env ${A_ENV} METRICS_PORT=19091 ./bin/milvus run mixture -rootcoord -querycoord -datacoord -indexcoord \
    1>>"${LOG}/a-mixcoord.stdout.log" 2>>"${LOG}/a-mixcoord.stderr.log" &

env ${A_ENV} PROXY_PORT=19530 PROXY_HTTP_PORT=18530 PROXY_INTERNAL_PORT=19527 METRICS_PORT=19191 ./bin/milvus run proxy \
    1>>"${LOG}/a-proxy.stdout.log" 2>>"${LOG}/a-proxy.stderr.log" &

env ${A_ENV} METRICS_PORT=19291 ./bin/milvus run datanode \
    1>>"${LOG}/a-datanode.stdout.log" 2>>"${LOG}/a-datanode.stderr.log" &

env ${A_ENV} METRICS_PORT=19391 LOCALSTORAGE_PATH="${VOL}/a-qn1/data/" ./bin/milvus run querynode \
    1>>"${LOG}/a-querynode.stdout.log" 2>>"${LOG}/a-querynode.stderr.log" &

env ${A_ENV} METRICS_PORT=19491 LOCALSTORAGE_PATH="${VOL}/a-sn1/data/" ./bin/milvus run streamingnode \
    1>>"${LOG}/a-sn1.stdout.log" 2>>"${LOG}/a-sn1.stderr.log" &

# CDC for cluster A
env ${A_ENV} PROXY_PORT=19530 PROXY_INTERNAL_PORT=19527 METRICS_PORT=29091 ./bin/milvus run cdc \
    >"${LOG}/a-cdc.stdout.log" 2>&1 &

echo "Cluster A started (6 processes)"

# ============================================================
# Cluster B (by-dev2): proxy=19531
# ============================================================
echo "=== Starting Cluster B (by-dev2) ==="

B_ENV="msgChannel_CHANNAMEPREFIX_CLUSTER=by-dev2 ETCD_ROOTPATH=by-dev2 MINIO_rootPath=by-dev2 TLS_CAPEMPATH=${CERT_DIR}/ca-dev2.pem TLS_SERVERPEMPATH=${CERT_DIR}/server-dev2.pem TLS_SERVERKEYPATH=${CERT_DIR}/server-dev2.key"

env ${B_ENV} ROOTCOORD_PORT=53101 METRICS_PORT=19092 ./bin/milvus run mixture -rootcoord -querycoord -datacoord -indexcoord \
    1>>"${LOG}/b-mixcoord.stdout.log" 2>>"${LOG}/b-mixcoord.stderr.log" &

env ${B_ENV} PROXY_PORT=19531 PROXY_HTTP_PORT=18531 PROXY_INTERNAL_PORT=19528 ROOTCOORD_PORT=53101 METRICS_PORT=19192 ./bin/milvus run proxy \
    1>>"${LOG}/b-proxy.stdout.log" 2>>"${LOG}/b-proxy.stderr.log" &

env ${B_ENV} METRICS_PORT=19292 ./bin/milvus run datanode \
    1>>"${LOG}/b-datanode.stdout.log" 2>>"${LOG}/b-datanode.stderr.log" &

env ${B_ENV} METRICS_PORT=19392 LOCALSTORAGE_PATH="${VOL}/b-qn1/data/" ./bin/milvus run querynode \
    1>>"${LOG}/b-querynode.stdout.log" 2>>"${LOG}/b-querynode.stderr.log" &

env ${B_ENV} METRICS_PORT=19592 LOCALSTORAGE_PATH="${VOL}/b-sn1/data/" ./bin/milvus run streamingnode \
    1>>"${LOG}/b-sn1.stdout.log" 2>>"${LOG}/b-sn1.stderr.log" &

# CDC for cluster B
env ${B_ENV} PROXY_PORT=19531 PROXY_INTERNAL_PORT=19528 ROOTCOORD_PORT=53101 METRICS_PORT=29092 ./bin/milvus run cdc \
    >"${LOG}/b-cdc.stdout.log" 2>&1 &

echo "Cluster B started (6 processes)"

# ============================================================
# Cluster C (by-dev3): proxy=19532
# ============================================================
echo "=== Starting Cluster C (by-dev3) ==="

C_ENV="msgChannel_CHANNAMEPREFIX_CLUSTER=by-dev3 ETCD_ROOTPATH=by-dev3 MINIO_rootPath=by-dev3 TLS_CAPEMPATH=${CERT_DIR}/ca-dev3.pem TLS_SERVERPEMPATH=${CERT_DIR}/server-dev3.pem TLS_SERVERKEYPATH=${CERT_DIR}/server-dev3.key"

env ${C_ENV} ROOTCOORD_PORT=53102 METRICS_PORT=19093 ./bin/milvus run mixture -rootcoord -querycoord -datacoord -indexcoord \
    1>>"${LOG}/c-mixcoord.stdout.log" 2>>"${LOG}/c-mixcoord.stderr.log" &

env ${C_ENV} PROXY_PORT=19532 PROXY_HTTP_PORT=18532 PROXY_INTERNAL_PORT=19529 ROOTCOORD_PORT=53102 METRICS_PORT=19193 ./bin/milvus run proxy \
    1>>"${LOG}/c-proxy.stdout.log" 2>>"${LOG}/c-proxy.stderr.log" &

env ${C_ENV} METRICS_PORT=19293 ./bin/milvus run datanode \
    1>>"${LOG}/c-datanode.stdout.log" 2>>"${LOG}/c-datanode.stderr.log" &

env ${C_ENV} METRICS_PORT=19393 LOCALSTORAGE_PATH="${VOL}/c-qn1/data/" ./bin/milvus run querynode \
    1>>"${LOG}/c-querynode.stdout.log" 2>>"${LOG}/c-querynode.stderr.log" &

env ${C_ENV} METRICS_PORT=19693 LOCALSTORAGE_PATH="${VOL}/c-sn1/data/" ./bin/milvus run streamingnode \
    1>>"${LOG}/c-sn1.stdout.log" 2>>"${LOG}/c-sn1.stderr.log" &

# CDC for cluster C
env ${C_ENV} PROXY_PORT=19532 PROXY_INTERNAL_PORT=19529 ROOTCOORD_PORT=53102 METRICS_PORT=29093 ./bin/milvus run cdc \
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
echo "  curl --cacert ${CERT_DIR}/ca-dev1.pem --cert ${CERT_DIR}/pymilvus-dev1.pem --key ${CERT_DIR}/pymilvus-dev1.key https://localhost:19530/healthz"
