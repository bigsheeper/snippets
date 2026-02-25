# CDC mTLS E2E Test Implementation Plan

**Goal:** Implement E2E test for CDC replication over mTLS with 3-cluster star topology and switchover

**Architecture:** 3 Milvus clusters (A, B, C) all with `tlsMode=2`, shared CA, per-cluster server/client certs, star topology A→B + A→C, then switchover B→A + B→C

**Tech Stack:** Bash (openssl, Milvus binary), Python (PyMilvus with TLS), loguru

---

### Task 1: Certificate Generation Script

**Files:**
- Create: `tests/test_cdc/test_mtls/gen_certs.sh`

**Step 1: Write gen_certs.sh**

```bash
#!/bin/bash
# Generate CA + 3 sets of server/client certs for mTLS testing.
# All certs share the same CA. SAN=localhost for local testing.
set -e

CERT_DIR="${1:-$(dirname "$0")/certs}"
DAYS=3650
mkdir -p "${CERT_DIR}"

# --- CA ---
openssl genrsa -out "${CERT_DIR}/ca.key" 4096
openssl req -new -x509 -key "${CERT_DIR}/ca.key" -out "${CERT_DIR}/ca.pem" \
  -days ${DAYS} -subj "/CN=MilvusTestCA"

# --- Per-cluster certs ---
for CLUSTER in cluster-a cluster-b cluster-c; do
  # Server cert
  openssl genrsa -out "${CERT_DIR}/${CLUSTER}-server.key" 2048
  openssl req -new -key "${CERT_DIR}/${CLUSTER}-server.key" \
    -out "${CERT_DIR}/${CLUSTER}-server.csr" -subj "/CN=${CLUSTER}-server"
  openssl x509 -req -in "${CERT_DIR}/${CLUSTER}-server.csr" \
    -CA "${CERT_DIR}/ca.pem" -CAkey "${CERT_DIR}/ca.key" -CAcreateserial \
    -out "${CERT_DIR}/${CLUSTER}-server.pem" -days ${DAYS} \
    -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1")

  # Client cert
  openssl genrsa -out "${CERT_DIR}/${CLUSTER}-client.key" 2048
  openssl req -new -key "${CERT_DIR}/${CLUSTER}-client.key" \
    -out "${CERT_DIR}/${CLUSTER}-client.csr" -subj "/CN=${CLUSTER}-client"
  openssl x509 -req -in "${CERT_DIR}/${CLUSTER}-client.csr" \
    -CA "${CERT_DIR}/ca.pem" -CAkey "${CERT_DIR}/ca.key" -CAcreateserial \
    -out "${CERT_DIR}/${CLUSTER}-client.pem" -days ${DAYS}

  rm -f "${CERT_DIR}/${CLUSTER}-server.csr" "${CERT_DIR}/${CLUSTER}-client.csr"
done

rm -f "${CERT_DIR}/ca.srl"
echo "Certificates generated in ${CERT_DIR}"
ls -la "${CERT_DIR}"
```

**Step 2: Verify script runs**

Run: `chmod +x tests/test_cdc/test_mtls/gen_certs.sh && ./tests/test_cdc/test_mtls/gen_certs.sh`
Expected: 13 files in certs/ (ca.pem, ca.key, 3×server.pem/key, 3×client.pem/key)

---

### Task 2: Start/Stop Clusters Script (3 Clusters with mTLS)

**Files:**
- Create: `tests/test_cdc/test_mtls/start_clusters.sh`
- Create: `tests/test_cdc/test_mtls/stop_clusters.sh`

**Step 1: Write start_clusters.sh**

Based on `test_replica_features/start_clusters.sh` pattern but with 3 clusters and TLS env vars.

```bash
#!/bin/bash
# Start 3 Milvus clusters with mTLS enabled.
# Cluster A (by-dev1): proxy=19530
# Cluster B (by-dev2): proxy=19531
# Cluster C (by-dev3): proxy=19532
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CERT_DIR="${SCRIPT_DIR}/certs"

# Generate certs if not present
if [[ ! -f "${CERT_DIR}/ca.pem" ]]; then
    echo "Generating certificates..."
    "${SCRIPT_DIR}/gen_certs.sh" "${CERT_DIR}"
fi

if [[ -z "${MILVUS_DEV_PATH}" ]]; then
    echo "MILVUS_DEV_PATH is not set"; exit 1
fi
if [[ -z "${MILVUS_VOLUME_DIRECTORY}" ]]; then
    echo "MILVUS_VOLUME_DIRECTORY is not set"; exit 1
fi

WORKSPACE_TAG=${WORKSPACE_TAG:-$(date +%F-%H-%M-%S)}
VOL="${MILVUS_VOLUME_DIRECTORY}/${WORKSPACE_TAG}"
LOG="${VOL}/milvus-logs"
mkdir -p "${LOG}"

cd "${MILVUS_DEV_PATH}"
source "./scripts/setenv.sh"

export LOG_LEVEL=debug
export MQ_TYPE=pulsar
export MILVUS_STREAMING_SERVICE_ENABLED=1

# TLS env vars (common to all clusters)
export COMMON_SECURITY_TLSMODE=2
export TLS_CAPEMPATH="${CERT_DIR}/ca.pem"

# --- Cluster A (by-dev1) ---
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
env ${A_ENV} ${A_TLS} PROXY_PORT=19530 METRICS_PORT=29091 ./bin/milvus run cdc \
    >"${LOG}/a-cdc.stdout.log" 2>&1 &
echo "Cluster A started (6 processes)"

# --- Cluster B (by-dev2) ---
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
env ${B_ENV} ${B_TLS} PROXY_PORT=19531 PROXY_INTERNAL_PORT=19528 ROOTCOORD_PORT=53101 METRICS_PORT=29092 ./bin/milvus run cdc \
    >"${LOG}/b-cdc.stdout.log" 2>&1 &
echo "Cluster B started (6 processes)"

# --- Cluster C (by-dev3) ---
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
env ${C_ENV} ${C_TLS} PROXY_PORT=19532 PROXY_INTERNAL_PORT=19529 ROOTCOORD_PORT=53102 METRICS_PORT=29093 ./bin/milvus run cdc \
    >"${LOG}/c-cdc.stdout.log" 2>&1 &
echo "Cluster C started (6 processes)"

echo ""
echo "=== All 3 clusters started (mTLS enabled) ==="
echo "Cluster A: https://localhost:19530  (by-dev1)"
echo "Cluster B: https://localhost:19531  (by-dev2)"
echo "Cluster C: https://localhost:19532  (by-dev3)"
echo "Certs: ${CERT_DIR}"
echo "Logs:  ${LOG}"
echo ""
echo "Wait ~30s, then health-check over mTLS:"
echo "  curl --cacert ${CERT_DIR}/ca.pem --cert ${CERT_DIR}/cluster-a-client.pem --key ${CERT_DIR}/cluster-a-client.key https://localhost:19530/healthz"
```

**Step 2: Write stop_clusters.sh**

```bash
#!/bin/bash
# Stop all 3 mTLS test clusters by killing Milvus processes.
set -e
echo "Stopping all milvus processes..."
pkill -f "milvus run" || true
sleep 2
echo "All clusters stopped."
```

**Step 3: Make executable and verify**

Run: `chmod +x tests/test_cdc/test_mtls/start_clusters.sh tests/test_cdc/test_mtls/stop_clusters.sh`

---

### Task 3: Python Test Utilities (common.py)

**Files:**
- Create: `tests/test_cdc/test_mtls/common.py`

**Step 1: Write common.py**

TLS-aware constants and client creation. PyMilvus `MilvusClient` supports `server_pem_path` for one-way TLS (CA verification). For test clients (not CDC), one-way TLS is sufficient since CDC handles mTLS internally.

```python
import os
import time
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
from pymilvus import MilvusClient

# --- Constants ---
TIMEOUT = 180
TOKEN = "root:Milvus"

CLUSTER_A_ID = "by-dev1"
CLUSTER_B_ID = "by-dev2"
CLUSTER_C_ID = "by-dev3"

CLUSTER_A_ADDR = "https://localhost:19530"
CLUSTER_B_ADDR = "https://localhost:19531"
CLUSTER_C_ADDR = "https://localhost:19532"

# Certificate paths
CERT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "certs")
CA_PEM = os.path.join(CERT_DIR, "ca.pem")

# Client cert paths (per cluster, for CDC connection_param)
CLUSTER_A_CLIENT_PEM = os.path.join(CERT_DIR, "cluster-a-client.pem")
CLUSTER_A_CLIENT_KEY = os.path.join(CERT_DIR, "cluster-a-client.key")
CLUSTER_B_CLIENT_PEM = os.path.join(CERT_DIR, "cluster-b-client.pem")
CLUSTER_B_CLIENT_KEY = os.path.join(CERT_DIR, "cluster-b-client.key")
CLUSTER_C_CLIENT_PEM = os.path.join(CERT_DIR, "cluster-c-client.pem")
CLUSTER_C_CLIENT_KEY = os.path.join(CERT_DIR, "cluster-c-client.key")

PCHANNEL_NUM = 16


def create_tls_client(uri):
    """Create a PyMilvus client with TLS CA verification."""
    return MilvusClient(uri=uri, token=TOKEN, server_pem_path=CA_PEM)


# Initialize clients
cluster_A_client = create_tls_client(CLUSTER_A_ADDR)
cluster_B_client = create_tls_client(CLUSTER_B_ADDR)
cluster_C_client = create_tls_client(CLUSTER_C_ADDR)


def reconnect_clients():
    """Reconnect all 3 clients after cluster restart."""
    global cluster_A_client, cluster_B_client, cluster_C_client
    for c in [cluster_A_client, cluster_B_client, cluster_C_client]:
        try:
            c.close()
        except Exception:
            pass
    cluster_A_client = create_tls_client(CLUSTER_A_ADDR)
    cluster_B_client = create_tls_client(CLUSTER_B_ADDR)
    cluster_C_client = create_tls_client(CLUSTER_C_ADDR)
    logger.info("Reconnected all 3 TLS clients")


def generate_pchannels(cluster_id, pchannel_num=PCHANNEL_NUM):
    return [f"{cluster_id}-rootcoord-dml_{i}" for i in range(pchannel_num)]


def build_replicate_config(source_id, target_ids, pchannel_num=PCHANNEL_NUM,
                           client_pem=None, client_key=None):
    """Build replicate config for star topology.

    Args:
        source_id: Source cluster ID
        target_ids: List of target cluster IDs
        pchannel_num: Number of pchannels per cluster
        client_pem: Client cert path for CDC outbound connections (source's cert)
        client_key: Client key path for CDC outbound connections (source's key)
    """
    cluster_info = {
        CLUSTER_A_ID: CLUSTER_A_ADDR,
        CLUSTER_B_ID: CLUSTER_B_ADDR,
        CLUSTER_C_ID: CLUSTER_C_ADDR,
    }

    clusters = []
    for cid, addr in cluster_info.items():
        conn_param = {"uri": addr, "token": TOKEN}
        # Add TLS cert paths for CDC connections
        if client_pem and client_key:
            conn_param["ca_pem_path"] = CA_PEM
            conn_param["client_pem_path"] = client_pem
            conn_param["client_key_path"] = client_key
        clusters.append({
            "cluster_id": cid,
            "connection_param": conn_param,
            "pchannels": generate_pchannels(cid, pchannel_num),
        })

    topology = [
        {"source_cluster_id": source_id, "target_cluster_id": tid}
        for tid in target_ids
    ]

    return {"clusters": clusters, "cross_cluster_topology": topology}


def update_replicate_config(source_id, target_ids, client_pem=None,
                            client_key=None, pchannel_num=PCHANNEL_NUM):
    """Update replicate config on all 3 clusters."""
    config = build_replicate_config(source_id, target_ids, pchannel_num,
                                    client_pem, client_key)
    clients = [cluster_A_client, cluster_B_client, cluster_C_client]
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(c.update_replicate_configuration, **config)
                   for c in clients]
        for f in futures:
            f.result()
    logger.info(f"Replicate config updated: {source_id} -> {target_ids}")
```

---

### Task 4: Collection Helpers (collection_helpers.py)

**Files:**
- Create: `tests/test_cdc/test_mtls/collection_helpers.py`

**Step 1: Write collection_helpers.py**

Adapted from `test_replica_features/collection_helpers.py` but supports multiple standby clusters.

```python
import random
import time
from loguru import logger
from pymilvus import MilvusClient, DataType
from pymilvus.client.types import LoadState
from common import TIMEOUT

DEFAULT_DIM = 4
VECTOR_FIELD_NAME = "vector"
PK_FIELD_NAME = "id"
INSERT_COUNT = 100


def create_collection_schema():
    schema = MilvusClient.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field(field_name=PK_FIELD_NAME, datatype=DataType.INT64, is_primary=True)
    schema.add_field(field_name=VECTOR_FIELD_NAME, datatype=DataType.FLOAT_VECTOR, dim=DEFAULT_DIM)
    return schema


def create_index_params(client):
    index_params = client.prepare_index_params()
    index_params.add_index(field_name=VECTOR_FIELD_NAME, index_type="AUTOINDEX", metric_type="COSINE")
    return index_params


def generate_data(n, start_id, dim=DEFAULT_DIM):
    return [
        {PK_FIELD_NAME: start_id + i, VECTOR_FIELD_NAME: [random.uniform(-1, 1) for _ in range(dim)]}
        for i in range(n)
    ]


def setup_collection(collection_name, primary_client, standby_clients):
    """Create collection, index, and load on primary. Wait for replication to all standbys."""
    schema = create_collection_schema()
    primary_client.create_collection(collection_name=collection_name, schema=schema, shards_num=1)
    logger.info(f"Collection created on primary: {collection_name}")
    for sc in standby_clients:
        wait_for_collection_created(collection_name, sc)

    index_params = create_index_params(primary_client)
    primary_client.create_index(collection_name, index_params=index_params)
    logger.info(f"Index created on primary: {collection_name}")
    for sc in standby_clients:
        wait_for_index_created(collection_name, sc)

    primary_client.load_collection(collection_name)
    logger.info(f"Collection loaded on primary: {collection_name}")
    for sc in standby_clients:
        wait_for_collection_loaded(collection_name, sc)


def insert_and_verify(collection_name, primary_client, standby_clients, start_id=1, count=INSERT_COUNT):
    """Insert data on primary and verify replication to all standbys."""
    data = generate_data(count, start_id=start_id)
    primary_client.insert(collection_name, data)
    logger.info(f"Inserted {count} rows on primary, start_id={start_id}")

    res_primary = primary_client.query(
        collection_name=collection_name,
        consistency_level="Strong",
        filter=f"{PK_FIELD_NAME} >= 0",
        output_fields=[PK_FIELD_NAME],
    )

    for sc in standby_clients:
        wait_for_query_consistent(collection_name, res_primary, sc)
    return start_id + count


def cleanup_collection(collection_name, primary_client, standby_clients):
    """Release and drop on primary, verify propagation to all standbys."""
    try:
        primary_client.release_collection(collection_name)
        for sc in standby_clients:
            wait_for_collection_released(collection_name, sc)
    except Exception as e:
        logger.warning(f"Release failed: {e}")

    primary_client.drop_collection(collection_name)
    logger.info(f"Collection dropped on primary: {collection_name}")
    for sc in standby_clients:
        wait_for_collection_dropped(collection_name, sc)


# ---- Wait helpers ----

def wait_for_collection_created(name, client):
    start = time.time()
    while True:
        if client.has_collection(name):
            break
        if time.time() - start > TIMEOUT:
            raise TimeoutError(f"Timeout: collection created on standby: {name}")
        time.sleep(1)
    logger.info(f"Collection created on standby: {name}")


def wait_for_index_created(name, client):
    start = time.time()
    while True:
        info = client.describe_index(name, index_name=VECTOR_FIELD_NAME)
        if info is not None:
            break
        if time.time() - start > TIMEOUT:
            raise TimeoutError(f"Timeout: index created on standby: {name}")
        time.sleep(1)
    logger.info(f"Index created on standby: {name}")


def wait_for_collection_loaded(name, client):
    start = time.time()
    while True:
        state = client.get_load_state(collection_name=name)
        if state["state"] == LoadState.Loaded:
            break
        if time.time() - start > TIMEOUT:
            raise TimeoutError(f"Timeout: collection loaded on standby: {name}")
        time.sleep(1)
    logger.info(f"Collection loaded on standby: {name}")


def wait_for_collection_released(name, client):
    start = time.time()
    while True:
        state = client.get_load_state(collection_name=name)
        if state["state"] == LoadState.NotLoad:
            break
        if time.time() - start > TIMEOUT:
            raise TimeoutError(f"Timeout: collection released on standby: {name}")
        time.sleep(1)
    logger.info(f"Collection released on standby: {name}")


def wait_for_collection_dropped(name, client):
    start = time.time()
    while True:
        if not client.has_collection(name):
            break
        if time.time() - start > TIMEOUT:
            raise TimeoutError(f"Timeout: collection dropped on standby: {name}")
        time.sleep(1)
    logger.info(f"Collection dropped on standby: {name}")


def wait_for_query_consistent(name, res_primary, client):
    start = time.time()
    while True:
        res = client.query(
            collection_name=name, consistency_level="Strong",
            filter=f"{PK_FIELD_NAME} >= 0", output_fields=[PK_FIELD_NAME],
        )
        if len(res) == len(res_primary):
            ids_p = sorted([r[PK_FIELD_NAME] for r in res_primary])
            ids_s = sorted([r[PK_FIELD_NAME] for r in res])
            if ids_p == ids_s:
                break
        else:
            logger.warning(f"Count mismatch: primary={len(res_primary)}, standby={len(res)}")
        if time.time() - start > TIMEOUT:
            raise TimeoutError(f"Timeout: query consistency on standby: {name}")
        time.sleep(1)
    logger.info(f"Query consistent on standby: {name}, count={len(res_primary)}")
```

---

### Task 5: mTLS Replication Test (Phase 2-3)

**Files:**
- Create: `tests/test_cdc/test_mtls/test_mtls_replication.py`

**Step 1: Write test_mtls_replication.py**

```python
"""
CDC mTLS E2E Test - Phase 2 & 3: Configure and verify replication over mTLS.

Topology: A → B, A → C (star with A as center)
All clusters run with tlsMode=2 (mTLS).
CDC uses cluster-a-client cert to connect to B and C.
"""
import traceback
from loguru import logger
from common import (
    CLUSTER_A_ID, CLUSTER_B_ID, CLUSTER_C_ID,
    CLUSTER_A_CLIENT_PEM, CLUSTER_A_CLIENT_KEY,
    cluster_A_client, cluster_B_client, cluster_C_client,
    update_replicate_config,
)
from collection_helpers import (
    setup_collection, insert_and_verify, cleanup_collection,
)

COLLECTION_NAME = "test_mtls_replication"


def main():
    logger.info("=== Phase 2: Configure CDC replication A → B, A → C ===")
    update_replicate_config(
        source_id=CLUSTER_A_ID,
        target_ids=[CLUSTER_B_ID, CLUSTER_C_ID],
        client_pem=CLUSTER_A_CLIENT_PEM,
        client_key=CLUSTER_A_CLIENT_KEY,
    )
    logger.info("Replicate config applied to all 3 clusters")

    logger.info("=== Phase 3: Verify replication over mTLS ===")

    # Step 1: Create collection on A, wait for B and C
    standbys = [cluster_B_client, cluster_C_client]
    setup_collection(COLLECTION_NAME, cluster_A_client, standbys)

    # Step 2: Insert 1000 rows on A, verify on B and C
    next_id = insert_and_verify(COLLECTION_NAME, cluster_A_client, standbys,
                                start_id=1, count=1000)

    # Step 3: Insert 1000 more rows, verify total 2000
    insert_and_verify(COLLECTION_NAME, cluster_A_client, standbys,
                      start_id=next_id, count=1000)

    # Step 4: Cleanup
    cleanup_collection(COLLECTION_NAME, cluster_A_client, standbys)

    logger.info("=== Phase 3 PASSED: mTLS replication verified ===")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.error(traceback.format_exc())
        raise
```

---

### Task 6: mTLS Switchover Test (Phase 4)

**Files:**
- Create: `tests/test_cdc/test_mtls/test_mtls_switchover.py`

**Step 1: Write test_mtls_switchover.py**

```python
"""
CDC mTLS E2E Test - Phase 4: Switchover (B becomes primary).

After Phase 3 completes:
1. Reconfigure topology: B → A, B → C
2. CDC now uses cluster-b-client cert for outbound connections
3. Create collection on B, verify replication to A and C
"""
import traceback
from loguru import logger
from common import (
    CLUSTER_A_ID, CLUSTER_B_ID, CLUSTER_C_ID,
    CLUSTER_B_CLIENT_PEM, CLUSTER_B_CLIENT_KEY,
    cluster_A_client, cluster_B_client, cluster_C_client,
    update_replicate_config,
)
from collection_helpers import (
    setup_collection, insert_and_verify, cleanup_collection,
)

COLLECTION_NAME = "test_mtls_switchover"


def main():
    logger.info("=== Phase 4: Switchover — B becomes primary ===")
    update_replicate_config(
        source_id=CLUSTER_B_ID,
        target_ids=[CLUSTER_A_ID, CLUSTER_C_ID],
        client_pem=CLUSTER_B_CLIENT_PEM,
        client_key=CLUSTER_B_CLIENT_KEY,
    )
    logger.info("Replicate config updated: B → A, B → C (using B's client cert)")

    # B is now primary, A and C are standbys
    standbys = [cluster_A_client, cluster_C_client]
    setup_collection(COLLECTION_NAME, cluster_B_client, standbys)

    next_id = insert_and_verify(COLLECTION_NAME, cluster_B_client, standbys,
                                start_id=1, count=1000)

    insert_and_verify(COLLECTION_NAME, cluster_B_client, standbys,
                      start_id=next_id, count=1000)

    cleanup_collection(COLLECTION_NAME, cluster_B_client, standbys)

    logger.info("=== Phase 4 PASSED: Switchover with B's client cert verified ===")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.error(traceback.format_exc())
        raise
```

---

### Task 7: README

**Files:**
- Create: `tests/test_cdc/test_mtls/README.md`

**Step 1: Write README with usage instructions**

Quick start guide covering:
- Prerequisites (Milvus binary with mTLS CDC support, openssl, PyMilvus)
- Run flow: gen_certs → start_clusters → test_mtls_replication → test_mtls_switchover → stop_clusters
- Port reference and cert layout

---

### Execution Order

1. Task 1: gen_certs.sh
2. Task 2: start/stop scripts
3. Task 3: common.py
4. Task 4: collection_helpers.py
5. Task 5: test_mtls_replication.py
6. Task 6: test_mtls_switchover.py
7. Task 7: README.md
