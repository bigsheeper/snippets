# CDC mTLS E2E Test Design

**Date:** 2026-02-25
**Author:** bigsheeper
**Status:** Draft

## Background

CDC now supports mTLS connections to target clusters via `ca_pem_path`, `client_pem_path`, `client_key_path` fields in `ConnectionParam`. We need an E2E test to verify CDC can replicate data over mTLS-secured connections, including switchover scenarios with multiple secondary clusters.

## Goals

- Verify CDC replication works with mTLS enabled on all clusters
- Test star topology: 1 primary → 2 secondaries, all over mTLS
- Test switchover: secondary becomes primary and replicates using its own client certs
- Test per-cluster client certificate configuration

## Non-Goals

- Negative test cases (wrong certs, expired certs) — can be added later
- Certificate rotation during replication
- Internal TLS between Milvus components (already tested separately)

## Test Architecture

### 3-Cluster Star Topology

```
Cluster A (by-dev1, :19530) ──mTLS──▶ Cluster B (by-dev2, :19531)
         primary              └──mTLS──▶ Cluster C (by-dev3, :19532)
```

All 3 clusters run with `common.security.tlsMode=2` (mTLS). Each cluster's proxy serves gRPC over TLS. CDC presents client certs via `connection_param` fields.

### Certificate Layout

```
certs/
├── ca.pem                  # Shared CA cert (trusted by all clusters)
├── ca.key                  # CA private key (used only for signing)
├── cluster-a-server.pem    # Cluster A proxy server cert (SAN: localhost)
├── cluster-a-server.key
├── cluster-a-client.pem    # Cluster A CDC client cert
├── cluster-a-client.key
├── cluster-b-server.pem    # Cluster B proxy server cert
├── cluster-b-server.key
├── cluster-b-client.pem    # Cluster B CDC client cert
├── cluster-b-client.key
├── cluster-c-server.pem    # Cluster C proxy server cert
├── cluster-c-server.key
├── cluster-c-client.pem    # Cluster C CDC client cert
└── cluster-c-client.key
```

Each cluster has:
- **Server cert** — proxy presents this to incoming connections
- **Client cert** — CDC presents this when connecting to other clusters

All certs share the same CA. SAN = `localhost` for local testing.

### Cluster Startup Config

Each cluster starts with these TLS env vars:

```bash
# Example for Cluster A
COMMON_SECURITY_TLSMODE=2
TLS_SERVERPEMPATH=/path/to/certs/cluster-a-server.pem
TLS_SERVERKEYPATH=/path/to/certs/cluster-a-server.key
TLS_CAPEMPATH=/path/to/certs/ca.pem
```

Ports:
- Cluster A: proxy=19530, metrics=19191
- Cluster B: proxy=19531, metrics=19192
- Cluster C: proxy=19532, metrics=19193

## Test Flow

### Phase 1: Setup

1. Run `gen_certs.sh` to generate all certificates
2. Run `start_clusters.sh` to start 3 clusters with mTLS
3. Wait for all proxies healthy (health check over mTLS)

### Phase 2: Configure CDC Replication (A → B, A → C)

```python
config = {
    "clusters": [
        {
            "cluster_id": "by-dev1",
            "connection_param": {
                "uri": "https://localhost:19530",
                "token": "root:Milvus",
                "ca_pem_path": "<certs>/ca.pem",
                "client_pem_path": "<certs>/cluster-a-client.pem",
                "client_key_path": "<certs>/cluster-a-client.key"
            },
            "pchannels": ["by-dev1-rootcoord-dml_0", ..., "by-dev1-rootcoord-dml_15"]
        },
        {
            "cluster_id": "by-dev2",
            "connection_param": {
                "uri": "https://localhost:19531",
                "token": "root:Milvus",
                "ca_pem_path": "<certs>/ca.pem",
                "client_pem_path": "<certs>/cluster-a-client.pem",
                "client_key_path": "<certs>/cluster-a-client.key"
            },
            "pchannels": ["by-dev2-rootcoord-dml_0", ..., "by-dev2-rootcoord-dml_15"]
        },
        {
            "cluster_id": "by-dev3",
            "connection_param": {
                "uri": "https://localhost:19532",
                "token": "root:Milvus",
                "ca_pem_path": "<certs>/ca.pem",
                "client_pem_path": "<certs>/cluster-a-client.pem",
                "client_key_path": "<certs>/cluster-a-client.key"
            },
            "pchannels": ["by-dev3-rootcoord-dml_0", ..., "by-dev3-rootcoord-dml_15"]
        }
    ],
    "cross_cluster_topology": [
        {"source_cluster_id": "by-dev1", "target_cluster_id": "by-dev2"},
        {"source_cluster_id": "by-dev1", "target_cluster_id": "by-dev3"}
    ]
}
```

Apply config to all 3 clusters via `update_replicate_configuration`.

### Phase 3: Verify Replication over mTLS

1. Create collection on A (INT64 PK + FLOAT_VECTOR)
2. Create index on A (AUTOINDEX + COSINE)
3. Load collection on A
4. Insert 1000 rows on A
5. Wait for B to replicate (poll query until row count = 1000, timeout 180s)
6. Wait for C to replicate (same poll)
7. Insert 1000 more rows on A
8. Verify B and C both reach 2000 rows
9. Drop collection on A
10. Verify collection dropped on B and C

### Phase 4: Switchover (B becomes primary)

1. Update replicate config: B → A, B → C
   - Config now uses B's client cert (`cluster-b-client.pem`) for outbound CDC connections
2. Create new collection on B
3. Insert 1000 rows on B
4. Wait for A to replicate (verify over mTLS using A's server cert)
5. Wait for C to replicate
6. Verify row counts match across all 3 clusters
7. Cleanup: drop collection on B, verify propagation

## File Structure

```
snippets/tests/test_cdc/test_mtls/
├── gen_certs.sh              # Certificate generation script
├── start_clusters.sh         # Start 3 clusters with mTLS
├── stop_clusters.sh          # Cleanup
├── common.py                 # Constants, TLS-aware client creation
├── update_config.py          # Replicate config with cert paths
├── test_mtls_replication.py  # Phase 3: verify replication
└── test_mtls_switchover.py   # Phase 4: switchover with cert rotation
```

## Test Utilities

### TLS-aware MilvusClient

PyMilvus `MilvusClient` needs to connect over TLS. The client supports `server_pem_path` and `ca_pem_path` parameters:

```python
from pymilvus import MilvusClient

client = MilvusClient(
    uri="https://localhost:19530",
    token="root:Milvus",
    server_pem_path="certs/cluster-a-server.pem",  # or use ca_pem_path
)
```

Note: PyMilvus TLS support needs to be verified — if it doesn't support mTLS client certs, the test clients can use one-way TLS (just CA verification) since the test scripts themselves are not CDC.

### Health Check

```bash
# mTLS health check
curl --cacert certs/ca.pem \
     --cert certs/cluster-a-client.pem \
     --key certs/cluster-a-client.key \
     https://localhost:19530/healthz
```

## Dependencies

- Milvus built with mTLS CDC support (PR #47844)
- Proto with ConnectionParam TLS fields (PR milvus-io/milvus-proto#570)
- `openssl` for certificate generation
- PyMilvus with TLS support

## Success Criteria

1. CDC successfully replicates data from A to B and C over mTLS
2. Data consistency verified: row counts and IDs match after replication
3. Switchover works: B becomes primary, replicates to A and C using B's client cert
4. No TLS handshake errors in CDC logs
