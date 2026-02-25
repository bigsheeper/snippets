# CDC mTLS E2E Test

Verifies CDC replication works over mTLS-secured connections with 3-cluster star topology and switchover.

## Architecture

```
Cluster A (by-dev1, :19530) ──mTLS──▶ Cluster B (by-dev2, :19531)
         primary              └──mTLS──▶ Cluster C (by-dev3, :19532)
```

All 3 clusters run with `common.security.tlsMode=2` (mutual TLS). Each cluster has its own server cert and client cert, all signed by a shared CA.

## Prerequisites

- Milvus binary built with mTLS CDC support (PR #47844)
- `openssl` installed
- PyMilvus with TLS support (`pip install pymilvus`)
- `loguru` (`pip install loguru`)
- Environment variables:
  - `MILVUS_DEV_PATH` — path to Milvus source (with `bin/milvus`)
  - `MILVUS_VOLUME_DIRECTORY` — path for volume storage

## Quick Start

```bash
# 1. Generate certificates (creates ./certs/ with CA + 3 cluster cert pairs)
./gen_certs.sh

# 2. Start 3 clusters with mTLS
./start_clusters.sh

# 3. Wait ~30s for clusters to be ready, then health-check
curl --cacert certs/ca.pem --cert certs/cluster-a-client.pem --key certs/cluster-a-client.key https://localhost:19530/healthz

# 4. Run Phase 2-3: Configure replication A→B,C and verify
python test_mtls_replication.py

# 5. Run Phase 4: Switchover — B becomes primary
python test_mtls_switchover.py

# 6. Stop all clusters
./stop_clusters.sh
```

## Test Phases

| Phase | Script | What it tests |
|-------|--------|---------------|
| 1 | `gen_certs.sh` + `start_clusters.sh` | Certificate generation + 3-cluster mTLS startup |
| 2-3 | `test_mtls_replication.py` | CDC replication A→B, A→C over mTLS |
| 4 | `test_mtls_switchover.py` | Switchover: B→A, B→C with B's client cert |

## File Structure

```
test_mtls/
├── gen_certs.sh              # Generate CA + per-cluster server/client certs
├── start_clusters.sh         # Start 3 clusters with mTLS (tlsMode=2)
├── stop_clusters.sh          # Kill all Milvus processes
├── common.py                 # Constants, TLS-aware client creation, config helpers
├── collection_helpers.py     # Collection CRUD and wait helpers (multi-standby)
├── test_mtls_replication.py  # Phase 2-3: replication over mTLS
├── test_mtls_switchover.py   # Phase 4: switchover with different client cert
├── README.md                 # This file
└── certs/                    # Generated certificates (gitignored)
    ├── ca.pem / ca.key
    ├── cluster-a-server.pem / .key
    ├── cluster-a-client.pem / .key
    ├── cluster-b-server.pem / .key
    ├── cluster-b-client.pem / .key
    ├── cluster-c-server.pem / .key
    └── cluster-c-client.pem / .key
```

## Ports Reference

| Cluster | Proxy (gRPC+TLS) | Metrics | CDC Metrics |
|---------|-------------------|---------|-------------|
| A (by-dev1) | 19530 | 19191 | 29091 |
| B (by-dev2) | 19531 | 19192 | 29092 |
| C (by-dev3) | 19532 | 19193 | 29093 |

## Certificate Layout

- **Shared CA**: All certs signed by the same CA (`ca.pem`)
- **Server certs**: Each cluster's proxy presents its own server cert (SAN: localhost, 127.0.0.1)
- **Client certs**: CDC uses the source cluster's client cert for outbound mTLS connections
  - Phase 2-3: A's client cert authenticates CDC connections to B and C
  - Phase 4: B's client cert authenticates CDC connections to A and C
