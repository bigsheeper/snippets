# CDC Replica Features Stress Test Design

**Date:** 2026-02-18
**Author:** bigsheeper
**Status:** Draft

## Background

Milvus CDC (Change Data Capture) replication supports a primary-standby architecture where data and DDL operations are replicated from a source cluster to a target cluster. Three new features have been added to enhance the replication system:

1. **Pchannel count increase** (`config_validator.go`) — Allow appending new pchannels to an existing `ReplicateConfiguration` (append-only, no removal or reorder). This supports cluster scale-up without breaking replication.

2. **UseLocalReplicaConfig** (`replicate_service.go`, `job_load.go`) — When replicating `AlterLoadConfig` messages, the secondary cluster uses its own local cluster-level replica/resource-group config instead of blindly applying the primary's config. This allows heterogeneous replica configurations across clusters.

3. **Configurable skip message types** (`replicate_service.go`, `component_param.go`) — A configurable list of message types (`streaming.replication.skipMessageTypes`) to skip during replication. Default: `AlterResourceGroup,DropResourceGroup`.

This test focuses on **pchannel increase** and **replica heterogeneity** in a long-running stress scenario with frequent switchovers.

## Goals

- Validate that pchannel count can be increased (appended) in replicate config without breaking replication.
- Validate that the standby cluster uses its own local replica config when processing replicated load operations (replica heterogeneity).
- Validate that switchover (toggling primary-standby roles) works reliably under varying pchannel counts and replica configurations.
- Run thousands of cycles to catch race conditions, state leaks, or config drift.

## Non-Goals

- Testing skip message types (out of scope for this stress test).
- Testing pchannel decrease or reorder (not supported, validated by `config_validator.go`).
- Performance benchmarking or latency measurement.
- Multi-node cluster mode (test uses standalone mode).

## Test Environment

### Dual-Cluster Setup

Two Milvus standalone instances sharing infrastructure (Pulsar, etcd, minio):

| Cluster | ID | Proxy Port | etcd Root | Channel Prefix | Metrics Port |
|---|---|---|---|---|---|
| A (primary) | `by-dev1` | 19530 | `by-dev1` | `by-dev1` | 19091 |
| B (standby) | `by-dev2` | 19531 | `by-dev2` | `by-dev2` | 19092 |

Each cluster also runs a CDC process for replication.

### Startup

```bash
# Start infrastructure (Pulsar + etcd + minio)
milvus_control -s start_milvus_inf

# Start both Milvus clusters with streaming enabled
# Cluster B has local replica config = 2
QUERYCOORD_CLUSTERLEVELLOADREPLICANUMBER=2 milvus_control -m -s start_milvus
```

Note: Cluster B is started with `QUERYCOORD_CLUSTERLEVELLOADREPLICANUMBER=2` so that when it receives replicated `AlterLoadConfig` messages, it uses 2 replicas instead of the primary's value.

## Test Design

### Stress Loop

The test runs a configurable number of cycles (default: 1000) with three interleaved operations:

```
cycle N (N=0..max_cycles):
  1. Pchannel increase:  every 100 cycles (2 -> 3 -> 4 -> ...)
  2. Switchover:         every 10 cycles  (toggle A->B / B->A)
  3. Replica variation:  every cycle       (replica_count = (N % 3) + 1)
  4. Create collection, load with replica_count, insert data
  5. Verify data replicated to standby
  6. Verify standby replica count (should reflect local config, not primary's)
  7. Drop collection, log cycle result
```

### Event Timeline (first 200 cycles)

```
Cycle   0: Init A->B, pchannels=2, replicas=1
Cycle   1: replicas=2
Cycle   2: replicas=3
...
Cycle   9: replicas=1
Cycle  10: Switchover B->A, replicas=2
Cycle  20: Switchover A->B, replicas=3
...
Cycle 100: Pchannel increase -> 3, Switchover B->A, replicas=2
Cycle 110: Switchover A->B, replicas=3
...
Cycle 200: Pchannel increase -> 4, Switchover B->A, replicas=3
```

After 1000 cycles: pchannels grow from 2 to 12, switchover happens 99 times, replicas cycle through 1/2/3.

### Per-Cycle Operations

1. **Config update** (if triggered):
   - Build replicate config with current topology and pchannel count.
   - Update standby first, then primary (ordering matters for safety).

2. **Collection lifecycle**:
   - Create collection with schema: `id` (INT64 PK) + `vector` (FLOAT_VECTOR, dim=4).
   - Create AUTOINDEX on vector field.
   - Load on primary with `replica_number` = (cycle % 3) + 1.
   - Wait for collection loaded on standby (poll `get_load_state`).

3. **Data verification**:
   - Insert 100 rows on primary.
   - Query primary for all rows.
   - Poll standby until query result matches primary (same IDs).

4. **Replica verification**:
   - Call `describe_replica()` on both clusters.
   - Primary should have `replica_count` replicas.
   - Standby should have its local config replica count (e.g., 2 if `QUERYCOORD_CLUSTERLEVELLOADREPLICANUMBER=2`).
   - Log mismatch/match for analysis.

5. **Cleanup**:
   - Release and drop collection on primary.
   - Wait for drop replicated to standby.

### Replica Heterogeneity Cases

| Cycle (N % 3) | Primary replicas | Standby replicas (if local=2) | Heterogeneous? |
|---|---|---|---|
| 0 | 1 | 2 | Yes |
| 1 | 2 | 2 | No (coincidence) |
| 2 | 3 | 2 | Yes |

This covers both cases requested:
- **Standby has richer config**: N % 3 == 0, primary=1, standby=2.
- **Primary has more replicas**: N % 3 == 2, primary=3, standby=2 (or standby defaults to 1 if no local config).

### Error Handling

- Each cycle is wrapped in try/except.
- On failure: log error with traceback, attempt cleanup, continue to next cycle.
- Final summary reports success/fail counts.
- Test raises `RuntimeError` if any cycle failed.

## File Structure

```
snippets/tests/test_cdc/test_replica_features/
├── common.py                # Constants, clients, replicate config helpers
├── collection_helpers.py    # Collection CRUD, wait helpers, replica count
└── test_stress.py           # Main stress loop with argparse CLI
```

### common.py

- Cluster addresses, IDs, auth tokens.
- `generate_pchannels(cluster_id, count)` — Generate pchannel names.
- `build_replicate_config(source, target, pchannel_num)` — Build config dict.
- `update_replicate_config(source, target, pchannel_num)` — Apply to both clusters.
- `get_primary_and_standby(source_id)` — Return client pair based on current topology.

### collection_helpers.py

- Schema creation (INT64 PK + FLOAT_VECTOR dim=4).
- `setup_collection()` — Create, index, load on primary; wait for standby.
- `insert_and_verify()` — Insert on primary, poll standby for consistency.
- `cleanup_collection()` — Release, drop on primary; wait for standby.
- `get_replica_count()` — Via `describe_replica()`, returns replica count.
- `wait_for_*()` — Polling helpers with configurable timeout (180s).

### test_stress.py

- CLI: `python test_stress.py [--cycles 1000] [--duration 3600]`
- Configurable intervals: `SWITCHOVER_INTERVAL=10`, `PCHANNEL_INCREASE_INTERVAL=100`, `INIT_PCHANNEL_NUM=2`.
- Per-cycle logging with cycle number, elapsed time, success/fail status.

## Usage

```bash
# Quick smoke test (10 cycles)
python test_stress.py --cycles 10

# Full stress test (1000 cycles)
python test_stress.py --cycles 1000

# Time-bounded run (1 hour max)
python test_stress.py --cycles 10000 --duration 3600
```

## Dependencies

- Python 3.8+
- `pymilvus` (matching Milvus server version)
- `loguru` for structured logging
