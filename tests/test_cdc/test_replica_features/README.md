# CDC Replica Features Stress Test

Stress tests for CDC replication features: **switchover**, **replica heterogeneity**, and **pchannel increase**.

## Architecture

Two Milvus clusters in cluster+streaming mode:

| Cluster | ID | Proxy Port | Config |
|---------|----|------------|--------|
| A | by-dev1 | 19530 | default |
| B | by-dev2 | 19531 | `QUERYCOORD_CLUSTERLEVELLOADREPLICANUMBER=2` |

Each cluster runs: mixcoord, proxy, datanode, indexnode, 3 streaming nodes, 3 query nodes, CDC.

## Prerequisites

- Milvus built at `$MILVUS_DEV_PATH` (`make build`)
- Infrastructure running (etcd, minio, pulsar)
- Python environment with `pymilvus` and `loguru`
- Environment variables set:
  ```bash
  export MILVUS_DEV_PATH=~/workspace/milvus
  export MILVUS_VOLUME_DIRECTORY=/data/tmp/milvus-volumes
  ```

## Quick Start

```bash
# 1. Start infrastructure + clusters
MILVUS_STREAMING_SERVICE_ENABLED=1 QUERYCOORD_CLUSTERLEVELLOADREPLICANUMBER=2 \
  ~/workspace/snippets/milvus_control/milvus_control -m -c -s start_milvus_full

# 2. Wait for clusters to be ready
curl http://localhost:19101/healthz  # Cluster A proxy
curl http://localhost:19201/healthz  # Cluster B proxy

# 3. Run tests
cd ~/workspace/snippets/tests/test_cdc/test_replica_features
conda activate milvus2
python test_stress.py --cycles 1000 --duration 3600
python test_pchannel_increase.py

# 4. Stop everything
~/workspace/snippets/milvus_control/milvus_control -f stop_milvus_full
```

## Tests

### test_stress.py — Switchover + Replica Heterogeneity

Loops N cycles, each cycle:
1. Create collection with `replica_count = (N % 3) + 1` on primary
2. Verify data replicates to standby
3. Verify standby uses its **local** replica config (2), not primary's
4. Drop collection

Every 10 cycles, switchover toggles the topology (A->B becomes B->A).

```bash
# Run 1000 cycles or 1 hour, whichever comes first
python test_stress.py --cycles 1000 --duration 3600

# Quick smoke test (30 seconds)
python test_stress.py --cycles 1000 --duration 30
```

### test_pchannel_increase.py — Pchannel Count Increase

5 rounds, increasing `dmlChannelNum` each round (16 -> 17 -> 18 -> 19 -> 20).
Each round restarts both clusters with the new pchannel count, updates replicate config, and verifies replication.

```bash
python test_pchannel_increase.py
```

## File Structure

| File | Description |
|------|-------------|
| `common.py` | Shared constants, client init, replicate config helpers |
| `collection_helpers.py` | Collection CRUD, wait helpers, replica count verification |
| `cluster_control.py` | Cluster stop/start/restart via `milvus_control` |
| `test_stress.py` | Switchover + replica heterogeneity stress loop |
| `test_pchannel_increase.py` | Pchannel increase with cluster restarts |
| `start_clusters.sh` | Reference shell script for manual cluster startup |

## Cluster Management

```bash
MILVUS_CONTROL=~/workspace/snippets/milvus_control/milvus_control

# Start infra only
$MILVUS_CONTROL start_milvus_inf

# Start clusters (reuse existing volume)
MILVUS_STREAMING_SERVICE_ENABLED=1 QUERYCOORD_CLUSTERLEVELLOADREPLICANUMBER=2 \
  $MILVUS_CONTROL -m -c -s -u start_milvus

# Stop clusters only
$MILVUS_CONTROL -f stop_milvus

# Stop everything
$MILVUS_CONTROL -f stop_milvus_full
```

## Metrics Ports

| Component | Cluster A | Cluster B |
|-----------|-----------|-----------|
| mixcoord | 19100 | 19200 |
| proxy | 19101 | 19201 |
| datanode | 19102 | 19202 |
| indexnode | 19103 | 19203 |
| streaming nodes | 19104-19106 | 19204-19206 |
| query nodes | 19107-19109 | 19207-19209 |
| CDC | 19150 | 19250 |
