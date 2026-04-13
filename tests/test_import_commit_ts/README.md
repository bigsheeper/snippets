# Import commit_timestamp E2E Tests

E2E tests for the import 2PC (Two-Phase Commit) feature with `commit_timestamp`, verifying MVCC visibility, delete correctness, and TTL behavior.

Based on:
- PR [#48472](https://github.com/milvus-io/milvus/pull/48472): Add `commit_timestamp` to SegmentInfo for correct MVCC/TTL/GC on import segments
- Issue [#48525](https://github.com/milvus-io/milvus/issues/48525): Support Import in replication clusters via 2PC
- PR [#48524](https://github.com/milvus-io/milvus/pull/48524): Import 2PC — CommitImport/AbortImport with auto_commit and WAL broadcast

## Test Cases

| Script | auto_commit | Scenario |
|--------|-------------|----------|
| `test_import_auto_commit.py` | `true` | Auto-commit path: import completes without explicit commit, search/query work |
| `test_import_basic.py` | `false` | Explicit commit: Uncommitted -> commit -> search/query/count |
| `test_import_mvcc.py` | `false` | Data invisible before commit (0 rows), visible after commit (500 rows) |
| `test_import_delete.py` | `false` | Delete before commit has no effect (500 rows), delete after commit works (400 rows) |
| `test_import_ttl.py` | `false` | TTL=30s: data survives at 15s after commit, expires at 35s after commit |
| `test_import_stability.py` | `false` | 30-min stability: alternates fresh + accumulate rounds, full MVCC + delete each round |

## Quick Start

```bash
# 1. Set environment
export MILVUS_DEV_PATH=~/workspace/milvus/.worktrees/test-import-commit-ts
export MILVUS_VOLUME_DIRECTORY=~/milvus-volumes
conda activate milvus2

# 2. Build Milvus (needs both PR #48472 and #48524 merged)
cd $MILVUS_DEV_PATH
source ./scripts/setenv.sh
make build-cpp-with-unittest  # C++ build (first time only, ~30min)
make build-go                 # Go build

# 3. Start Milvus
bash start_milvus.sh

# 4. Run tests
python test_import_auto_commit.py
python test_import_basic.py
python test_import_mvcc.py
python test_import_delete.py
python test_import_ttl.py       # ~1.5min (includes TTL wait times)
python test_import_stability.py  # ~30min (override with STABILITY_DURATION_MINUTES)

# 5. Stop Milvus
bash stop_milvus.sh
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MILVUS_URI` | `http://localhost:19530` | Milvus REST/gRPC endpoint |
| `MINIO_ENDPOINT` | `http://localhost:9000` | MinIO S3 endpoint |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `minioadmin` | MinIO secret key |
| `MINIO_BUCKET` | `a-bucket` | MinIO bucket name |
| `MILVUS_ROOT_PATH` | `files` | Milvus root path in object storage |
| `STABILITY_DURATION_MINUTES` | `30` | Duration for stability test (minutes) |
| `MILVUS_DEV_PATH` | (required) | Path to Milvus source with built binaries |
| `MILVUS_VOLUME_DIRECTORY` | (required) | Writable directory for Milvus data/logs |

## Prerequisites

- Docker (for etcd + minio + pulsar, managed by `start_milvus.sh` / `stop_milvus.sh`)
- Milvus built from PR #48472 + #48524 branches
- Python packages: `pymilvus`, `pyarrow`, `boto3`, `numpy`, `requests`
- Compose file: `~/workspace/snippets/milvus_control/docker-compose-pulsar.yml`

## How It Works

The tests exercise the import 2PC flow:

1. **Import with `auto_commit=false`**: data is ingested and indexed but stays invisible (`isImporting=true`)
2. **Uncommitted state**: segments exist but queries return 0 rows
3. **Commit**: `commit_timestamp` is assigned to segments, `isImporting` is cleared, data becomes visible
4. **commit_timestamp effects**:
   - MVCC: data only visible after commit_ts
   - Deletes with ts < commit_ts have no effect (row didn't logically exist yet)
   - TTL expiration counts from commit_ts, not row_ts (prevents premature expiration)
