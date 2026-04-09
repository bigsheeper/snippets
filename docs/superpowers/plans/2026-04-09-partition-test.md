# Partition CDC Test Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a new end-to-end CDC testcase for partition lifecycle replication, following the orchestration style of `test_collection.py` while also verifying inserts into named partitions.

**Architecture:** Keep `test_partition.py` as a short orchestration entrypoint and put all partition-specific Milvus operations in a new `partition.py` helper beside the existing `collection.py` and `insert.py` helpers. Implement the work with strict TDD: add the failing test entrypoint first, confirm the import failure, then add the minimum partition helper functions needed for create, insert, flush, load, release, drop, and standby polling.

**Tech Stack:** Python, `pymilvus.MilvusClient`, existing CDC testcase helpers under `tests/test_cdc/testcases`

---

### Task 1: Add the failing partition testcase entrypoint

**Files:**
- Create: `tests/test_cdc/testcases/test_partition.py`
- Modify: none
- Test: `tests/test_cdc/testcases/test_partition.py`

- [ ] **Step 1: Write the failing test**

```python
from common import *
from collection import *
from insert import *
from partition import *


def test_partition():
    primary_client = cluster_A_client
    standby_client = cluster_B_client

    collection_names = get_collection_names()
    partition_names = get_partition_names()

    create_collections_on_primary(collection_names, primary_client)
    wait_for_standby_create_collections(collection_names, standby_client)

    create_partitions_on_primary(collection_names, partition_names, primary_client)
    wait_for_standby_create_partitions(collection_names, partition_names, standby_client)

    insert_into_primary_partitions(collection_names, partition_names, primary_client)
    flush_collections_on_primary(collection_names, primary_client)

    load_partitions_on_primary(collection_names, partition_names, primary_client)
    wait_for_standby_load_partitions(collection_names, partition_names, standby_client)

    release_partitions_on_primary(collection_names, partition_names, primary_client)
    wait_for_standby_release_partitions(collection_names, partition_names, standby_client)

    drop_partitions_on_primary(collection_names, partition_names, primary_client)
    wait_for_standby_drop_partitions(collection_names, partition_names, standby_client)

    drop_collections_on_primary(collection_names, primary_client)
    wait_for_standby_drop_collections(collection_names, standby_client)


if __name__ == "__main__":
    test_partition()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `source ~/.zshrc >/dev/null 2>&1 && conda activate milvus2 && cd /home/sheep/workspace/snippets/.worktrees/feat-test-partition/tests/test_cdc/testcases && python test_partition.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'partition'`

- [ ] **Step 3: Commit**

```bash
git -C "/home/sheep/workspace/snippets/.worktrees/feat-test-partition" add tests/test_cdc/testcases/test_partition.py
git -C "/home/sheep/workspace/snippets/.worktrees/feat-test-partition" commit -s -m "$(cat <<'EOF'
test: add failing partition CDC testcase entrypoint
EOF
)"
```

### Task 2: Add minimal partition helper creation and polling support

**Files:**
- Create: `tests/test_cdc/testcases/partition.py`
- Modify: none
- Test: `tests/test_cdc/testcases/test_partition.py`

- [ ] **Step 1: Write the minimal helper implementation for create path**

```python
import time
from loguru import logger
from common import *

PARTITION_NAME_PREFIX = "partition_"


def get_partition_name(i, prefix=PARTITION_NAME_PREFIX):
    return f"{prefix}{i}"


def get_partition_names(num_partitions=NUM_COLLECTIONS, prefix=PARTITION_NAME_PREFIX):
    return [get_partition_name(i, prefix) for i in range(num_partitions)]


def create_partition_on_primary(collection_name, partition_name, client):
    client.create_partition(collection_name=collection_name, partition_name=partition_name)
    logger.info(f"Partition created on primary, collection: {collection_name}, name: {partition_name}")


def create_partitions_on_primary(collection_names, partition_names, client):
    for collection_name, partition_name in zip(collection_names, partition_names):
        create_partition_on_primary(collection_name, partition_name, client)


def wait_for_standby_create_partition(collection_name, partition_name, client):
    start_time = time.time()
    while True:
        if client.has_partition(collection_name=collection_name, partition_name=partition_name):
            break
        if time.time() - start_time > TIMEOUT:
            error_msg = f"Timeout waiting for partition to be created on standby: {collection_name}/{partition_name}"
            logger.error(error_msg)
            raise TimeoutError(error_msg)
        time.sleep(1)
    logger.info(f"Partition created on standby, collection: {collection_name}, name: {partition_name}")


def wait_for_standby_create_partitions(collection_names, partition_names, client):
    for collection_name, partition_name in zip(collection_names, partition_names):
        wait_for_standby_create_partition(collection_name, partition_name, client)
```

- [ ] **Step 2: Run test to verify it fails for the next missing symbol**

Run: `source ~/.zshrc >/dev/null 2>&1 && conda activate milvus2 && cd /home/sheep/workspace/snippets/.worktrees/feat-test-partition/tests/test_cdc/testcases && python test_partition.py`
Expected: FAIL with `NameError` for `insert_into_primary_partitions`, `flush_collections_on_primary`, or another still-missing partition helper

- [ ] **Step 3: Commit**

```bash
git -C "/home/sheep/workspace/snippets/.worktrees/feat-test-partition" add tests/test_cdc/testcases/partition.py
git -C "/home/sheep/workspace/snippets/.worktrees/feat-test-partition" commit -s -m "$(cat <<'EOF'
test: add partition creation helpers for CDC testcase
EOF
)"
```

### Task 3: Add partition insert, flush, load, release, and drop helpers

**Files:**
- Modify: `tests/test_cdc/testcases/partition.py`
- Test: `tests/test_cdc/testcases/test_partition.py`

- [ ] **Step 1: Extend the helper with the remaining minimal implementation**

```python
from collection import *
from insert import generate_data, INSERT_COUNT
from pymilvus.client.types import LoadState


def insert_into_primary_partition(start_id, collection_name, partition_name, client):
    data = generate_data(INSERT_COUNT, start_id=start_id, dim=DEFAULT_DIM)
    client.insert(collection_name=collection_name, data=data, partition_name=partition_name)
    logger.info(
        f"Inserted {INSERT_COUNT} data into primary partition, collection: {collection_name}, partition: {partition_name}, start_id: {start_id}"
    )
    return start_id + INSERT_COUNT


def insert_into_primary_partitions(collection_names, partition_names, client):
    start_id = 1
    for collection_name, partition_name in zip(collection_names, partition_names):
        start_id = insert_into_primary_partition(start_id, collection_name, partition_name, client)


def flush_collection_on_primary(collection_name, client):
    client.flush(collection_name=collection_name)
    logger.info(f"Collection flushed on primary, name: {collection_name}")


def flush_collections_on_primary(collection_names, client):
    for collection_name in collection_names:
        flush_collection_on_primary(collection_name, client)


def load_partition_on_primary(collection_name, partition_name, client):
    client.load_partitions(collection_name=collection_name, partition_names=[partition_name])
    logger.info(f"Partition loaded on primary, collection: {collection_name}, name: {partition_name}")


def load_partitions_on_primary(collection_names, partition_names, client):
    for collection_name, partition_name in zip(collection_names, partition_names):
        load_partition_on_primary(collection_name, partition_name, client)


def wait_for_standby_load_partition(collection_name, partition_name, client):
    start_time = time.time()
    while True:
        load_state = client.get_load_state(collection_name=collection_name, partition_name=partition_name)
        if load_state["state"] == LoadState.Loaded:
            break
        if time.time() - start_time > TIMEOUT:
            error_msg = f"Timeout waiting for partition to be loaded on standby: {collection_name}/{partition_name}"
            logger.error(error_msg)
            raise TimeoutError(error_msg)
        time.sleep(1)
    logger.info(f"Partition loaded on standby, collection: {collection_name}, name: {partition_name}")


def wait_for_standby_load_partitions(collection_names, partition_names, client):
    for collection_name, partition_name in zip(collection_names, partition_names):
        wait_for_standby_load_partition(collection_name, partition_name, client)


def release_partition_on_primary(collection_name, partition_name, client):
    client.release_partitions(collection_name=collection_name, partition_names=[partition_name])
    logger.info(f"Partition released on primary, collection: {collection_name}, name: {partition_name}")


def release_partitions_on_primary(collection_names, partition_names, client):
    for collection_name, partition_name in zip(collection_names, partition_names):
        release_partition_on_primary(collection_name, partition_name, client)


def wait_for_standby_release_partition(collection_name, partition_name, client):
    start_time = time.time()
    while True:
        load_state = client.get_load_state(collection_name=collection_name, partition_name=partition_name)
        if load_state["state"] == LoadState.NotLoad:
            break
        if time.time() - start_time > TIMEOUT:
            error_msg = f"Timeout waiting for partition to be released on standby: {collection_name}/{partition_name}"
            logger.error(error_msg)
            raise TimeoutError(error_msg)
        time.sleep(1)
    logger.info(f"Partition released on standby, collection: {collection_name}, name: {partition_name}")


def wait_for_standby_release_partitions(collection_names, partition_names, client):
    for collection_name, partition_name in zip(collection_names, partition_names):
        wait_for_standby_release_partition(collection_name, partition_name, client)


def drop_partition_on_primary(collection_name, partition_name, client):
    client.drop_partition(collection_name=collection_name, partition_name=partition_name)
    logger.info(f"Partition dropped on primary, collection: {collection_name}, name: {partition_name}")


def drop_partitions_on_primary(collection_names, partition_names, client):
    for collection_name, partition_name in zip(collection_names, partition_names):
        drop_partition_on_primary(collection_name, partition_name, client)


def wait_for_standby_drop_partition(collection_name, partition_name, client):
    start_time = time.time()
    while True:
        if not client.has_partition(collection_name=collection_name, partition_name=partition_name):
            break
        if time.time() - start_time > TIMEOUT:
            error_msg = f"Timeout waiting for partition to be dropped on standby: {collection_name}/{partition_name}"
            logger.error(error_msg)
            raise TimeoutError(error_msg)
        time.sleep(1)
    logger.info(f"Partition dropped on standby, collection: {collection_name}, name: {partition_name}")


def wait_for_standby_drop_partitions(collection_names, partition_names, client):
    for collection_name, partition_name in zip(collection_names, partition_names):
        wait_for_standby_drop_partition(collection_name, partition_name, client)
```

- [ ] **Step 2: Run test to verify it passes**

Run: `source ~/.zshrc >/dev/null 2>&1 && conda activate milvus2 && cd /home/sheep/workspace/snippets/.worktrees/feat-test-partition/tests/test_cdc/testcases && python test_partition.py`
Expected: PASS with log lines showing create, insert, flush, load, release, drop, and standby wait completion for partitions and collections

- [ ] **Step 3: Commit**

```bash
git -C "/home/sheep/workspace/snippets/.worktrees/feat-test-partition" add tests/test_cdc/testcases/partition.py
git -C "/home/sheep/workspace/snippets/.worktrees/feat-test-partition" commit -s -m "$(cat <<'EOF'
test: add partition lifecycle helpers for CDC testcase
EOF
)"
```

### Task 4: Final verification and cleanup review

**Files:**
- Verify: `tests/test_cdc/testcases/test_partition.py`
- Verify: `tests/test_cdc/testcases/partition.py`
- Verify: `docs/superpowers/specs/2026-04-09-partition-test-design.md`
- Verify: `docs/superpowers/plans/2026-04-09-partition-test.md`

- [ ] **Step 1: Re-run the focused testcase from a clean command line**

Run: `source ~/.zshrc >/dev/null 2>&1 && conda activate milvus2 && cd /home/sheep/workspace/snippets/.worktrees/feat-test-partition/tests/test_cdc/testcases && python test_partition.py`
Expected: PASS with no Python traceback

- [ ] **Step 2: Inspect git diff to confirm scope stayed minimal**

Run: `git -C "/home/sheep/workspace/snippets/.worktrees/feat-test-partition" diff -- tests/test_cdc/testcases/test_partition.py tests/test_cdc/testcases/partition.py docs/superpowers/specs/2026-04-09-partition-test-design.md docs/superpowers/plans/2026-04-09-partition-test.md`
Expected: Only the new testcase, helper module, spec, and plan changes appear

- [ ] **Step 3: Commit**

```bash
git -C "/home/sheep/workspace/snippets/.worktrees/feat-test-partition" add docs/superpowers/plans/2026-04-09-partition-test.md
git -C "/home/sheep/workspace/snippets/.worktrees/feat-test-partition" commit -s -m "$(cat <<'EOF'
doc: add partition CDC testcase implementation plan
EOF
)"
```
