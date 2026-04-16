"""Endurance test: repeatedly failover A->B with DDL before and after each promotion.

Runs for 30 minutes by default (configurable via ENDURANCE_DURATION_MINUTES env var).
Each iteration:
  1. Reset B to secondary, establish replication A -> B
  2. DDL before failover on A: create collection + index + load, create partition, insert+verify, release
  3. Force promote B
  4. DDL after failover on B: create new collection + index + load, insert+verify, drop old collection
  5. Cleanup
"""
import os
import time
import _path_setup  # noqa: F401
from loguru import logger
from common import (
    cluster_A_client, cluster_B_client, INSERT_COUNT,
    setup_collection, drop_if_exists, generate_data,
    wait_for_partition_created,
)
from common.schema import create_collection_schema, default_index_params
from failover.utils import ensure_secondary_b, init_replication_a_to_b, force_promote_b, await_rows

DURATION_MINUTES = int(os.getenv("ENDURANCE_DURATION_MINUTES", "30"))
COL_PREFIX = "endurance_failover"
PARTITION_NAME = "test_part"


def one_iteration(iteration: int):
    col_before = f"{COL_PREFIX}_before_{iteration}"
    col_after = f"{COL_PREFIX}_after_{iteration}"

    # --- Phase 1: Reset B to secondary, establish replication ---
    ensure_secondary_b()
    init_replication_a_to_b()

    # --- Phase 2: DDL before failover (on A, replicated to B) ---
    # create collection + index + load
    setup_collection(col_before, cluster_A_client, cluster_B_client)

    # create partition
    cluster_A_client.create_partition(col_before, PARTITION_NAME)
    wait_for_partition_created(col_before, PARTITION_NAME, cluster_B_client)
    logger.info(f"[iter {iteration}] Partition created on A, replicated to B")

    # insert + verify (DML to validate DDL actually works)
    data = generate_data(INSERT_COUNT, 1)
    cluster_A_client.insert(col_before, data)
    await_rows(cluster_B_client, col_before, INSERT_COUNT)
    logger.info(f"[iter {iteration}] Insert+verify before failover OK")

    # release (in-flight DDL, may not replicate before promote)
    cluster_A_client.release_collection(col_before)
    logger.info(f"[iter {iteration}] DDL before failover done")

    # --- Phase 3: Force promote B ---
    force_promote_b()
    logger.info(f"[iter {iteration}] Force promote B done")

    # --- Phase 4: DDL after failover (on promoted B) ---
    # create new collection + index + load
    schema = create_collection_schema()
    cluster_B_client.create_collection(collection_name=col_after, schema=schema)
    idx = default_index_params(cluster_B_client)
    cluster_B_client.create_index(col_after, index_params=idx)
    cluster_B_client.load_collection(col_after)
    logger.info(f"[iter {iteration}] New collection created+indexed+loaded on B")

    # insert + verify (DML to validate DDL actually works)
    data = generate_data(INSERT_COUNT, 1)
    cluster_B_client.insert(col_after, data)
    await_rows(cluster_B_client, col_after, INSERT_COUNT)
    logger.info(f"[iter {iteration}] Insert+verify after failover OK")

    # drop old collection (DDL)
    cluster_B_client.release_collection(col_before)
    cluster_B_client.drop_collection(col_before)
    logger.info(f"[iter {iteration}] Dropped old collection {col_before} on B")

    # --- Phase 5: Cleanup ---
    drop_if_exists(cluster_B_client, col_after, "B")
    drop_if_exists(cluster_A_client, col_before, "A")
    logger.info(f"[iter {iteration}] Cleanup done")


def test_endurance_failover_ddl():
    deadline = time.time() + DURATION_MINUTES * 60
    iteration = 0

    logger.info(f"Starting endurance failover DDL test, duration={DURATION_MINUTES}m")

    while time.time() < deadline:
        iteration += 1
        logger.info(f"=== Iteration {iteration} ===")
        start = time.time()
        one_iteration(iteration)
        elapsed = time.time() - start
        logger.info(f"[iter {iteration}] PASSED in {elapsed:.1f}s")

    total = DURATION_MINUTES * 60
    logger.info(
        f"PASSED: endurance failover DDL — {iteration} iterations in {DURATION_MINUTES}m, "
        f"avg {total / iteration:.1f}s/iter"
    )


if __name__ == "__main__":
    test_endurance_failover_ddl()
