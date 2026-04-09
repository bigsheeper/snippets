"""Continuous insert/upsert/delete stress test with data integrity checks.

Modes:
  full             - setup + insert loop + cleanup (A -> B)
  insert           - insert loop only, resume from existing data (A -> B)
  insert_reversed  - insert loop only with B -> A (after switchover)
  cleanup          - cleanup only

Usage:
  python test_continuously_insert.py --mode full --duration 600
"""
import argparse
import time
import _path_setup  # noqa: F401
from loguru import logger
from common import (
    cluster_A_client, cluster_B_client,
    DEFAULT_COLLECTION_NAME, INSERT_COUNT, PK_FIELD_NAME,
    init_replication_a_to_b,
    setup_collection, cleanup_collection,
    generate_data, query_all, wait_for_query_consistent,
)


def get_max_id(client, collection_name):
    res = query_all(client, collection_name)
    if not res:
        raise ValueError(f"Collection {collection_name} is empty")
    return max(r[PK_FIELD_NAME] for r in res)


def insert_loop(collection_name, primary, standby, duration, start_id=0):
    """Core loop: insert + periodic upsert/delete + verify consistency."""
    start_time = time.time()
    total = start_id
    loop = 0

    while time.time() - start_time < duration:
        # Insert
        data = generate_data(INSERT_COUNT, start_id)
        primary.insert(collection_name, data)
        start_id += INSERT_COUNT
        total += INSERT_COUNT
        loop += 1

        # Upsert every 20 loops (re-upsert current batch)
        if loop % 20 == 0:
            upsert_start = start_id - INSERT_COUNT
            data = generate_data(INSERT_COUNT, upsert_start)
            primary.upsert(collection_name, data)
            logger.info(f"Upsert: IDs {upsert_start}..{upsert_start + INSERT_COUNT - 1}")

        # Delete half of current batch every 10 loops
        if loop % 10 == 0:
            del_start = start_id - INSERT_COUNT
            del_end = del_start + INSERT_COUNT // 2
            primary.delete(collection_name=collection_name,
                           filter=f"{PK_FIELD_NAME} >= {del_start} and {PK_FIELD_NAME} < {del_end}")
            total -= INSERT_COUNT // 2
            logger.info(f"Delete: IDs {del_start}..{del_end - 1}")

        # Verify consistency
        res = query_all(primary, collection_name)
        wait_for_query_consistent(collection_name, res, standby)

    logger.info(f"Loop done: {loop} iterations, {total} rows, {time.time() - start_time:.0f}s")
    return total


def test_full(duration):
    name = DEFAULT_COLLECTION_NAME
    init_replication_a_to_b()
    setup_collection(name, cluster_A_client, cluster_B_client)
    insert_loop(name, cluster_A_client, cluster_B_client, duration)
    cleanup_collection(name, cluster_A_client, cluster_B_client)
    logger.info("PASSED: full cycle continuous insert")


def test_insert(duration):
    name = DEFAULT_COLLECTION_NAME
    start_id = get_max_id(cluster_A_client, name) + 1
    logger.info(f"Resuming from ID {start_id}")
    insert_loop(name, cluster_A_client, cluster_B_client, duration, start_id)
    logger.info("PASSED: insert-only continuous insert")


def test_insert_reversed(duration):
    name = DEFAULT_COLLECTION_NAME
    start_id = get_max_id(cluster_B_client, name) + 1
    logger.info(f"Reversed (B->A), resuming from ID {start_id}")
    insert_loop(name, cluster_B_client, cluster_A_client, duration, start_id)
    logger.info("PASSED: reversed continuous insert")


def test_cleanup():
    cleanup_collection(DEFAULT_COLLECTION_NAME, cluster_A_client, cluster_B_client)
    logger.info("PASSED: cleanup")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Continuous insert stress test")
    parser.add_argument("--mode", choices=["full", "insert", "insert_reversed", "cleanup"], default="full")
    parser.add_argument("--duration", type=int, default=600, help="Duration in seconds")
    args = parser.parse_args()

    {"full": test_full, "insert": test_insert,
     "insert_reversed": test_insert_reversed, "cleanup": test_cleanup,
     }[args.mode](args.duration) if args.mode != "cleanup" else test_cleanup()
