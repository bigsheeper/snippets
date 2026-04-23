"""Pre/post-commit delete semantics mirror across A→B replication.

- Delete BEFORE commit: delete_ts < commit_ts → no-op on both A and B.
- Delete AFTER commit:  delete_ts > commit_ts → effective on both A and B.
"""
import os
import sys
import time

import _path_setup  # noqa: F401

# Side-load local common.py under a distinct name to avoid colliding with
# the test_cdc/common/ package that _path_setup put on sys.path.
import importlib.util as _ilu
_here = os.path.dirname(os.path.abspath(__file__))
_spec = _ilu.spec_from_file_location("import_common", os.path.join(_here, "common.py"))
imp_common = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(imp_common)

from loguru import logger

from common import (
    cluster_A_client, cluster_B_client,
    init_replication_a_to_b,
    setup_collection, cleanup_collection, drop_if_exists,
    wait_for_row_count,
)

generate_and_upload_parquet = imp_common.generate_and_upload_parquet
import_create_on_a = imp_common.import_create_on_a
import_commit_on_a = imp_common.import_commit_on_a
wait_for_import_state_on_a = imp_common.wait_for_import_state_on_a
wait_for_import_done_on_a = imp_common.wait_for_import_done_on_a
count_rows = imp_common.count_rows

COLLECTION_NAME = "test_import_replication_delete"
NUM_ROWS = 500


def _query_ids(client, collection, ids):
    """Return the subset of `ids` that currently exist on the client."""
    if not ids:
        return []
    results = client.query(
        collection_name=collection,
        filter=f"id in {list(ids)}",
        output_fields=["id"],
    )
    return sorted(r["id"] for r in results)


def test_import_replication_delete():
    primary = cluster_A_client
    standby = cluster_B_client

    init_replication_a_to_b()
    drop_if_exists(primary, COLLECTION_NAME, "A")
    drop_if_exists(standby, COLLECTION_NAME, "B")

    setup_collection(COLLECTION_NAME, primary, standby)

    # Start 2PC import on A
    remote_key = generate_and_upload_parquet(NUM_ROWS, start_id=0, prefix="delete_repl")
    job_id = import_create_on_a(COLLECTION_NAME, [[remote_key]], auto_commit=False)
    wait_for_import_state_on_a(job_id, "Uncommitted")
    logger.info("Import on A reached Uncommitted")

    # --- Pre-commit delete on A: delete_ts < commit_ts → should be a no-op ---
    pre_ids = list(range(0, 100))
    primary.delete(collection_name=COLLECTION_NAME, filter=f"id in {pre_ids}")
    logger.info("Issued pre-commit delete on A for ids [0..99]")
    time.sleep(1)

    # Commit on A (→ replicates via WAL to B)
    import_commit_on_a(job_id)
    wait_for_import_done_on_a(job_id)
    logger.info("Import on A committed + completed")

    # Pre-commit delete must be a no-op on BOTH sides
    wait_for_row_count(primary, COLLECTION_NAME, NUM_ROWS, timeout=120)
    wait_for_row_count(standby, COLLECTION_NAME, NUM_ROWS)
    logger.info(f"Pre-commit delete no-op confirmed: A=B={NUM_ROWS}")

    sample_pre = [0, 1, 2, 50, 99]
    assert _query_ids(primary, COLLECTION_NAME, sample_pre) == sample_pre, \
        "pre-commit-deleted IDs should still exist on A (delete_ts < commit_ts)"
    assert _query_ids(standby, COLLECTION_NAME, sample_pre) == sample_pre, \
        "pre-commit-deleted IDs should still exist on B (delete_ts < commit_ts)"
    logger.info(f"Pre-commit-deleted IDs {sample_pre} still present on A and B")

    # --- Post-commit delete on A: delete_ts > commit_ts → should take effect ---
    post_ids = list(range(100, 200))
    primary.delete(collection_name=COLLECTION_NAME, filter=f"id in {post_ids}")
    logger.info("Issued post-commit delete on A for ids [100..199]")

    expected_remaining = NUM_ROWS - 100
    wait_for_row_count(primary, COLLECTION_NAME, expected_remaining)
    wait_for_row_count(standby, COLLECTION_NAME, expected_remaining)
    logger.info(f"Post-commit delete effective: A=B={expected_remaining}")

    sample_post = [100, 150, 199]
    assert _query_ids(primary, COLLECTION_NAME, sample_post) == [], \
        "post-commit-deleted IDs should be gone from A"
    assert _query_ids(standby, COLLECTION_NAME, sample_post) == [], \
        "post-commit-deleted IDs should be gone from B"
    logger.info(f"Post-commit-deleted IDs {sample_post} gone from A and B")

    # Non-deleted IDs still present on B
    survivors = [0, 50, 200, 300, 499]
    assert _query_ids(standby, COLLECTION_NAME, survivors) == sorted(survivors), \
        "non-deleted IDs should still exist on B"
    logger.info(f"Non-deleted IDs {survivors} still present on B")

    cleanup_collection(COLLECTION_NAME, primary, standby)
    logger.info("PASSED: test_import_replication_delete")


if __name__ == "__main__":
    test_import_replication_delete()
