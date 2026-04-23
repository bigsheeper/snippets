"""2PC import on A replicates to B only after commit.

Import with auto_commit=false on A; verify B stays empty while
Uncommitted, then converges to N rows after commit on A.
"""
import os
import sys

# _path_setup must come before any `from common import ...`, because
# that resolves to the sibling test_cdc/common/ package.
import _path_setup  # noqa: F401

# Load our local common.py under a distinct name to avoid colliding with
# the test_cdc/common/ package that _path_setup put on sys.path.
import importlib.util as _ilu
_here = os.path.dirname(os.path.abspath(__file__))
_spec = _ilu.spec_from_file_location("import_common", os.path.join(_here, "common.py"))
imp_common = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(imp_common)

import numpy as np
from loguru import logger

from common import (
    cluster_A_client, cluster_B_client,
    init_replication_a_to_b,
    setup_collection, cleanup_collection, drop_if_exists,
    query_all, wait_for_query_consistent, wait_for_row_count,
    VECTOR_FIELD_NAME,
)

generate_and_upload_parquet = imp_common.generate_and_upload_parquet
import_create_on_a = imp_common.import_create_on_a
import_commit_on_a = imp_common.import_commit_on_a
wait_for_import_state_on_a = imp_common.wait_for_import_state_on_a
wait_for_import_done_on_a = imp_common.wait_for_import_done_on_a
count_rows = imp_common.count_rows
DIM = imp_common.DIM
RNG = imp_common.RNG

COLLECTION_NAME = "test_import_replication_basic"
NUM_ROWS = 1000


def test_import_replication_basic():
    primary = cluster_A_client
    standby = cluster_B_client

    # Replication init + clean state
    init_replication_a_to_b()
    drop_if_exists(primary, COLLECTION_NAME, "A")
    drop_if_exists(standby, COLLECTION_NAME, "B")

    # Collection + index + load on A; wait for B to mirror
    setup_collection(COLLECTION_NAME, primary, standby)

    # Upload parquet + start 2PC import on A
    remote_key = generate_and_upload_parquet(NUM_ROWS, start_id=0, prefix="basic_repl")
    job_id = import_create_on_a(COLLECTION_NAME, [[remote_key]], auto_commit=False)

    # Wait until Uncommitted — data must be invisible on both A and B
    wait_for_import_state_on_a(job_id, "Uncommitted")
    logger.info("Import on A reached Uncommitted")

    assert count_rows(primary, COLLECTION_NAME) == 0, "A should have 0 rows pre-commit"
    wait_for_row_count(standby, COLLECTION_NAME, 0, timeout=30)
    logger.info("Pre-commit invariant holds: A=0, B=0")

    # Commit on A
    import_commit_on_a(job_id)
    wait_for_import_done_on_a(job_id)
    logger.info("Import on A committed + completed (job state)")

    # Wait for all NUM_ROWS to become query-visible on A.
    # The job reaching Completed state does not guarantee immediate query
    # visibility; there may be a brief lag before the segments are sealed
    # and flushed into the query layer.
    wait_for_row_count(primary, COLLECTION_NAME, NUM_ROWS, timeout=120)
    logger.info(f"A now has {NUM_ROWS} rows visible")

    # A + B must both see all NUM_ROWS rows with matching PKs
    a_rows = query_all(primary, COLLECTION_NAME)
    assert len(a_rows) == NUM_ROWS, f"Expected {NUM_ROWS} on A, got {len(a_rows)}"
    wait_for_query_consistent(COLLECTION_NAME, a_rows, standby)
    logger.info(f"Post-commit: A=B={NUM_ROWS} with matching PK set")

    # Search on B returns 10 hits
    query_vec = RNG.random((1, DIM), dtype=np.float32).tolist()
    search_results = standby.search(
        collection_name=COLLECTION_NAME,
        data=query_vec,
        anns_field=VECTOR_FIELD_NAME,
        search_params={"metric_type": "COSINE"},
        limit=10,
        output_fields=["id"],
    )
    assert len(search_results) == 1 and len(search_results[0]) == 10, \
        f"Expected 10 hits on B, got {search_results}"
    for hit in search_results[0]:
        assert 0 <= hit["id"] < NUM_ROWS, f"unexpected hit id: {hit['id']}"
    logger.info("[PASS] Search on B returns 10 hits")

    cleanup_collection(COLLECTION_NAME, primary, standby)
    logger.info("PASSED: test_import_replication_basic")


if __name__ == "__main__":
    test_import_replication_basic()
