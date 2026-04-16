"""Test: B cluster restart during force promotion still completes successfully."""
import os
import time
import _path_setup  # noqa: F401
from loguru import logger
from common import (
    cluster_A_client, cluster_B_client, INSERT_COUNT,
    setup_collection, drop_if_exists, generate_data,
    start_async, stop_cluster_b, start_cluster_b,
    wait_for_collection_dropped,
)
from failover.utils import ensure_secondary_b, init_replication_a_to_b, force_promote_b, await_rows

COL = "failover_restart_b"
COL_NEW = f"{COL}_new"


def test_restart_b_mid_promotion():
    # Pre-cleanup: try both sides (B may be primary or secondary from last run)
    drop_if_exists(cluster_B_client, COL, "B")
    drop_if_exists(cluster_A_client, COL, "A")

    # Setup
    ensure_secondary_b()
    init_replication_a_to_b()

    setup_collection(COL, cluster_A_client, cluster_B_client)

    data = generate_data(INSERT_COUNT, 1)
    cluster_A_client.insert(COL, data)
    await_rows(cluster_B_client, COL, INSERT_COUNT)

    # Force promote async, restart B mid-way
    _, join = start_async(force_promote_b)
    time.sleep(2)
    stop_cluster_b()
    time.sleep(2)
    start_cluster_b()

    timeout = int(os.getenv("FAILOVER_TIMEOUT", "180"))
    join(timeout=timeout)

    # B is now primary — create new collection
    from common.schema import create_collection_schema, default_index_params
    schema = create_collection_schema()
    cluster_B_client.create_collection(collection_name=COL_NEW, schema=schema)
    idx = default_index_params(cluster_B_client)
    cluster_B_client.create_index(COL_NEW, index_params=idx)
    cluster_B_client.load_collection(COL_NEW)

    # Insert + verify on B
    data = generate_data(INSERT_COUNT, 1)
    cluster_B_client.insert(COL_NEW, data)
    await_rows(cluster_B_client, COL_NEW, INSERT_COUNT)

    # Cleanup
    drop_if_exists(cluster_B_client, COL, "B")
    drop_if_exists(cluster_B_client, COL_NEW, "B")
    drop_if_exists(cluster_A_client, COL, "A")
    logger.info("PASSED: restart B during force promote")


if __name__ == "__main__":
    test_restart_b_mid_promotion()
