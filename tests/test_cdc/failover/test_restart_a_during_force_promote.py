"""Test: A cluster restart during force promotion still completes successfully."""
import os
import time
import _path_setup  # noqa: F401
from loguru import logger
from common import (
    cluster_A_client, cluster_B_client, INSERT_COUNT,
    setup_collection, drop_if_exists, generate_data,
    start_async, stop_cluster_a, start_cluster_a,
)
from failover.utils import ensure_secondary_b, init_replication_a_to_b, force_promote_b, await_rows

COL = "failover_restart_a"


def test_restart_a_mid_promotion():
    # Setup
    ensure_secondary_b()
    init_replication_a_to_b()
    drop_if_exists(cluster_A_client, COL, "A")
    drop_if_exists(cluster_B_client, COL, "B")

    setup_collection(COL, cluster_A_client, cluster_B_client)

    data = generate_data(INSERT_COUNT, 1)
    cluster_A_client.insert(COL, data)
    await_rows(cluster_B_client, COL, INSERT_COUNT)

    # Force promote async, restart A mid-way
    _, join = start_async(force_promote_b)
    time.sleep(2)
    stop_cluster_a()
    time.sleep(2)
    start_cluster_a()

    timeout = int(os.getenv("FAILOVER_TIMEOUT", "180"))
    join(timeout=timeout)

    # Verify B accepts writes
    cluster_B_client.insert(COL, generate_data(500, INSERT_COUNT + 1))
    await_rows(cluster_B_client, COL, INSERT_COUNT + 500)

    # Cleanup
    drop_if_exists(cluster_B_client, COL, "B")
    drop_if_exists(cluster_A_client, COL, "A")
    logger.info("PASSED: restart A during force promote")


if __name__ == "__main__":
    test_restart_a_mid_promotion()
