"""Test: concurrent update_replicate_configuration calls during force promotion."""
import threading
import time
import _path_setup  # noqa: F401
from loguru import logger
from common import (
    cluster_A_client, cluster_B_client, INSERT_COUNT,
    setup_collection, drop_if_exists, generate_data,
    wait_for_collection_dropped,
)
from failover.utils import (
    ensure_secondary_b, init_replication_a_to_b, force_promote_b, await_rows,
    REPL_A_TO_B,
)

COL = "failover_fastlock"
COL_NEW = f"{COL}_new"


def test_fastlock_contention():
    # Setup
    ensure_secondary_b()
    init_replication_a_to_b()

    setup_collection(COL, cluster_A_client, cluster_B_client)

    data = generate_data(INSERT_COUNT, 1)
    cluster_A_client.insert(COL, data)
    await_rows(cluster_B_client, COL, INSERT_COUNT)

    # Contention loop: keep calling update_replicate_configuration
    stop = threading.Event()

    def contention_loop():
        while not stop.is_set():
            try:
                cluster_B_client.update_replicate_configuration(**REPL_A_TO_B)
            except Exception:
                pass
            time.sleep(0.2)

    th = threading.Thread(target=contention_loop, daemon=True)
    th.start()

    try:
        force_promote_b()
    finally:
        stop.set()
        th.join(timeout=10)

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

    drop_if_exists(cluster_B_client, COL, "B")
    drop_if_exists(cluster_B_client, COL_NEW, "B")
    drop_if_exists(cluster_A_client, COL, "A")
    logger.info("PASSED: fastlock contention during promotion")


if __name__ == "__main__":
    test_fastlock_contention()
