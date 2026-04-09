"""Test: DDL operations during force promotion (incomplete broadcast edge case)."""
import _path_setup  # noqa: F401
from loguru import logger
from common import (
    cluster_A_client, cluster_B_client, INSERT_COUNT,
    setup_collection, drop_if_exists, generate_data,
    wait_for_collection_loaded,
)
from failover.utils import ensure_secondary_b, init_replication_a_to_b, force_promote_b, await_rows

COL = "failover_broadcast_ddl"
COL_NEW = f"{COL}_new"


def test_incomplete_broadcast_ddl():
    # Setup
    ensure_secondary_b()
    init_replication_a_to_b()
    drop_if_exists(cluster_A_client, COL, "A")
    drop_if_exists(cluster_B_client, COL, "B")
    drop_if_exists(cluster_B_client, COL_NEW, "B")

    # Create + index + load on A, wait on B
    setup_collection(COL, cluster_A_client, cluster_B_client)

    # Release on A (in-flight ddl, may not replicate before promote)
    cluster_A_client.release_collection(COL)

    # Force promote B
    force_promote_b()

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
    logger.info("PASSED: incomplete broadcast DDL")


if __name__ == "__main__":
    test_incomplete_broadcast_ddl()
