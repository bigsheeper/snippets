"""Smoke test: init replication, create collection on A, verify it appears on B."""
import _path_setup  # noqa: F401
from loguru import logger
from common import (
    cluster_A_client, cluster_B_client,
    CLUSTER_A_ID, CLUSTER_B_ID,
    build_replicate_config_2, drop_if_exists,
    setup_collection, cleanup_collection,
)

COLLECTION_NAME = "smoke_quick_start"


def test_quick_start():
    # Init replication A -> B
    config = build_replicate_config_2(CLUSTER_A_ID, CLUSTER_B_ID)
    cluster_B_client.update_replicate_configuration(**config)
    cluster_A_client.update_replicate_configuration(**config)
    logger.info("Replication A -> B initialized")

    # Cleanup from previous runs
    drop_if_exists(cluster_A_client, COLLECTION_NAME, "A")
    drop_if_exists(cluster_B_client, COLLECTION_NAME, "B")

    # Create + verify replication
    setup_collection(COLLECTION_NAME, cluster_A_client, cluster_B_client)
    logger.info("Collection replicated to B")

    # Cleanup
    cleanup_collection(COLLECTION_NAME, cluster_A_client, cluster_B_client)
    logger.info("PASSED: quick start smoke test")


if __name__ == "__main__":
    test_quick_start()
