"""Test collection lifecycle replication: create -> index -> load -> release -> drop."""
import _path_setup  # noqa: F401
from loguru import logger
from common import (
    cluster_A_client, cluster_B_client,
    COLLECTION_NAME_PREFIX, NUM_COLLECTIONS,
    init_replication_a_to_b,
    setup_collection, cleanup_collection,
    wait_for_collection_released, wait_for_collection_dropped,
)


def test_collection():
    primary = cluster_A_client
    standby = cluster_B_client

    init_replication_a_to_b()

    names = [f"{COLLECTION_NAME_PREFIX}{i}" for i in range(NUM_COLLECTIONS)]

    logger.info(f"Creating {NUM_COLLECTIONS} collections with index + load...")
    for name in names:
        setup_collection(name, primary, standby)

    logger.info("Releasing all collections...")
    for name in names:
        primary.release_collection(name)
        wait_for_collection_released(name, standby)

    logger.info("Dropping all collections...")
    for name in names:
        primary.drop_collection(name)
        wait_for_collection_dropped(name, standby)

    logger.info("PASSED: collection lifecycle replication")


if __name__ == "__main__":
    test_collection()
