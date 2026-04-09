"""Test partition lifecycle replication: create -> insert -> load -> query -> release -> drop."""
import _path_setup  # noqa: F401
from loguru import logger
from common import (
    cluster_A_client, cluster_B_client,
    COLLECTION_NAME_PREFIX, NUM_COLLECTIONS, PARTITION_NAME_PREFIX,
    INSERT_COUNT, PK_FIELD_NAME,
    init_replication_a_to_b,
    setup_collection, drop_if_exists, generate_data,
    wait_for_partition_created, wait_for_partition_loaded,
    wait_for_partition_released, wait_for_partition_dropped,
    wait_for_query_consistent, wait_for_collection_dropped,
    query_all,
)


def test_partition():
    primary = cluster_A_client
    standby = cluster_B_client

    init_replication_a_to_b()
    col_names = [f"{COLLECTION_NAME_PREFIX}{i}" for i in range(NUM_COLLECTIONS)]
    part_names = [f"{PARTITION_NAME_PREFIX}{i}" for i in range(NUM_COLLECTIONS)]

    # Pre-cleanup
    for name in col_names:
        drop_if_exists(primary, name, "A")
        drop_if_exists(standby, name, "B")

    # Create collections with index
    for name in col_names:
        setup_collection(name, primary, standby)

    # Create partitions
    for col, part in zip(col_names, part_names):
        primary.create_partition(collection_name=col, partition_name=part)
        wait_for_partition_created(col, part, standby)

    # Insert into partitions
    start_id = 1
    for col, part in zip(col_names, part_names):
        data = generate_data(INSERT_COUNT, start_id)
        primary.insert(collection_name=col, partition_name=part, data=data)
        start_id += INSERT_COUNT

    # Load partitions
    for col, part in zip(col_names, part_names):
        primary.load_partitions(collection_name=col, partition_names=[part])
        wait_for_partition_loaded(col, part, standby)

    # Query consistency
    for col in col_names:
        res = query_all(primary, col)
        assert len(res) == INSERT_COUNT, f"Expected {INSERT_COUNT} on primary for {col}"
        wait_for_query_consistent(col, res, standby)

    # Release partitions
    for col, part in zip(col_names, part_names):
        primary.release_partitions(collection_name=col, partition_names=[part])
        wait_for_partition_released(col, part, standby)

    # Drop partitions
    for col, part in zip(col_names, part_names):
        primary.drop_partition(collection_name=col, partition_name=part)
        wait_for_partition_dropped(col, part, standby)

    # Drop collections
    for name in col_names:
        primary.drop_collection(name)
        wait_for_collection_dropped(name, standby)

    logger.info("PASSED: partition lifecycle replication")


if __name__ == "__main__":
    test_partition()
