"""Test insert/delete replication with data integrity checks."""
import _path_setup  # noqa: F401
from loguru import logger
from common import (
    cluster_A_client, cluster_B_client,
    DEFAULT_COLLECTION_NAME, PK_FIELD_NAME, INSERT_COUNT, INSERT_ROUNDS,
    init_replication_a_to_b,
    setup_collection, cleanup_collection, insert_and_verify,
    generate_data, query_all, wait_for_query_consistent,
)


def test_insert():
    primary = cluster_A_client
    standby = cluster_B_client
    name = DEFAULT_COLLECTION_NAME

    init_replication_a_to_b()
    setup_collection(name, primary, standby)

    # Multi-round insert
    total = INSERT_COUNT * INSERT_ROUNDS
    start_id = 1
    for _ in range(INSERT_ROUNDS):
        data = generate_data(INSERT_COUNT, start_id)
        primary.insert(name, data)
        start_id += INSERT_COUNT
    logger.info(f"Inserted {total} rows in {INSERT_ROUNDS} rounds")

    res = query_all(primary, name)
    assert len(res) == total, f"Expected {total} rows on primary, got {len(res)}"
    wait_for_query_consistent(name, res, standby)

    # Delete first 2000
    delete_count = min(2000, total)
    primary.delete(collection_name=name, filter=f"{PK_FIELD_NAME} <= {delete_count}")
    logger.info(f"Deleted rows with {PK_FIELD_NAME} <= {delete_count}")

    res = query_all(primary, name)
    assert len(res) == total - delete_count, f"Expected {total - delete_count}, got {len(res)}"
    wait_for_query_consistent(name, res, standby)

    cleanup_collection(name, primary, standby)
    logger.info("PASSED: insert/delete replication")


if __name__ == "__main__":
    test_insert()
