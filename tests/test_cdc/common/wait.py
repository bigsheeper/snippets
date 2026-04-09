"""Generic poll-until-true helper and all replication wait functions."""
import time
from loguru import logger
from pymilvus.client.types import LoadState
from common.constants import TIMEOUT, PK_FIELD_NAME, VECTOR_FIELD_NAME


def wait_until(predicate, timeout=TIMEOUT, interval=1, msg="condition"):
    """Poll predicate() until truthy or timeout. Returns the truthy value."""
    start = time.time()
    while True:
        result = predicate()
        if result:
            return result
        if time.time() - start > timeout:
            raise TimeoutError(f"Timeout ({timeout}s) waiting for: {msg}")
        time.sleep(interval)


# ---- Collection ----

def wait_for_collection_created(name, client, timeout=TIMEOUT):
    wait_until(lambda: client.has_collection(name), timeout=timeout,
               msg=f"collection '{name}' created on standby")
    logger.info(f"Collection created on standby: {name}")


def wait_for_collection_loaded(name, client, timeout=TIMEOUT):
    wait_until(
        lambda: client.get_load_state(collection_name=name)["state"] == LoadState.Loaded,
        timeout=timeout, msg=f"collection '{name}' loaded on standby")
    logger.info(f"Collection loaded on standby: {name}")


def wait_for_collection_released(name, client, timeout=TIMEOUT):
    wait_until(
        lambda: client.get_load_state(collection_name=name)["state"] == LoadState.NotLoad,
        timeout=timeout, msg=f"collection '{name}' released on standby")
    logger.info(f"Collection released on standby: {name}")


def wait_for_collection_dropped(name, client, timeout=TIMEOUT):
    wait_until(lambda: not client.has_collection(name), timeout=timeout,
               msg=f"collection '{name}' dropped on standby")
    logger.info(f"Collection dropped on standby: {name}")


# ---- Index ----

def wait_for_index_created(name, client, timeout=TIMEOUT):
    wait_until(
        lambda: client.describe_index(name, index_name=VECTOR_FIELD_NAME) is not None,
        timeout=timeout, msg=f"index on '{name}' created on standby")
    logger.info(f"Index created on standby: {name}")


# ---- Partition ----

def wait_for_partition_created(collection_name, partition_name, client, timeout=TIMEOUT):
    wait_until(
        lambda: client.has_partition(collection_name=collection_name, partition_name=partition_name),
        timeout=timeout,
        msg=f"partition '{partition_name}' on '{collection_name}' created on standby")
    logger.info(f"Partition created on standby: {collection_name}/{partition_name}")


def wait_for_partition_loaded(collection_name, partition_name, client, timeout=TIMEOUT):
    wait_until(
        lambda: client.get_load_state(collection_name=collection_name, partition_name=partition_name)["state"] == LoadState.Loaded,
        timeout=timeout,
        msg=f"partition '{partition_name}' on '{collection_name}' loaded on standby")
    logger.info(f"Partition loaded on standby: {collection_name}/{partition_name}")


def wait_for_partition_released(collection_name, partition_name, client, timeout=TIMEOUT):
    wait_until(
        lambda: client.get_load_state(collection_name=collection_name, partition_name=partition_name)["state"] == LoadState.NotLoad,
        timeout=timeout,
        msg=f"partition '{partition_name}' on '{collection_name}' released on standby")
    logger.info(f"Partition released on standby: {collection_name}/{partition_name}")


def wait_for_partition_dropped(collection_name, partition_name, client, timeout=TIMEOUT):
    wait_until(
        lambda: not client.has_partition(collection_name=collection_name, partition_name=partition_name),
        timeout=timeout,
        msg=f"partition '{partition_name}' on '{collection_name}' dropped on standby")
    logger.info(f"Partition dropped on standby: {collection_name}/{partition_name}")


# ---- Query consistency ----

def query_all(client, collection_name):
    """Query all rows from a collection, return list of dicts."""
    return client.query(
        collection_name=collection_name,
        consistency_level="Strong",
        filter=f"{PK_FIELD_NAME} >= 0",
        output_fields=[PK_FIELD_NAME],
    )


def wait_for_query_consistent(collection_name, expected_results, client, timeout=TIMEOUT):
    """Poll until standby query result matches expected (by sorted PK list)."""
    expected_ids = sorted([r[PK_FIELD_NAME] for r in expected_results])

    def _check():
        actual = query_all(client, collection_name)
        if len(actual) != len(expected_ids):
            logger.warning(f"Query count mismatch: expected={len(expected_ids)}, actual={len(actual)}")
            return False
        return sorted([r[PK_FIELD_NAME] for r in actual]) == expected_ids

    wait_until(_check, timeout=timeout, msg=f"query consistency on '{collection_name}'")
    logger.info(f"Query consistent on standby: {collection_name}, count={len(expected_ids)}")


def wait_for_row_count(client, collection_name, expected_count, timeout=TIMEOUT):
    """Poll until collection has exactly expected_count rows."""
    def _check():
        res = query_all(client, collection_name)
        return len(res) == expected_count

    wait_until(_check, timeout=timeout, interval=2,
               msg=f"row count {expected_count} on '{collection_name}'")


# ---- High-level helpers ----

def setup_collection(collection_name, primary_client, standby_clients, replica_num=1, shard_num=1):
    """Create collection + index + load on primary, wait for replication to standby(s).

    standby_clients: a single client or a list of clients.
    """
    from common.schema import create_collection_schema, default_index_params

    if not isinstance(standby_clients, (list, tuple)):
        standby_clients = [standby_clients]

    schema = create_collection_schema()
    primary_client.create_collection(collection_name=collection_name, schema=schema, shards_num=shard_num)
    logger.info(f"Collection created on primary: {collection_name}")
    for sc in standby_clients:
        wait_for_collection_created(collection_name, sc)

    index_params = default_index_params(primary_client)
    primary_client.create_index(collection_name, index_params=index_params)
    logger.info(f"Index created on primary: {collection_name}")
    for sc in standby_clients:
        wait_for_index_created(collection_name, sc)

    primary_client.load_collection(collection_name, replica_number=replica_num)
    logger.info(f"Collection loaded on primary: {collection_name}")
    for sc in standby_clients:
        wait_for_collection_loaded(collection_name, sc)


def insert_and_verify(collection_name, primary_client, standby_clients,
                      start_id=1, count=None):
    """Insert data on primary and verify replication to standby(s). Returns next start_id."""
    from common.schema import generate_data
    from common.constants import INSERT_COUNT

    if count is None:
        count = INSERT_COUNT
    if not isinstance(standby_clients, (list, tuple)):
        standby_clients = [standby_clients]

    data = generate_data(count, start_id=start_id)
    primary_client.insert(collection_name, data)
    logger.info(f"Inserted {count} rows on primary, start_id={start_id}")

    res_primary = query_all(primary_client, collection_name)
    for sc in standby_clients:
        wait_for_query_consistent(collection_name, res_primary, sc)

    return start_id + count


def cleanup_collection(collection_name, primary_client, standby_clients):
    """Release + drop collection on primary, wait for standby(s)."""
    if not isinstance(standby_clients, (list, tuple)):
        standby_clients = [standby_clients]

    try:
        primary_client.release_collection(collection_name)
        for sc in standby_clients:
            wait_for_collection_released(collection_name, sc)
    except Exception as e:
        logger.warning(f"Release failed (may already be released): {e}")

    primary_client.drop_collection(collection_name)
    logger.info(f"Collection dropped on primary: {collection_name}")
    for sc in standby_clients:
        wait_for_collection_dropped(collection_name, sc)


def drop_if_exists(client, collection_name, label=""):
    """Safely drop a collection if it exists (release first)."""
    try:
        if client.has_collection(collection_name):
            try:
                client.release_collection(collection_name)
            except Exception:
                pass
            client.drop_collection(collection_name)
            if label:
                logger.info(f"[{label}] dropped: {collection_name}")
    except Exception as e:
        logger.warning(f"[{label}] cleanup warning for {collection_name}: {e}")
