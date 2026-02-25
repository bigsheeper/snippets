"""
Collection CRUD and wait helpers for CDC mTLS E2E tests.
Supports multiple standby clusters (star topology: 1 primary → N standbys).
"""
import random
import time
from loguru import logger
from pymilvus import MilvusClient, DataType
from pymilvus.client.types import LoadState
from common import TIMEOUT

# Schema constants
DEFAULT_DIM = 4
VECTOR_FIELD_NAME = "vector"
PK_FIELD_NAME = "id"
INSERT_COUNT = 100


def create_collection_schema():
    """Create a simple schema with INT64 PK + FLOAT_VECTOR."""
    schema = MilvusClient.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field(field_name=PK_FIELD_NAME, datatype=DataType.INT64, is_primary=True)
    schema.add_field(field_name=VECTOR_FIELD_NAME, datatype=DataType.FLOAT_VECTOR, dim=DEFAULT_DIM)
    return schema


def create_index_params(client):
    """Create AUTOINDEX with COSINE metric."""
    index_params = client.prepare_index_params()
    index_params.add_index(
        field_name=VECTOR_FIELD_NAME,
        index_type="AUTOINDEX",
        metric_type="COSINE",
    )
    return index_params


def generate_data(n, start_id, dim=DEFAULT_DIM):
    """Generate n rows of test data starting from start_id."""
    return [
        {PK_FIELD_NAME: start_id + i, VECTOR_FIELD_NAME: [random.uniform(-1, 1) for _ in range(dim)]}
        for i in range(n)
    ]


def setup_collection(collection_name, primary_client, standby_clients, shard_num=1):
    """Create collection, index, and load on primary. Wait for replication to all standbys."""
    schema = create_collection_schema()
    primary_client.create_collection(collection_name=collection_name, schema=schema, shards_num=shard_num)
    logger.info(f"Collection created on primary: {collection_name}")
    for sc in standby_clients:
        wait_for_collection_created(collection_name, sc)

    index_params = create_index_params(primary_client)
    primary_client.create_index(collection_name, index_params=index_params)
    logger.info(f"Index created on primary: {collection_name}")
    for sc in standby_clients:
        wait_for_index_created(collection_name, sc)

    primary_client.load_collection(collection_name)
    logger.info(f"Collection loaded on primary: {collection_name}")
    for sc in standby_clients:
        wait_for_collection_loaded(collection_name, sc)


def insert_and_verify(collection_name, primary_client, standby_clients,
                      start_id=1, count=INSERT_COUNT):
    """Insert data on primary and verify it replicates to all standbys."""
    data = generate_data(count, start_id=start_id)
    primary_client.insert(collection_name, data)
    logger.info(f"Inserted {count} rows on primary, start_id={start_id}")

    # Query primary for ground truth
    res_primary = primary_client.query(
        collection_name=collection_name,
        consistency_level="Strong",
        filter=f"{PK_FIELD_NAME} >= 0",
        output_fields=[PK_FIELD_NAME],
    )

    # Wait for all standbys to catch up
    for sc in standby_clients:
        wait_for_query_consistent(collection_name, res_primary, sc)

    return start_id + count


def cleanup_collection(collection_name, primary_client, standby_clients):
    """Release and drop collection on primary, verify propagation to all standbys."""
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


# ---- Wait helpers ----

def wait_for_collection_created(name, client):
    """Poll until collection exists on standby."""
    start = time.time()
    while True:
        if client.has_collection(name):
            break
        if time.time() - start > TIMEOUT:
            raise TimeoutError(f"Timeout waiting for collection created on standby: {name}")
        time.sleep(1)
    logger.info(f"Collection created on standby: {name}")


def wait_for_index_created(name, client):
    """Poll until index exists on standby."""
    start = time.time()
    while True:
        info = client.describe_index(name, index_name=VECTOR_FIELD_NAME)
        if info is not None:
            break
        if time.time() - start > TIMEOUT:
            raise TimeoutError(f"Timeout waiting for index created on standby: {name}")
        time.sleep(1)
    logger.info(f"Index created on standby: {name}")


def wait_for_collection_loaded(name, client):
    """Poll until collection is loaded on standby."""
    start = time.time()
    while True:
        state = client.get_load_state(collection_name=name)
        if state["state"] == LoadState.Loaded:
            break
        if time.time() - start > TIMEOUT:
            raise TimeoutError(f"Timeout waiting for collection loaded on standby: {name}")
        time.sleep(1)
    logger.info(f"Collection loaded on standby: {name}")


def wait_for_collection_released(name, client):
    """Poll until collection is released on standby."""
    start = time.time()
    while True:
        state = client.get_load_state(collection_name=name)
        if state["state"] == LoadState.NotLoad:
            break
        if time.time() - start > TIMEOUT:
            raise TimeoutError(f"Timeout waiting for collection released on standby: {name}")
        time.sleep(1)
    logger.info(f"Collection released on standby: {name}")


def wait_for_collection_dropped(name, client):
    """Poll until collection no longer exists on standby."""
    start = time.time()
    while True:
        if not client.has_collection(name):
            break
        if time.time() - start > TIMEOUT:
            raise TimeoutError(f"Timeout waiting for collection dropped on standby: {name}")
        time.sleep(1)
    logger.info(f"Collection dropped on standby: {name}")


def wait_for_query_consistent(name, res_primary, client):
    """Poll until standby query result matches primary."""
    start = time.time()
    while True:
        res = client.query(
            collection_name=name,
            consistency_level="Strong",
            filter=f"{PK_FIELD_NAME} >= 0",
            output_fields=[PK_FIELD_NAME],
        )
        if len(res) == len(res_primary):
            ids_p = sorted([r[PK_FIELD_NAME] for r in res_primary])
            ids_s = sorted([r[PK_FIELD_NAME] for r in res])
            if ids_p == ids_s:
                break
        else:
            logger.warning(
                f"Query count mismatch: primary={len(res_primary)}, standby={len(res)}"
            )
        if time.time() - start > TIMEOUT:
            raise TimeoutError(f"Timeout waiting for query consistency on standby: {name}")
        time.sleep(1)
    logger.info(f"Query consistent on standby: {name}, count={len(res_primary)}")
