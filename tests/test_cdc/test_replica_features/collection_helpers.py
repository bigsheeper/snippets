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
    schema = MilvusClient.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field(field_name=PK_FIELD_NAME, datatype=DataType.INT64, is_primary=True)
    schema.add_field(field_name=VECTOR_FIELD_NAME, datatype=DataType.FLOAT_VECTOR, dim=DEFAULT_DIM)
    return schema


def create_index_params(client):
    index_params = client.prepare_index_params()
    index_params.add_index(
        field_name=VECTOR_FIELD_NAME,
        index_type="AUTOINDEX",
        metric_type="COSINE",
    )
    return index_params


def generate_data(n, start_id, dim=DEFAULT_DIM):
    return [
        {PK_FIELD_NAME: start_id + i, VECTOR_FIELD_NAME: [random.uniform(-1, 1) for _ in range(dim)]}
        for i in range(n)
    ]


def setup_collection(collection_name, primary_client, standby_client, replica_num=1):
    """Create collection, index, and load on primary. Wait for replication to standby."""
    schema = create_collection_schema()
    primary_client.create_collection(collection_name=collection_name, schema=schema)
    logger.info(f"Collection created on primary: {collection_name}")
    wait_for_collection_created(collection_name, standby_client)

    index_params = create_index_params(primary_client)
    primary_client.create_index(collection_name, index_params=index_params)
    logger.info(f"Index created on primary: {collection_name}")
    wait_for_index_created(collection_name, standby_client)

    primary_client.load_collection(collection_name, replica_number=replica_num)
    logger.info(f"Collection loaded on primary: {collection_name}, replicas={replica_num}")
    wait_for_collection_loaded(collection_name, standby_client)


def insert_and_verify(collection_name, primary_client, standby_client, start_id=1):
    """Insert data on primary and verify it replicates to standby."""
    data = generate_data(INSERT_COUNT, start_id=start_id)
    primary_client.insert(collection_name, data)
    logger.info(f"Inserted {INSERT_COUNT} rows on primary, start_id={start_id}")

    # Query primary
    res_primary = primary_client.query(
        collection_name=collection_name,
        consistency_level="Strong",
        filter=f"{PK_FIELD_NAME} >= 0",
        output_fields=[PK_FIELD_NAME],
    )

    # Wait for standby to catch up
    wait_for_query_consistent(collection_name, res_primary, standby_client)
    return start_id + INSERT_COUNT


def cleanup_collection(collection_name, primary_client, standby_client):
    """Release and drop collection on primary, wait for standby."""
    try:
        primary_client.release_collection(collection_name)
        wait_for_collection_released(collection_name, standby_client)
    except Exception as e:
        logger.warning(f"Release failed (may already be released): {e}")

    primary_client.drop_collection(collection_name)
    logger.info(f"Collection dropped on primary: {collection_name}")
    wait_for_collection_dropped(collection_name, standby_client)


def get_replica_count(collection_name, client):
    """Get the number of replicas for a loaded collection via describe_replica."""
    try:
        replicas = client.describe_replica(collection_name)
        return len(replicas)
    except Exception as e:
        logger.warning(f"Failed to get replicas for {collection_name}: {e}")
        return -1


# ---- Wait helpers ----

def wait_for_collection_created(collection_name, client):
    start_time = time.time()
    while True:
        if client.has_collection(collection_name):
            break
        if time.time() - start_time > TIMEOUT:
            raise TimeoutError(f"Timeout waiting for collection created on standby: {collection_name}")
        time.sleep(1)
    logger.info(f"Collection created on standby: {collection_name}")


def wait_for_index_created(collection_name, client):
    start_time = time.time()
    while True:
        index_info = client.describe_index(collection_name, index_name=VECTOR_FIELD_NAME)
        if index_info is not None:
            break
        if time.time() - start_time > TIMEOUT:
            raise TimeoutError(f"Timeout waiting for index created on standby: {collection_name}")
        time.sleep(1)
    logger.info(f"Index created on standby: {collection_name}")


def wait_for_collection_loaded(collection_name, client):
    start_time = time.time()
    while True:
        state = client.get_load_state(collection_name=collection_name)
        if state["state"] == LoadState.Loaded:
            break
        if time.time() - start_time > TIMEOUT:
            raise TimeoutError(f"Timeout waiting for collection loaded on standby: {collection_name}")
        time.sleep(1)
    logger.info(f"Collection loaded on standby: {collection_name}")


def wait_for_collection_released(collection_name, client):
    start_time = time.time()
    while True:
        state = client.get_load_state(collection_name=collection_name)
        if state["state"] == LoadState.NotLoad:
            break
        if time.time() - start_time > TIMEOUT:
            raise TimeoutError(f"Timeout waiting for collection released on standby: {collection_name}")
        time.sleep(1)
    logger.info(f"Collection released on standby: {collection_name}")


def wait_for_collection_dropped(collection_name, client):
    start_time = time.time()
    while True:
        if not client.has_collection(collection_name):
            break
        if time.time() - start_time > TIMEOUT:
            raise TimeoutError(f"Timeout waiting for collection dropped on standby: {collection_name}")
        time.sleep(1)
    logger.info(f"Collection dropped on standby: {collection_name}")


def wait_for_query_consistent(collection_name, res_primary, client):
    start_time = time.time()
    while True:
        res_standby = client.query(
            collection_name=collection_name,
            consistency_level="Strong",
            filter=f"{PK_FIELD_NAME} >= 0",
            output_fields=[PK_FIELD_NAME],
        )
        if len(res_standby) == len(res_primary):
            ids_primary = sorted([r[PK_FIELD_NAME] for r in res_primary])
            ids_standby = sorted([r[PK_FIELD_NAME] for r in res_standby])
            if ids_primary == ids_standby:
                break
        else:
            logger.warning(
                f"Query count mismatch: primary={len(res_primary)}, standby={len(res_standby)}"
            )
        if time.time() - start_time > TIMEOUT:
            raise TimeoutError(f"Timeout waiting for query consistency on standby: {collection_name}")
        time.sleep(1)
    logger.info(f"Query consistent on standby: {collection_name}, count={len(res_primary)}")
