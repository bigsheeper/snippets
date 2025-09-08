import time
from loguru import logger
from pymilvus import MilvusClient, DataType
from pymilvus.client.types import LoadState
from common import *

# Collection related constants
DEFAULT_COLLECTION_NAME = "test_collection"
COLLECTION_NAME_PREFIX = "collection_"
NUM_COLLECTIONS = 10

# Schema related constants
DEFAULT_DIM = 4
VECTOR_FIELD_NAME = "vector"
PK_FIELD_NAME = "id"


def create_collection_schema():
    schema = MilvusClient.create_schema(
        auto_id=False,
        enable_dynamic_field=False,
    )
    schema.add_field(field_name=PK_FIELD_NAME,
                     datatype=DataType.INT64, is_primary=True)
    schema.add_field(field_name=VECTOR_FIELD_NAME,
                     datatype=DataType.FLOAT_VECTOR, dim=DEFAULT_DIM)
    return schema


def create_collection_on_primary(collection_name):
    schema = create_collection_schema()
    primary_client.create_collection(
        collection_name=collection_name,
        schema=schema,
    )
    logger.info(f"Collection created on primary, name: {collection_name}")


def create_collections_on_primary(collection_names):
    for collection_name in collection_names:
        create_collection_on_primary(collection_name)


def load_collection_on_primary(collection_name):
    primary_client.load_collection(collection_name)
    logger.info(f"Collection loaded on primary, name: {collection_name}")


def load_collections_on_primary(collection_names):
    for collection_name in collection_names:
        load_collection_on_primary(collection_name)


def release_collection_on_primary(collection_name):
    primary_client.release_collection(collection_name)
    logger.info(f"Collection released on primary, name: {collection_name}")


def release_collections_on_primary(collection_names):
    for collection_name in collection_names:
        release_collection_on_primary(collection_name)


def drop_collection_on_primary(collection_name):
    primary_client.drop_collection(collection_name)
    logger.info(f"Collection dropped on primary, name: {collection_name}")


def drop_collections_on_primary(collection_names):
    for collection_name in collection_names:
        drop_collection_on_primary(collection_name)


def get_collection_name(i, prefix=COLLECTION_NAME_PREFIX):
    return f"{prefix}{i}"


def get_collection_names(num_collections=NUM_COLLECTIONS, prefix=COLLECTION_NAME_PREFIX):
    return [get_collection_name(i, prefix) for i in range(num_collections)]


def wait_for_secondary_create_collection(collection_name):
    start_time = time.time()
    while True:
        has_collection = secondary_client.has_collection(collection_name)
        if has_collection:
            break
        if time.time() - start_time > TIMEOUT:
            error_msg = f"Timeout waiting for collection to be created on secondary: {collection_name}"
            logger.error(error_msg)
            raise TimeoutError(error_msg)
        time.sleep(1)
    logger.info(f"Collection created on secondary, name: {collection_name}")


def wait_for_secondary_load_collection(collection_name):
    start_time = time.time()
    while True:
        load_state = secondary_client.get_load_state(
            collection_name=collection_name)
        if load_state['state'] == LoadState.Loaded:
            break
        if time.time() - start_time > TIMEOUT:
            error_msg = f"Timeout waiting for collection to be loaded on secondary: {collection_name}"
            logger.error(error_msg)
            raise TimeoutError(error_msg)
        time.sleep(1)
    logger.info(f"Collection loaded on secondary, name: {collection_name}")


def wait_for_secondary_release_collection(collection_name):
    start_time = time.time()
    while True:
        load_state = secondary_client.get_load_state(
            collection_name=collection_name)
        if load_state['state'] == LoadState.NotLoad:
            break
        if time.time() - start_time > TIMEOUT:
            error_msg = f"Timeout waiting for collection to be released on secondary: {collection_name}"
            logger.error(error_msg)
            raise TimeoutError(error_msg)
        time.sleep(1)
    logger.info(f"Collection released on secondary, name: {collection_name}")


def wait_for_secondary_drop_collection(collection_name):
    start_time = time.time()
    while True:
        has_collection = secondary_client.has_collection(collection_name)
        if not has_collection:
            break
        if time.time() - start_time > TIMEOUT:
            error_msg = f"Timeout waiting for collection to be dropped on secondary: {collection_name}"
            logger.error(error_msg)
            raise TimeoutError(error_msg)
        time.sleep(1)
    logger.info(f"Collection dropped on secondary, name: {collection_name}")


def wait_for_secondary_create_collections(collection_names):
    for collection_name in collection_names:
        wait_for_secondary_create_collection(collection_name)


def wait_for_secondary_load_collections(collection_names):
    for collection_name in collection_names:
        wait_for_secondary_load_collection(collection_name)


def wait_for_secondary_release_collections(collection_names):
    for collection_name in collection_names:
        wait_for_secondary_release_collection(collection_name)


def wait_for_secondary_drop_collections(collection_names):
    for collection_name in collection_names:
        wait_for_secondary_drop_collection(collection_name)
