import time
from loguru import logger
from common import *
from collection import *


def default_index_params():
    index_params = cluster_A_client.prepare_index_params()
    index_params.add_index(
        field_name=VECTOR_FIELD_NAME,
        index_type="AUTOINDEX",
        metric_type="COSINE"
    )
    return index_params


def create_index_on_primary(collection_name, client):
    index_params = default_index_params()
    client.create_index(collection_name, index_params=index_params)
    logger.info(f"Index created on primary, name: {collection_name}")


def create_indexes_on_primary(collection_names, client):
    for collection_name in collection_names:
        create_index_on_primary(collection_name, client)


def wait_for_standby_create_index(collection_name, client):
    start_time = time.time()
    while True:
        index_info = client.describe_index(collection_name, index_name=VECTOR_FIELD_NAME)
        if index_info is not None:
            break
        if time.time() - start_time > TIMEOUT:
            error_msg = f"Timeout waiting for index to be created on standby: {collection_name}"
            logger.error(error_msg)
            raise TimeoutError(error_msg)
        time.sleep(1)
    logger.info(f"Index created on standby, name: {collection_name}")


def wait_for_standby_create_indexes(collection_names, client):
    for collection_name in collection_names:
        wait_for_standby_create_index(collection_name, client)