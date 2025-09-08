import time
from common import *
from collection import *


def default_index_params():
    index_params = primary_client.prepare_index_params()
    index_params.add_index(
        field_name=VECTOR_FIELD_NAME,
        index_type="AUTOINDEX",
        metric_type="COSINE"
    )
    return index_params


def create_index_on_primary(collection_name):
    index_params = default_index_params()
    primary_client.create_index(collection_name, index_params=index_params)
    print(f"Index created on primary, name: {collection_name}")


def create_indexes_on_primary(collection_names):
    for collection_name in collection_names:
        create_index_on_primary(collection_name)


def wait_for_secondary_create_index(collection_name):
    start_time = time.time()
    while True:
        index_info = secondary_client.describe_index(collection_name, index_name=VECTOR_FIELD_NAME)
        if index_info is not None:
            break
        if time.time() - start_time > TIMEOUT:
            raise TimeoutError(
                f"Timeout waiting for index to be created on secondary: {collection_name}")
        time.sleep(1)
    print(f"Index created on secondary, name: {collection_name}")


def wait_for_secondary_create_indexes(collection_names):
    for collection_name in collection_names:
        wait_for_secondary_create_index(collection_name)