from common import *
from collection import *
from index import *
from insert import *


def test_collection():
    primary_client = cluster_A_client
    standby_client = cluster_B_client
    
    collection_names = get_collection_names()

    create_collections_on_primary(collection_names, primary_client)
    wait_for_standby_create_collections(collection_names, standby_client)

    create_indexes_on_primary(collection_names, primary_client)
    wait_for_standby_create_indexes(collection_names, standby_client)

    load_collections_on_primary(collection_names, primary_client)
    wait_for_standby_load_collections(collection_names, standby_client)

    release_collections_on_primary(collection_names, primary_client)
    wait_for_standby_release_collections(collection_names, standby_client)

    drop_collections_on_primary(collection_names, primary_client)
    wait_for_standby_drop_collections(collection_names, standby_client)


if __name__ == "__main__":
    test_collection()
