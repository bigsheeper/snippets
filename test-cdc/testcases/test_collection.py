from common import *
from collection import *
from index import *
from insert import *


def test_collection():
    collection_names = get_collection_names()

    create_collections_on_primary(collection_names)
    wait_for_secondary_create_collections(collection_names)

    create_indexes_on_primary(collection_names)
    wait_for_secondary_create_indexes(collection_names)

    load_collections_on_primary(collection_names)
    wait_for_secondary_load_collections(collection_names)

    # release_collections_on_primary(collection_names)
    # wait_for_secondary_release_collections(collection_names)

    drop_collections_on_primary(collection_names)
    wait_for_secondary_drop_collections(collection_names)


if __name__ == "__main__":
    test_collection()
