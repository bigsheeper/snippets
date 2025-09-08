from common import *
from collection import *
from index import *
from insert import *

def test_insert():
    collection_name = DEFAULT_COLLECTION_NAME

    create_collection_on_primary(collection_name)
    wait_for_secondary_create_collection(collection_name)

    create_index_on_primary(collection_name)
    wait_for_secondary_create_index(collection_name)

    load_collection_on_primary(collection_name)
    wait_for_secondary_load_collection(collection_name)

    total_count = INSERT_COUNT * INSERT_ROUNDS
    insert_into_primary(collection_name)
    res_on_primary = query_on_primary(collection_name, total_count)
    wait_for_secondary_query(collection_name, res_on_primary)

    delete_from_primary(collection_name)
    res_on_primary = query_on_primary(collection_name, total_count - DELETE_COUNT)
    wait_for_secondary_query(collection_name, res_on_primary)

    drop_collection_on_primary(collection_name)
    wait_for_secondary_drop_collection(collection_name)


if __name__ == "__main__":
    test_insert()
