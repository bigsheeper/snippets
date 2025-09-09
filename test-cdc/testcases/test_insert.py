from common import *
from collection import *
from index import *
from insert import *
from query import *

def test_insert():
    collection_name = DEFAULT_COLLECTION_NAME

    primary_client = cluster_A_client
    standby_client = cluster_B_client

    create_collection_on_primary(collection_name, primary_client)
    wait_for_standby_create_collection(collection_name, standby_client)

    create_index_on_primary(collection_name, primary_client)
    wait_for_standby_create_index(collection_name, standby_client)

    load_collection_on_primary(collection_name, primary_client)
    wait_for_standby_load_collection(collection_name, standby_client)

    total_count = INSERT_COUNT * INSERT_ROUNDS
    insert_into_primary_multiple_rounds(collection_name, primary_client)
    res_on_primary = query_on_primary(collection_name, total_count, primary_client)
    wait_for_standby_query(collection_name, res_on_primary, standby_client)

    delete_expr = f"{PK_FIELD_NAME} <= {DELETE_COUNT}"
    delete_from_primary(collection_name, delete_expr, primary_client)
    res_on_primary = query_on_primary(collection_name, total_count - DELETE_COUNT, primary_client)
    wait_for_standby_query(collection_name, res_on_primary, standby_client)

    release_collection_on_primary(collection_name, primary_client)
    wait_for_standby_release_collection(collection_name, standby_client)

    drop_collection_on_primary(collection_name, primary_client)
    wait_for_standby_drop_collection(collection_name, standby_client)


if __name__ == "__main__":
    test_insert()
