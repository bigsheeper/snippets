import random
import time
from common import *
from collection import *

# Insert/Delete related constants
INSERT_COUNT = 1000
INSERT_ROUNDS = 100
DELETE_COUNT = 2000


def generate_data(n, start_id, dim=DEFAULT_DIM):
    vectors = []
    for i in range(n):
        vec = [random.uniform(-1, 1) for _ in range(dim)]
        vectors.append({PK_FIELD_NAME: start_id + i, VECTOR_FIELD_NAME: vec})
    return vectors


def insert_into_primary(collection_name):
    current_id = 1
    for _ in range(INSERT_ROUNDS):
        data = generate_data(INSERT_COUNT, start_id=current_id, dim=DEFAULT_DIM)
        primary_client.insert(collection_name, data)
        print(f"Inserted {INSERT_COUNT} data into primary")
        current_id += INSERT_COUNT


def delete_from_primary(collection_name):
    delete_expr = f"{PK_FIELD_NAME} <= {DELETE_COUNT}"
    primary_client.delete(
        collection_name=collection_name,
        filter=delete_expr
    )
    print(f"Deleted data from primary, expr: {delete_expr}")


def query_on_primary(collection_name, expected_count):
    query_expr = f"{PK_FIELD_NAME} >= 0"
    res = primary_client.query(
        collection_name=collection_name,
        consistency_level="Strong",
        filter=query_expr,
        output_fields=[PK_FIELD_NAME]
    )
    if len(res) != expected_count:
        raise ValueError(
            f"Query result count not match on primary: expected={expected_count}, actual={len(res)}")
    return res


def query_on_secondary(collection_name):
    query_expr = f"{PK_FIELD_NAME} >= 0"
    res = secondary_client.query(
        collection_name=collection_name,
        consistency_level="Strong",
        filter=query_expr,
        output_fields=[PK_FIELD_NAME]
    )
    return res


def wait_for_secondary_query(collection_name, res_on_primary):
    start_time = time.time()
    while True:
        res_on_secondary = query_on_secondary(collection_name)
        if len(res_on_secondary) != len(res_on_primary):
            print(
                f"Length not match: primary={len(res_on_primary)}, secondary={len(res_on_secondary)}")
        else:
            ids_primary = sorted([item[PK_FIELD_NAME] for item in res_on_primary])
            ids_secondary = sorted([item[PK_FIELD_NAME] for item in res_on_secondary])
            if ids_primary == ids_secondary:
                break
        time.sleep(1)
        if time.time() - start_time > TIMEOUT:
            raise TimeoutError(
                f"Timeout waiting for query result to be consistent on secondary")
    print(f"Success: Query result consistent between primary and secondary!")
