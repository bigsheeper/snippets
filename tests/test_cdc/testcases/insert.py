import random
import time
from loguru import logger
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


def insert_into_primary_multiple_rounds(collection_name, client):
    start_id = 1
    for _ in range(INSERT_ROUNDS):
        insert_into_primary(start_id, collection_name, client)
        start_id += INSERT_COUNT


def insert_into_primary(start_id, collection_name, client):
    data = generate_data(INSERT_COUNT, start_id=start_id, dim=DEFAULT_DIM)
    client.insert(collection_name, data)
    logger.info(f"Inserted {INSERT_COUNT} data into primary, start_id: {start_id}")
    return start_id + INSERT_COUNT


def delete_from_primary(collection_name, delete_expr, client):
    client.delete(
        collection_name=collection_name,
        filter=delete_expr
    )
    logger.info(f"Deleted data from primary, expr: {delete_expr}")


def upsert_into_primary(start_id, collection_name, client):
    data = generate_data(INSERT_COUNT, start_id=start_id, dim=DEFAULT_DIM)
    client.upsert(collection_name, data)
    logger.info(f"Upserted {INSERT_COUNT} data into primary, start_id: {start_id}")
    return start_id + INSERT_COUNT
