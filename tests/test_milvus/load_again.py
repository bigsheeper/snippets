import time
import numpy as np
from pymilvus import (
    MilvusClient,
)
from loguru import logger

fmt = "\n=== {:30} ===\n"
dim = 8
collection_name = "hello_milvus"
milvus_client = MilvusClient("http://localhost:19530")

has_collection = milvus_client.has_collection(collection_name, timeout=5)
if has_collection:
    milvus_client.drop_collection(collection_name)
milvus_client.create_collection(collection_name, dim, shards_num=2, consistency_level="Bounded", metric_type="L2")

logger.info(fmt.format("    all collections    "))
logger.info(milvus_client.list_collections())

logger.info(fmt.format(f"schema of collection {collection_name}"))
logger.info(milvus_client.describe_collection(collection_name))

milvus_client.release_collection(collection_name)
logger.info(fmt.format("release collection done"))

rng = np.random.default_rng(seed=19530)
rows = [
        {"id": 1, "vector": rng.random((1, dim))[0], "a": 100},
        {"id": 2, "vector": rng.random((1, dim))[0], "b": 200},
        {"id": 3, "vector": rng.random((1, dim))[0], "c": 300},
        {"id": 4, "vector": rng.random((1, dim))[0], "d": 400},
        {"id": 5, "vector": rng.random((1, dim))[0], "e": 500},
        {"id": 6, "vector": rng.random((1, dim))[0], "f": 600},
]

logger.info(fmt.format("Start inserting entities"))
insert_result = milvus_client.insert(collection_name, rows, progress_bar=True)
logger.info(fmt.format("Inserting entities done"))
logger.info(insert_result)

logger.info(fmt.format("Start flush"))
milvus_client.flush(collection_name)
logger.info(fmt.format("flush done"))

milvus_client.load_collection(collection_name)
logger.info(fmt.format("load collection done"))

logger.info(fmt.format("Start query by specifying primary keys"))
query_results = milvus_client.query(collection_name, ids=[2])
logger.info(query_results[0])

for i in range(1000):
    for j in range(1):
        milvus_client.release_collection(collection_name)
        logger.info(fmt.format("release done"))

    for k in range(3):
        milvus_client.load_collection(collection_name)
        logger.info(fmt.format("load done"))

    logger.info(fmt.format("Start query by specifying primary keys"))
    query_results = milvus_client.query(collection_name, ids=[2])
    logger.info(query_results[0])

milvus_client.drop_collection(collection_name)
