import time
import numpy as np
from pymilvus import (
    MilvusClient,
)

fmt = "\n=== {:30} ===\n"
dim = 1024
num_entities = 1500
collection_name = "hello_milvus"
milvus_client = MilvusClient("http://localhost:19530")

has_collection = milvus_client.has_collection(collection_name, timeout=5)
if has_collection:
    milvus_client.drop_collection(collection_name)
milvus_client.create_collection(collection_name, dim, consistency_level="Strong", metric_type="L2")

print(fmt.format("    all collections    "))
print(milvus_client.list_collections())

print(fmt.format(f"schema of collection {collection_name}"))
print(milvus_client.describe_collection(collection_name))

rng = np.random.default_rng(seed=19530)
rows = []
for i in range(num_entities):
    rows.append({
        "id": i + 1, 
        "vector": rng.random((1, dim))[0], 
        "a": (i + 1) * 100
    })

for i in range(10):
    print(fmt.format("Start inserting entities"))
    start_time = time.time()
    insert_result = milvus_client.insert(collection_name, rows, progress_bar=True)
    end_time = time.time()
    latency_ms = (end_time - start_time) * 1000
    print(fmt.format(f"Inserting entities done, latency: {latency_ms:.2f} ms"))

milvus_client.drop_collection(collection_name)
