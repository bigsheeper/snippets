import time
import numpy as np
from pymilvus import (
    MilvusClient,
)

fmt = "\n=== {:30} ===\n"
milvus_client = MilvusClient("http://localhost:19530")

dim = 8
collection_name_prefix = "collection"
collection_num = 800

for i in range(collection_num):
    collection_name = f"{collection_name_prefix}_{i}"
    # has_collection = milvus_client.has_collection(collection_name, timeout=5)
    # if has_collection:
    #     milvus_client.drop_collection(collection_name)
    milvus_client.create_collection(collection_name, dim, consistency_level="Strong", metric_type="L2")
    print(fmt.format(f"created collection {collection_name}"))

print(fmt.format("    all collections    "))
print(milvus_client.list_collections())
