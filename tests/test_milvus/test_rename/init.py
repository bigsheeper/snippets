import numpy as np
from pymilvus import MilvusClient

fmt = "\n=== {:30} ===\n"
dim = 8
collection_a = "collection_A"
collection_b = "collection_B"
milvus_client = MilvusClient("http://localhost:19530")
db_prod = "prod"

# 清理已存在的 collection&db
# milvus_client_prod = MilvusClient(uri="http://localhost:19530", db_name=db_prod)
# for col_name in [collection_a, collection_b, "collection_B_5"]:
#     milvus_client.drop_collection(col_name)
#     milvus_client_prod.drop_collection(col_name)
# milvus_client.drop_database(db_prod)

print(fmt.format(f"Creating db {db_prod}"))
milvus_client.create_database(db_prod)
milvus_client_prod = MilvusClient(uri="http://localhost:19530", db_name=db_prod)

# 创建 collection A
print(fmt.format(f"Creating {collection_a}"))
milvus_client.create_collection(collection_a, dim, consistency_level="Strong", metric_type="L2")

print(fmt.format(f"Describe collection {collection_a}"))
res = milvus_client.describe_collection(collection_name=collection_a)
print(res)

# 创建 collection B(default)
print(fmt.format(f"Creating collection_B_5 (default)"))
milvus_client.create_collection("collection_B_5", dim, consistency_level="Strong", metric_type="L2")

print(fmt.format(f"Describe collection collection_B_5"))
res = milvus_client.describe_collection(collection_name="collection_B_5")
print(res)

# 创建 collection B(prod)
print(fmt.format(f"Creating collection_B_5 (prod)"))
milvus_client_prod.create_collection("collection_B_5", dim, consistency_level="Strong", metric_type="L2")

print(fmt.format(f"Describe collection collection_B_5"))
res = milvus_client_prod.describe_collection(collection_name="collection_B_5")
print(res)

# rename collection B's db
# print(fmt.format(f"Renaming collection B's db"))
# from pymilvus import utility, connections
# connections.connect(db_name=db_prod, host="localhost", port="19530")
# utility.rename_collection(old_collection_name=collection_b, new_collection_name=collection_b, new_db_name="default")

# print(fmt.format(f"Renaming collection_B to collection_B_1"))
# milvus_client.rename_collection(old_name="collection_B", new_name="collection_B_1")

# print(fmt.format(f"Renaming collection_B_1 to collection_B_2"))
# milvus_client.rename_collection(old_name="collection_B_1", new_name="collection_B_2")

# print(fmt.format(f"Renaming collection_B_2 to collection_B_3"))
# milvus_client.rename_collection(old_name="collection_B_2", new_name="collection_B_3")

# print(fmt.format(f"Renaming collection_B_3 to collection_B_4"))
# milvus_client.rename_collection(old_name="collection_B_3", new_name="collection_B_4")

# print(fmt.format(f"Renaming collection_B_4 to collection_B_5"))
# milvus_client.rename_collection(old_name="collection_B_4", new_name="collection_B_5")

print(fmt.format("    all collections    "))
print(milvus_client.list_collections())

# # 为 collection B 插入数据
# rng = np.random.default_rng(seed=19530)
# rows = [
#     {"id": 1, "vector": rng.random((1, dim))[0], "a": 100},
#     {"id": 2, "vector": rng.random((1, dim))[0], "b": 200},
#     {"id": 3, "vector": rng.random((1, dim))[0], "c": 300},
# ]

# print(fmt.format(f"Inserting entities to {collection_b}"))
# insert_result = milvus_client.insert(collection_b, rows, progress_bar=True)
# print(insert_result)

# print(fmt.format("Start flush"))
# milvus_client.flush(collection_b)
# print(fmt.format("flush done"))

import time
time.sleep(1)
# Drop collection A
print(fmt.format(f"Dropping {collection_a}"))
milvus_client.drop_collection(collection_a)

print(fmt.format("    all collections    "))
print(milvus_client.list_collections())

res = milvus_client.describe_collection(collection_name="collection_B_5")
print(fmt.format(f"Describe collection collection_B_5"))
print(res)

print(fmt.format("Init done"))
