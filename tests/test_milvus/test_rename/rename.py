from pymilvus import MilvusClient, connections

fmt = "\n=== {:30} ===\n"
collection_a = "collection_A"
collection_b = "collection_B_5"

connections.connect("default", host="localhost", port="19530")
milvus_client = MilvusClient("http://localhost:19530")

print(fmt.format("Before rename"))
print("All collections:", milvus_client.list_collections())

# 将 collection B 重命名为 collection A
print(fmt.format(f"Renaming {collection_b} to {collection_a}"))
milvus_client.rename_collection(old_name=collection_b, new_name=collection_a)

print(fmt.format("After rename"))
print("All collections:", milvus_client.list_collections())

print(fmt.format("Rename done"))

import time
for i in range(10):
    res = milvus_client.describe_collection(collection_name=collection_a)
    print(res)
    time.sleep(1)
