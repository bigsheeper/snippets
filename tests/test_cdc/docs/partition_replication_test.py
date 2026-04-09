from pymilvus import MilvusClient

client_a = MilvusClient(uri="http://localhost:19530", token="root:Milvus")
client_b = MilvusClient(uri="http://localhost:19531", token="root:Milvus")

replicate_config = {
    "clusters": [
        {
            "cluster_id": "by-dev1",
            "connection_param": {"uri": "http://localhost:19530", "token": "root:Milvus"},
            "pchannels": [f"by-dev1-rootcoord-dml_{i}" for i in range(16)],
        },
        {
            "cluster_id": "by-dev2",
            "connection_param": {"uri": "http://localhost:19531", "token": "root:Milvus"},
            "pchannels": [f"by-dev2-rootcoord-dml_{i}" for i in range(16)],
        },
    ],
    "cross_cluster_topology": [
        {"source_cluster_id": "by-dev1", "target_cluster_id": "by-dev2"}
    ],
}

client_b.update_replicate_configuration(**replicate_config)
client_a.update_replicate_configuration(**replicate_config)
print("复制关系已建立：A → B")

from pymilvus import DataType

schema = MilvusClient.create_schema(auto_id=False, enable_dynamic_field=False)
schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
schema.add_field(field_name="vector", datatype=DataType.FLOAT_VECTOR, dim=4)

collection_name = "test_partition_replication"

# 清理上次残留（如有）
if client_a.has_collection(collection_name):
    client_a.drop_collection(collection_name)

client_a.create_collection(collection_name=collection_name, schema=schema)
print(f"Collection 已创建：{collection_name}")

import time

start = time.time()
while not client_b.has_collection(collection_name):
    if time.time() - start > 60:
        raise TimeoutError("B 上未出现 collection")
    time.sleep(1)
print("B 已同步：collection 创建成功")

# 创建索引（复制到 B 之前需要）
index_params = client_a.prepare_index_params()
index_params.add_index(field_name="vector", index_type="AUTOINDEX", metric_type="COSINE")
client_a.create_index(collection_name, index_params=index_params)
print("索引已创建")

# 创建 Partition
partition_name = "region_asia"
client_a.create_partition(collection_name=collection_name, partition_name=partition_name)
print(f"Partition 已创建：{partition_name}")

start = time.time()
while not client_b.has_partition(collection_name=collection_name, partition_name=partition_name):
    if time.time() - start > 60:
        raise TimeoutError("B 上未出现 partition")
    time.sleep(1)
print("B 已同步：partition 创建成功")

import random

data = [
    {"id": i, "vector": [random.uniform(-1, 1) for _ in range(4)]}
    for i in range(1, 301)  # 300 条数据，id 从 1 到 300
]

client_a.insert(
    collection_name=collection_name,
    partition_name=partition_name,
    data=data,
)
print(f"已写入 {len(data)} 条数据到 partition '{partition_name}'")

# 加载 partition
client_a.load_partitions(collection_name=collection_name, partition_names=[partition_name])
print("Partition 已加载")

from pymilvus.client.types import LoadState

start = time.time()
while True:
    state = client_b.get_load_state(collection_name=collection_name, partition_name=partition_name)
    if state["state"] == LoadState.Loaded:
        break
    if time.time() - start > 60:
        raise TimeoutError("B 上 partition 未加载完成")
    time.sleep(1)
print("B 已同步：partition 加载完成")

query_filter = "id >= 0"

res_a = client_a.query(
    collection_name=collection_name,
    consistency_level="Strong",
    filter=query_filter,
    output_fields=["id"],
)

res_b = client_b.query(
    collection_name=collection_name,
    consistency_level="Strong",
    filter=query_filter,
    output_fields=["id"],
)

ids_a = sorted([r["id"] for r in res_a])
ids_b = sorted([r["id"] for r in res_b])

assert ids_a == ids_b, f"数据不一致！A 有 {len(ids_a)} 条，B 有 {len(ids_b)} 条"
print(f"数据一致性校验通过：A 和 B 均有 {len(ids_a)} 条数据")

client_a.release_partitions(collection_name=collection_name, partition_names=[partition_name])
print("A 上 partition 已释放")

start = time.time()
while True:
    state = client_b.get_load_state(collection_name=collection_name, partition_name=partition_name)
    if state["state"] == LoadState.NotLoad:
        break
    if time.time() - start > 60:
        raise TimeoutError("B 上 partition 未释放")
    time.sleep(1)
print("B 已同步：partition 释放完成")

client_a.drop_partition(collection_name=collection_name, partition_name=partition_name)
print("A 上 partition 已删除")

start = time.time()
while not client_b.has_partition(collection_name=collection_name, partition_name=partition_name) == False:
    if time.time() - start > 60:
        raise TimeoutError("B 上 partition 未删除")
    time.sleep(1)
print("B 已同步：partition 删除完成")

client_a.drop_collection(collection_name)
print("A 上 collection 已删除")

start = time.time()
while client_b.has_collection(collection_name):
    if time.time() - start > 60:
        raise TimeoutError("B 上 collection 未删除")
    time.sleep(1)
print("B 已同步：collection 删除完成")
print("全部测试通过！")
