# Partition Cross-Cluster Replication Test Guide

This guide walks you through verifying the full partition replication lifecycle between two Milvus clusters: create, insert, load, query, release, and drop -- every operation on the primary cluster (A) is automatically replicated to the standby cluster (B).

## Prerequisites

- Two Milvus clusters up and running (this guide uses a local deployment as an example)
  - Cluster A: `localhost:19530` (primary)
  - Cluster B: `localhost:19531` (standby)
- Python 3.10+, with `pymilvus` installed:
  ```bash
  pip install pymilvus
  ```

## Step 1: Connect to Both Clusters

```python
from pymilvus import MilvusClient

client_a = MilvusClient(uri="http://localhost:19530", token="root:Milvus")
client_b = MilvusClient(uri="http://localhost:19531", token="root:Milvus")
```

## Step 2: Set Up Replication (A -> B)

Configure both clusters so that A is the primary and B is the standby. All operations on A will be automatically replicated to B.

> `pchannels` must match the actual pchannel count of your clusters (default: 16). `cluster_id` must match the `msgChannel.chanNamePrefix.cluster` setting in each cluster's configuration.

```python
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

# Update the standby first, then the primary
client_b.update_replicate_configuration(**replicate_config)
client_a.update_replicate_configuration(**replicate_config)
print("Replication established: A -> B")
```

## Step 3: Create a Collection on A

```python
from pymilvus import DataType

schema = MilvusClient.create_schema(auto_id=False, enable_dynamic_field=False)
schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
schema.add_field(field_name="vector", datatype=DataType.FLOAT_VECTOR, dim=4)

collection_name = "test_partition_replication"

# Clean up leftovers from previous runs (if any)
if client_a.has_collection(collection_name):
    client_a.drop_collection(collection_name)

client_a.create_collection(collection_name=collection_name, schema=schema)
print(f"Collection created: {collection_name}")
```

**Verify replication to B:**

```python
import time

start = time.time()
while not client_b.has_collection(collection_name):
    if time.time() - start > 60:
        raise TimeoutError("Collection did not appear on B")
    time.sleep(1)
print("B synced: collection created successfully")
```

## Step 4: Create an Index and a Partition on A

```python
# Create an index (required before loading)
index_params = client_a.prepare_index_params()
index_params.add_index(field_name="vector", index_type="AUTOINDEX", metric_type="COSINE")
client_a.create_index(collection_name, index_params=index_params)
print("Index created")

# Create a partition
partition_name = "region_asia"
client_a.create_partition(collection_name=collection_name, partition_name=partition_name)
print(f"Partition created: {partition_name}")
```

**Verify replication to B:**

```python
start = time.time()
while not client_b.has_partition(collection_name=collection_name, partition_name=partition_name):
    if time.time() - start > 60:
        raise TimeoutError("Partition did not appear on B")
    time.sleep(1)
print("B synced: partition created successfully")
```

## Step 5: Insert Data into the Partition

```python
import random

data = [
    {"id": i, "vector": [random.uniform(-1, 1) for _ in range(4)]}
    for i in range(1, 301)  # 300 rows, id from 1 to 300
]

client_a.insert(
    collection_name=collection_name,
    partition_name=partition_name,
    data=data,
)
print(f"Inserted {len(data)} rows into partition '{partition_name}'")
```

## Step 6: Load the Partition and Query

```python
# Load the partition
client_a.load_partitions(collection_name=collection_name, partition_names=[partition_name])
print("Partition loaded")
```

**Wait for the partition to be loaded on B:**

```python
from pymilvus.client.types import LoadState

start = time.time()
while True:
    state = client_b.get_load_state(collection_name=collection_name, partition_name=partition_name)
    if state["state"] == LoadState.Loaded:
        break
    if time.time() - start > 60:
        raise TimeoutError("Partition not loaded on B")
    time.sleep(1)
print("B synced: partition loaded")
```

**Query both clusters and compare results:**

```python
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

assert ids_a == ids_b, f"Data inconsistency! A has {len(ids_a)} rows, B has {len(ids_b)} rows"
print(f"Data consistency verified: both A and B have {len(ids_a)} rows")
```

## Step 7: Release the Partition

```python
client_a.release_partitions(collection_name=collection_name, partition_names=[partition_name])
print("Partition released on A")

start = time.time()
while True:
    state = client_b.get_load_state(collection_name=collection_name, partition_name=partition_name)
    if state["state"] == LoadState.NotLoad:
        break
    if time.time() - start > 60:
        raise TimeoutError("Partition not released on B")
    time.sleep(1)
print("B synced: partition released")
```

## Step 8: Drop the Partition

```python
client_a.drop_partition(collection_name=collection_name, partition_name=partition_name)
print("Partition dropped on A")

start = time.time()
while not client_b.has_partition(collection_name=collection_name, partition_name=partition_name) == False:
    if time.time() - start > 60:
        raise TimeoutError("Partition not dropped on B")
    time.sleep(1)
print("B synced: partition dropped")
```

## Step 9: Drop the Collection (Cleanup)

```python
client_a.drop_collection(collection_name)
print("Collection dropped on A")

start = time.time()
while client_b.has_collection(collection_name):
    if time.time() - start > 60:
        raise TimeoutError("Collection not dropped on B")
    time.sleep(1)
print("B synced: collection dropped")
print("All tests passed!")
```

## Summary

```
A (Primary)                             B (Standby)
─────────────────────────────────────────────────────
1. Set up replication A -> B            Receives config
2. create_collection                    -> auto-synced
3. create_index                         -> auto-synced
4. create_partition                     -> auto-synced
5. insert 300 rows into partition       -> auto-synced
6. load_partitions                      -> auto-synced
7. query -> 300 rows                    query -> 300 rows ✓
8. release_partitions                   -> auto-synced
9. drop_partition                       -> auto-synced
10. drop_collection                     -> auto-synced
```

Every operation only needs to be performed on the primary cluster A. The standby cluster B automatically receives and replays all changes via CDC.
