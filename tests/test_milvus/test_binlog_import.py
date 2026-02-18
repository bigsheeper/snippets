"""
Test binlog import to verify fix https://github.com/milvus-io/milvus/pull/47276
"""

import time
import numpy as np
import boto3
from pymilvus import connections, Collection, FieldSchema, CollectionSchema, DataType, utility
from pymilvus.bulk_writer import bulk_import, get_import_progress

MILVUS_URI = "http://localhost:19530"
DIM = 128
SOURCE_COLLECTION = "binlog_import_source"
TARGET_COLLECTION = "binlog_import_target"
INSERT_ROWS = 500
DELETE_ROWS = 100
ROOT_PATH = "files"
BUCKET = "a-bucket"


def wait_for_import(job_id, timeout=120):
    print(f"Waiting for import job {job_id}...")
    start = time.time()
    while time.time() - start < timeout:
        resp = get_import_progress(url=MILVUS_URI, job_id=job_id)
        data = resp.json().get("data", {})
        state = data.get("state", "Unknown")
        progress = data.get("progress", 0)
        print(f"  state={state}, progress={progress}")
        if state == "Completed":
            return True
        if state == "Failed":
            print(f"  FAILED: {data.get('reason', 'unknown')}")
            return False
        time.sleep(3)
    print("  Timed out")
    return False


def list_delta_segments(collection_id):
    """List L0 segment dirs from minio."""
    s3 = boto3.client(
        "s3", endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin",
    )
    prefix = f"{ROOT_PATH}/delta_log/{collection_id}/"
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, Delimiter="/")
    delta_files = []
    for part_dir in [cp["Prefix"] for cp in resp.get("CommonPrefixes", [])]:
        resp2 = s3.list_objects_v2(Bucket=BUCKET, Prefix=part_dir, Delimiter="/")
        for seg_dir in [cp["Prefix"] for cp in resp2.get("CommonPrefixes", [])]:
            delta_files.append([seg_dir.rstrip("/")])
    return delta_files


def main():
    connections.connect("default", uri=MILVUS_URI)
    print("Connected to Milvus")

    # === Create source collection ===
    for name in [SOURCE_COLLECTION, TARGET_COLLECTION]:
        if utility.has_collection(name):
            utility.drop_collection(name)

    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name="float_vec", dtype=DataType.FLOAT_VECTOR, dim=DIM),
        FieldSchema(name="varchar", dtype=DataType.VARCHAR, max_length=256),
    ]
    schema = CollectionSchema(fields=fields)
    source_col = Collection(SOURCE_COLLECTION, schema=schema)
    source_col.create_index("float_vec", {"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 128}})
    source_col.load()
    print(f"Created source collection: {SOURCE_COLLECTION}")

    # Insert data
    rng = np.random.default_rng(seed=42)
    vectors = rng.random((INSERT_ROWS, DIM), dtype=np.float32).tolist()
    varchars = [f"row_{i}" for i in range(INSERT_ROWS)]
    result = source_col.insert([vectors, varchars])
    all_ids = result.primary_keys
    print(f"Inserted {INSERT_ROWS} rows")

    # Delete some rows
    ids_to_delete = all_ids[:DELETE_ROWS]
    source_col.delete(f"id in {ids_to_delete}")
    print(f"Deleted {DELETE_ROWS} rows")

    # Flush and wait for segments to appear
    source_col.flush()
    print("Flush requested, waiting for segments...")
    collection_id = source_col.describe()["collection_id"]

    # Wait for segments to show up in query segment info
    segments = []
    for i in range(30):
        source_col.release()
        source_col.load()
        time.sleep(2)
        segments = utility.get_query_segment_info(SOURCE_COLLECTION)
        if segments:
            break
        print(f"  Retry {i+1}: no segments yet...")
    assert segments, "No segments found after flush"

    partition_id = segments[0].partitionID
    l1_seg_ids = [seg.segmentID for seg in segments]
    print(f"collection_id={collection_id}, partition_id={partition_id}, segments={l1_seg_ids}")

    # === Create target collection ===
    target_col = Collection(TARGET_COLLECTION, schema=schema)
    target_col.create_index("float_vec", {"index_type": "IVF_FLAT", "metric_type": "L2", "params": {"nlist": 128}})
    print(f"Created target collection: {TARGET_COLLECTION}")

    # === L1 binlog import ===
    print("\n--- L1 binlog import ---")
    insert_files = [[f"{ROOT_PATH}/insert_log/{collection_id}/{partition_id}/{sid}"] for sid in l1_seg_ids]
    print(f"Files: {insert_files}")
    resp = bulk_import(url=MILVUS_URI, collection_name=TARGET_COLLECTION, files=insert_files,
                       partition_name="_default", options={"backup": "true", "storage_version": "2"})
    job_id = resp.json()["data"]["jobId"]
    assert wait_for_import(job_id), "L1 import failed"

    # === L0 import ===
    print("\n--- L0 import ---")
    delta_files = list_delta_segments(collection_id)
    if delta_files:
        print(f"Delta files: {delta_files}")
        resp = bulk_import(url=MILVUS_URI, collection_name=TARGET_COLLECTION, files=delta_files,
                           options={"l0_import": "true", "storage_version": "2"})
        job_id = resp.json()["data"]["jobId"]
        assert wait_for_import(job_id), "L0 import failed"
    else:
        print("No delta logs found")

    print("\nBinlog import done.")


if __name__ == "__main__":
    main()
