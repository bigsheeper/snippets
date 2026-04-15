"""
Shared helpers for import commit_timestamp e2e tests.
Uses MilvusClient API (not deprecated ORM).
"""

import time
import os
import tempfile

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import requests
from pymilvus import MilvusClient, DataType

MILVUS_URI = os.getenv("MILVUS_URI", "http://localhost:19530")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "a-bucket")
MILVUS_ROOT_PATH = os.getenv("MILVUS_ROOT_PATH", "files")

DIM = 128
RNG = np.random.default_rng(seed=42)


def get_client():
    """Create and return a MilvusClient instance."""
    return MilvusClient(uri=MILVUS_URI)


def create_collection(client, name, ttl_seconds=0):
    """Drop if exists, then create collection with standard schema."""
    if client.has_collection(name):
        client.drop_collection(name)

    schema = client.create_schema()
    schema.add_field("id", DataType.INT64, is_primary=True)
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=DIM)
    schema.add_field("varchar", DataType.VARCHAR, max_length=256)

    client.create_collection(collection_name=name, schema=schema)

    if ttl_seconds > 0:
        client.alter_collection_properties(
            collection_name=name,
            properties={"collection.ttl.seconds": str(ttl_seconds)},
        )


def create_index_and_load(client, name):
    """Create HNSW index on vec field and load collection."""
    index_params = client.prepare_index_params()
    index_params.add_index(
        field_name="vec",
        index_type="HNSW",
        metric_type="L2",
        params={"M": 16, "efConstruction": 200},
    )
    client.create_index(collection_name=name, index_params=index_params)
    client.load_collection(collection_name=name)


def create_collection_auto_id(client, name, ttl_seconds=0):
    """Drop if exists, then create collection with auto_id on the primary key."""
    if client.has_collection(name):
        client.drop_collection(name)

    schema = client.create_schema()
    schema.add_field("id", DataType.INT64, is_primary=True, auto_id=True)
    schema.add_field("vec", DataType.FLOAT_VECTOR, dim=DIM)
    schema.add_field("varchar", DataType.VARCHAR, max_length=256)

    client.create_collection(collection_name=name, schema=schema)

    if ttl_seconds > 0:
        client.alter_collection_properties(
            collection_name=name,
            properties={"collection.ttl.seconds": str(ttl_seconds)},
        )


def insert_rows(client, collection_name, ids, varchar_prefix="insert"):
    """Insert rows with explicit IDs via SDK. Returns inserted count."""
    vectors = RNG.random((len(ids), DIM), dtype=np.float32)
    data = [
        {
            "id": id_val,
            "vec": vectors[i].tolist(),
            "varchar": f"{varchar_prefix}_{id_val}",
        }
        for i, id_val in enumerate(ids)
    ]
    result = client.insert(collection_name=collection_name, data=data)
    return result["insert_count"]


def insert_rows_auto_id(client, collection_name, num_rows,
                         varchar_prefix="insert"):
    """Insert rows without ID field (auto-ID). Returns list of auto-generated IDs."""
    vectors = RNG.random((num_rows, DIM), dtype=np.float32)
    data = [
        {
            "vec": vectors[i].tolist(),
            "varchar": f"{varchar_prefix}_{i}",
        }
        for i in range(num_rows)
    ]
    result = client.insert(collection_name=collection_name, data=data)
    return result["ids"]


def generate_and_upload_parquet(num_rows, start_id=0, prefix="import_test",
                               step=1, ids=None, varchar_values=None):
    """Generate parquet file and upload to minio. Returns the remote path.

    Args:
        num_rows: Number of rows (ignored when ids is provided).
        start_id: Starting ID for sequential generation.
        prefix: S3 path prefix.
        step: Step between consecutive IDs (e.g. step=2 -> 0,2,4,...).
        ids: Explicit list of IDs to use. Overrides num_rows/start_id/step.
        varchar_values: Explicit list of varchar values. Must match row count.
    """
    if ids is not None:
        ids = list(ids)
        num_rows = len(ids)
    else:
        ids = list(range(start_id, start_id + num_rows * step, step))

    vectors = RNG.random((num_rows, DIM), dtype=np.float32)

    if varchar_values is not None:
        varchars = list(varchar_values)
    else:
        varchars = [f"row_{i}" for i in ids]

    vec_arrays = [vectors[i].tolist() for i in range(num_rows)]
    table = pa.table({
        "id": pa.array(ids, type=pa.int64()),
        "vec": pa.array(vec_arrays, type=pa.list_(pa.float32())),
        "varchar": pa.array(varchars, type=pa.string()),
    })

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        pq.write_table(table, f.name)
        local_path = f.name

    remote_key = f"{MILVUS_ROOT_PATH}/{prefix}_{start_id}_{num_rows}.parquet"
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    s3.upload_file(local_path, MINIO_BUCKET, remote_key)
    os.unlink(local_path)

    print(f"Uploaded parquet to s3://{MINIO_BUCKET}/{remote_key} ({num_rows} rows)")
    return remote_key


def generate_and_upload_parquet_auto_id(num_rows, varchar_prefix="import",
                                        prefix="import_auto"):
    """Generate parquet WITHOUT id field (for auto-ID collections) and upload.

    Returns the remote S3 path.
    """
    vectors = RNG.random((num_rows, DIM), dtype=np.float32)
    varchars = [f"{varchar_prefix}_{i}" for i in range(num_rows)]

    vec_arrays = [vectors[i].tolist() for i in range(num_rows)]
    table = pa.table({
        "vec": pa.array(vec_arrays, type=pa.list_(pa.float32())),
        "varchar": pa.array(varchars, type=pa.string()),
    })

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        pq.write_table(table, f.name)
        local_path = f.name

    remote_key = f"{MILVUS_ROOT_PATH}/{prefix}_{num_rows}.parquet"
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    s3.upload_file(local_path, MINIO_BUCKET, remote_key)
    os.unlink(local_path)

    print(f"Uploaded auto-id parquet to s3://{MINIO_BUCKET}/{remote_key} ({num_rows} rows)")
    return remote_key


# ---------- REST API helpers for import 2PC ----------

def _api(path, payload=None):
    url = f"{MILVUS_URI}/v2/vectordb{path}"
    resp = requests.post(url, json=payload or {})
    resp.raise_for_status()
    result = resp.json()
    if result.get("code", 0) != 0:
        raise RuntimeError(f"API error: {result}")
    return result


def import_create(collection_name, files, auto_commit=True, partition_name=""):
    """Create an import job. files is list of list of strings."""
    payload = {
        "collectionName": collection_name,
        "files": files,
    }
    if partition_name:
        payload["partitionName"] = partition_name
    if not auto_commit:
        payload["options"] = {"auto_commit": "false"}
    result = _api("/jobs/import/create", payload)
    job_id = result["data"]["jobId"]
    print(f"Import job created: {job_id} (auto_commit={auto_commit})")
    return job_id


def import_get_progress(job_id):
    """Get import job progress. Returns (state, progress_pct)."""
    result = _api("/jobs/import/describe", {"jobId": job_id})
    data = result.get("data", {})
    return data.get("state", "Unknown"), data.get("progress", 0)


def import_commit(job_id):
    """Commit an uncommitted import job."""
    _api("/jobs/import/commit", {"jobId": job_id})
    print(f"Import job {job_id} committed")


def import_abort(job_id):
    """Abort an import job."""
    _api("/jobs/import/abort", {"jobId": job_id})
    print(f"Import job {job_id} aborted")


def wait_for_import_state(job_id, target_state, timeout=120):
    """Wait until import job reaches target_state."""
    print(f"Waiting for job {job_id} to reach state '{target_state}'...")
    start = time.time()
    while time.time() - start < timeout:
        state, progress = import_get_progress(job_id)
        print(f"  state={state}, progress={progress}")
        if state == target_state:
            return True
        if state == "Failed":
            raise RuntimeError(f"Import job {job_id} failed")
        time.sleep(2)
    raise TimeoutError(f"Import job {job_id} did not reach '{target_state}' within {timeout}s")


def wait_for_import_done(job_id, timeout=120):
    """Wait for import to complete."""
    return wait_for_import_state(job_id, "Completed", timeout)
