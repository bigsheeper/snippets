"""Helpers for Import 2PC over replication tests.

All REST calls target cluster A's HTTP endpoint (imports are initiated on
the primary). Cluster B is verified via pymilvus query in the tests
themselves.

Schema matches test_cdc/common/schema.py: INT64 PK 'id' + FLOAT_VECTOR
dim=4 'vector'. No varchar field (upstream test_import_commit_ts uses
dim=128 + varchar; we drop both to avoid schema drift with the rest of
the CDC suite).

The REST URI defaults (19530/19531) assume PROXY_HTTP_PORT is unset and
the proxy multiplexes HTTP on the gRPC port. Override via env vars if
the cluster was started with an explicit PROXY_HTTP_PORT.
"""
import os
import tempfile
import time

import boto3
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from loguru import logger

# --- REST / MinIO config ---
CLUSTER_A_REST_URI = os.getenv("CLUSTER_A_REST_URI", "http://localhost:19530")
CLUSTER_B_REST_URI = os.getenv("CLUSTER_B_REST_URI", "http://localhost:19531")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "a-bucket")
MILVUS_ROOT_PATH = os.getenv("MILVUS_ROOT_PATH", "files")

# --- Schema ---
DIM = 4
RNG = np.random.default_rng(seed=42)


# ---------- REST helpers ----------

def _api(path, payload=None, base_uri=CLUSTER_A_REST_URI):
    """POST to base_uri + /v2/vectordb + path. Raises on non-zero code."""
    url = f"{base_uri}/v2/vectordb{path}"
    resp = requests.post(url, json=payload or {})
    resp.raise_for_status()
    result = resp.json()
    if result.get("code", 0) != 0:
        raise RuntimeError(f"API error ({url}): {result}")
    return result


def import_create_on_a(collection_name, files, auto_commit=False, partition_name=""):
    """Create an import job on cluster A. Returns job_id."""
    payload = {
        "collectionName": collection_name,
        "files": files,
    }
    if partition_name:
        payload["partitionName"] = partition_name
    if not auto_commit:
        payload["options"] = {"auto_commit": "false"}
    result = _api("/jobs/import/create", payload, base_uri=CLUSTER_A_REST_URI)
    job_id = result["data"]["jobId"]
    logger.info(f"Import job created on A: {job_id} (auto_commit={auto_commit})")
    return job_id


def import_get_progress_on_a(job_id):
    """Return (state, progress)."""
    result = _api("/jobs/import/describe", {"jobId": job_id}, base_uri=CLUSTER_A_REST_URI)
    data = result.get("data", {})
    return data.get("state", "Unknown"), data.get("progress", 0)


def import_commit_on_a(job_id):
    _api("/jobs/import/commit", {"jobId": job_id}, base_uri=CLUSTER_A_REST_URI)
    logger.info(f"Import job {job_id} committed on A")


def import_abort_on_a(job_id):
    _api("/jobs/import/abort", {"jobId": job_id}, base_uri=CLUSTER_A_REST_URI)
    logger.info(f"Import job {job_id} aborted on A")


def wait_for_import_state_on_a(job_id, target_state, timeout=180):
    """Poll import state on A until target_state reached, or raise."""
    logger.info(f"Waiting for A job {job_id} to reach '{target_state}' (timeout={timeout}s)")
    start = time.time()
    while time.time() - start < timeout:
        state, progress = import_get_progress_on_a(job_id)
        if state == target_state:
            return True
        if state == "Failed":
            raise RuntimeError(f"Import job {job_id} on A failed")
        logger.debug(f"  state={state}, progress={progress}")
        time.sleep(2)
    raise TimeoutError(f"Import {job_id} on A did not reach '{target_state}' in {timeout}s")


def wait_for_import_done_on_a(job_id, timeout=180):
    return wait_for_import_state_on_a(job_id, "Completed", timeout)


# ---------- Parquet generator ----------

def generate_and_upload_parquet(num_rows, start_id=0, prefix="import",
                                step=1, ids=None):
    """Generate parquet with columns (id int64, vector list<float32>) and
    upload to MinIO. Returns the remote key (path inside the bucket).

    Args:
        num_rows: number of rows (ignored if ids provided).
        start_id: starting ID for sequential generation.
        prefix: S3 path prefix under MILVUS_ROOT_PATH.
        step: ID step for sequential generation (e.g. step=2 -> evens only).
        ids: explicit list of IDs; overrides num_rows/start_id/step.
    """
    if ids is not None:
        ids = list(ids)
        num_rows = len(ids)
    else:
        ids = list(range(start_id, start_id + num_rows * step, step))

    vectors = RNG.random((num_rows, DIM), dtype=np.float32)
    vec_arrays = [vectors[i].tolist() for i in range(num_rows)]

    table = pa.table({
        "id": pa.array(ids, type=pa.int64()),
        "vector": pa.array(vec_arrays, type=pa.list_(pa.float32())),
    })

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        pq.write_table(table, f.name)
        local_path = f.name

    # Suffix with timestamp to avoid object collisions across test runs
    suffix = int(time.time() * 1000)
    remote_key = f"{MILVUS_ROOT_PATH}/{prefix}_{start_id}_{num_rows}_{suffix}.parquet"

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    s3.upload_file(local_path, MINIO_BUCKET, remote_key)
    os.unlink(local_path)

    logger.info(f"Uploaded parquet to s3://{MINIO_BUCKET}/{remote_key} ({num_rows} rows)")
    return remote_key


# ---------- SDK insert helper ----------

def insert_rows(client, collection_name, ids):
    """Insert rows with explicit IDs and random vectors via SDK.

    Returns the insert_count from the response.
    """
    vectors = RNG.random((len(ids), DIM), dtype=np.float32)
    data = [
        {"id": id_val, "vector": vectors[i].tolist()}
        for i, id_val in enumerate(ids)
    ]
    result = client.insert(collection_name=collection_name, data=data)
    return result["insert_count"]


# ---------- Count helper (used across tests) ----------

def count_rows(client, collection_name):
    """Return COUNT(*) of rows visible on the given client."""
    res = client.query(
        collection_name=collection_name,
        filter="id >= 0",
        output_fields=["count(*)"],
    )
    return res[0]["count(*)"]
