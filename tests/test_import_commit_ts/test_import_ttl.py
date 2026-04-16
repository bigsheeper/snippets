"""
Test: Collection-level TTL with commit_timestamp.

Proves TTL expiration uses commit_ts (not row_ts):
- Import with auto_commit=false, wait 20s in Uncommitted state (row_ts ages)
- Commit → commit_ts = now
- After 15s (35s from row_ts, 15s from commit_ts): data SURVIVES (TTL=30s from commit_ts)
- After 35s from commit_ts: data EXPIRES

If commit_ts were not used, row_ts would be 35s old at step 6, exceeding 30s TTL.
"""

import time
from common import (
    get_client, create_collection,
    generate_and_upload_parquet,
    import_create, import_commit, wait_for_import_state, wait_for_import_done,
)

COLLECTION = "test_import_ttl"
NUM_ROWS = 500
TTL_SECONDS = 30
PRE_COMMIT_WAIT = 20


def query_count(client):
    try:
        results = client.query(collection_name=COLLECTION, filter="id >= 0", output_fields=["count(*)"])
        return results[0]["count(*)"]
    except Exception as e:
        print(f"  Query error (may be expected): {e}")
        return 0


def test_ttl_with_commit_timestamp():
    client = get_client()
    create_collection(client, COLLECTION, ttl_seconds=TTL_SECONDS)

    # Create index upfront
    index_params = client.prepare_index_params()
    index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2",
                           params={"M": 16, "efConstruction": 200})
    client.create_index(collection_name=COLLECTION, index_params=index_params)
    print(f"Collection '{COLLECTION}' created with TTL={TTL_SECONDS}s")

    # Import with auto_commit=false
    remote_path = generate_and_upload_parquet(NUM_ROWS, start_id=0, prefix="ttl")
    job_id = import_create(COLLECTION, files=[[remote_path]], auto_commit=False)

    # Wait until Uncommitted
    wait_for_import_state(job_id, "Uncommitted")
    print("Import reached Uncommitted state")

    # Wait PRE_COMMIT_WAIT seconds — row_ts ages, but commit hasn't happened
    print(f"Waiting {PRE_COMMIT_WAIT}s before commit (aging row_ts)...")
    time.sleep(PRE_COMMIT_WAIT)

    # Commit — commit_ts is set to NOW
    commit_time = time.time()
    import_commit(job_id)
    wait_for_import_done(job_id)
    print(f"Import committed at t={commit_time:.0f}")

    # Load and verify data exists
    client.load_collection(COLLECTION)
    time.sleep(2)
    count = query_count(client)
    assert count == NUM_ROWS, \
        f"[FAIL] Expected {NUM_ROWS} rows immediately after commit, got {count}"
    print(f"[PASS] Immediately after commit: {count} rows")

    # ---- Check at 15s after commit ----
    elapsed = time.time() - commit_time
    sleep_to_15s = max(0, 15 - elapsed)
    if sleep_to_15s > 0:
        print(f"Sleeping {sleep_to_15s:.0f}s to reach 15s after commit...")
        time.sleep(sleep_to_15s)

    # Re-load to ensure fresh state
    client.release_collection(COLLECTION)
    client.load_collection(COLLECTION)
    time.sleep(2)

    count_at_15s = query_count(client)
    elapsed_from_commit = time.time() - commit_time
    print(f"  At {elapsed_from_commit:.0f}s after commit (row_ts ~{PRE_COMMIT_WAIT + elapsed_from_commit:.0f}s old)")
    assert count_at_15s == NUM_ROWS, \
        f"[FAIL] Expected {NUM_ROWS} rows at 15s after commit (TTL should use commit_ts), got {count_at_15s}"
    print(f"[PASS] At ~15s after commit: {count_at_15s} rows (TTL uses commit_ts, not row_ts)")

    # ---- Check at 35s after commit ----
    elapsed = time.time() - commit_time
    sleep_to_35s = max(0, 35 - elapsed)
    if sleep_to_35s > 0:
        print(f"Sleeping {sleep_to_35s:.0f}s to reach 35s after commit...")
        time.sleep(sleep_to_35s)

    # Trigger compaction to apply TTL GC
    client.compact(COLLECTION)
    time.sleep(5)

    client.release_collection(COLLECTION)
    client.load_collection(COLLECTION)
    time.sleep(2)

    count_at_35s = query_count(client)
    elapsed_from_commit = time.time() - commit_time
    print(f"  At {elapsed_from_commit:.0f}s after commit")
    assert count_at_35s == 0, \
        f"[FAIL] Expected 0 rows at 35s after commit (TTL expired), got {count_at_35s}"
    print(f"[PASS] At ~35s after commit: {count_at_35s} rows (TTL expired from commit_ts)")

    client.drop_collection(COLLECTION)
    print("\n[ALL PASSED] test_ttl_with_commit_timestamp")


if __name__ == "__main__":
    test_ttl_with_commit_timestamp()
