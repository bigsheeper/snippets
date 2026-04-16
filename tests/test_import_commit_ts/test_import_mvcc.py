"""
Test: MVCC visibility before and after commit.
Verifies that imported data is invisible before commit and visible after.
"""

import time
from common import (
    get_client, create_collection, create_index_and_load,
    generate_and_upload_parquet,
    import_create, import_commit, wait_for_import_state, wait_for_import_done,
)

COLLECTION = "test_import_mvcc"
NUM_ROWS = 500


def test_mvcc_visibility():
    client = get_client()
    create_collection(client, COLLECTION)

    # Create index upfront so data can be loaded during uncommitted state
    index_params = client.prepare_index_params()
    index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2",
                           params={"M": 16, "efConstruction": 200})
    client.create_index(collection_name=COLLECTION, index_params=index_params)
    print(f"Collection '{COLLECTION}' created with index")

    # Import with auto_commit=false
    remote_path = generate_and_upload_parquet(NUM_ROWS, start_id=0, prefix="mvcc")
    job_id = import_create(COLLECTION, files=[[remote_path]], auto_commit=False)

    # Wait until Uncommitted
    wait_for_import_state(job_id, "Uncommitted")
    print("Import reached Uncommitted state")

    # Query BEFORE commit — data should be invisible
    client.load_collection(COLLECTION)
    time.sleep(2)
    results = client.query(collection_name=COLLECTION, filter="id >= 0", output_fields=["count(*)"])
    count_before_commit = results[0]["count(*)"]
    assert count_before_commit == 0, \
        f"[FAIL] Expected 0 rows before commit, got {count_before_commit}"
    print(f"[PASS] Query before commit: {count_before_commit} rows (invisible)")

    # Commit the import
    import_commit(job_id)
    wait_for_import_done(job_id)
    print("Import committed and completed")

    # Re-load to pick up newly committed segments
    client.release_collection(COLLECTION)
    client.load_collection(COLLECTION)
    time.sleep(2)

    # Query at current time — should be 500 (data visible after commit)
    results = client.query(collection_name=COLLECTION, filter="id >= 0", output_fields=["count(*)"])
    count_now = results[0]["count(*)"]
    assert count_now == NUM_ROWS, \
        f"[FAIL] Expected {NUM_ROWS} rows at current time, got {count_now}"
    print(f"[PASS] Query at current time: {count_now} rows (visible)")

    client.drop_collection(COLLECTION)
    print("\n[ALL PASSED] test_mvcc_visibility")


if __name__ == "__main__":
    test_mvcc_visibility()
