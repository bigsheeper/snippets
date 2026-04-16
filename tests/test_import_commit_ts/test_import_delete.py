"""
Test: Delete interaction with commit_timestamp.
- Delete BEFORE commit: delete_ts < commit_ts → delete has NO effect
- Delete AFTER commit: delete_ts > commit_ts → delete takes effect
"""

import time
from common import (
    get_client, create_collection,
    generate_and_upload_parquet,
    import_create, import_commit, wait_for_import_state, wait_for_import_done,
)

COLLECTION = "test_import_delete"
NUM_ROWS = 500


def test_delete_before_and_after_commit():
    client = get_client()
    create_collection(client, COLLECTION)

    # Create index upfront
    index_params = client.prepare_index_params()
    index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2",
                           params={"M": 16, "efConstruction": 200})
    client.create_index(collection_name=COLLECTION, index_params=index_params)
    print(f"Collection '{COLLECTION}' created with index")

    # Import with auto_commit=false
    remote_path = generate_and_upload_parquet(NUM_ROWS, start_id=0, prefix="delete")
    job_id = import_create(COLLECTION, files=[[remote_path]], auto_commit=False)

    # Wait until Uncommitted
    wait_for_import_state(job_id, "Uncommitted")
    print("Import reached Uncommitted state")

    # Load to issue delete
    client.load_collection(COLLECTION)
    time.sleep(2)

    # ---- Delete BEFORE commit ----
    delete_ids = list(range(0, 100))
    client.delete(collection_name=COLLECTION, filter=f"id in {delete_ids}")
    print("Deleted ids [0..99] BEFORE commit (delete_ts < commit_ts)")
    time.sleep(1)

    # Commit the import
    import_commit(job_id)
    wait_for_import_done(job_id)
    print("Import committed and completed")

    # Re-load to pick up committed segments
    client.release_collection(COLLECTION)
    client.load_collection(COLLECTION)
    time.sleep(2)

    # Query — pre-commit deletes should have NO effect
    results = client.query(collection_name=COLLECTION, filter="id >= 0", output_fields=["count(*)"])
    count_after_pre_delete = results[0]["count(*)"]
    assert count_after_pre_delete == NUM_ROWS, \
        f"[FAIL] Expected {NUM_ROWS} rows (pre-commit delete should have no effect), got {count_after_pre_delete}"
    print(f"[PASS] After pre-commit delete: {count_after_pre_delete} rows (delete had no effect)")

    # Verify specific pre-commit deleted IDs are still present
    results = client.query(collection_name=COLLECTION, filter="id in [0, 1, 2, 50, 99]", output_fields=["id"])
    assert len(results) == 5, \
        f"[FAIL] Expected 5 rows for pre-commit deleted IDs, got {len(results)}"
    print("[PASS] Pre-commit deleted IDs [0,1,2,50,99] still present")

    # ---- Delete AFTER commit ----
    delete_ids_post = list(range(100, 200))
    client.delete(collection_name=COLLECTION, filter=f"id in {delete_ids_post}")
    print("Deleted ids [100..199] AFTER commit (delete_ts > commit_ts)")
    time.sleep(2)

    # Query — post-commit deletes should take effect
    results = client.query(collection_name=COLLECTION, filter="id >= 0", output_fields=["count(*)"])
    count_after_post_delete = results[0]["count(*)"]
    assert count_after_post_delete == NUM_ROWS - 100, \
        f"[FAIL] Expected {NUM_ROWS - 100} rows after post-commit delete, got {count_after_post_delete}"
    print(f"[PASS] After post-commit delete: {count_after_post_delete} rows")

    # Verify post-commit deleted IDs are gone
    results = client.query(collection_name=COLLECTION, filter="id in [100, 150, 199]", output_fields=["id"])
    assert len(results) == 0, \
        f"[FAIL] Expected 0 rows for post-commit deleted IDs, got {len(results)}"
    print("[PASS] Post-commit deleted IDs [100,150,199] are gone")

    # Verify non-deleted IDs still exist
    results = client.query(collection_name=COLLECTION, filter="id in [0, 50, 200, 300, 499]", output_fields=["id"])
    assert len(results) == 5, \
        f"[FAIL] Expected 5 non-deleted rows, got {len(results)}"
    print("[PASS] Non-deleted IDs [0,50,200,300,499] still present")

    client.drop_collection(COLLECTION)
    print("\n[ALL PASSED] test_delete_before_and_after_commit")


if __name__ == "__main__":
    test_delete_before_and_after_commit()
