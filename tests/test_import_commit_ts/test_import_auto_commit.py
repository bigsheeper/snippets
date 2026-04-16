"""
Test: auto_commit=true (default) import path.
Verifies that import with auto_commit=true auto-completes without explicit commit,
and search/query work correctly after completion.
"""

import numpy as np
from common import (
    get_client, create_collection, create_index_and_load,
    generate_and_upload_parquet,
    import_create, wait_for_import_done, DIM, RNG,
)

COLLECTION = "test_import_auto_commit"
NUM_ROWS = 1000


def test_import_auto_commit():
    client = get_client()
    create_collection(client, COLLECTION)
    print(f"Collection '{COLLECTION}' created")

    remote_path = generate_and_upload_parquet(NUM_ROWS, start_id=0, prefix="auto_commit")

    job_id = import_create(COLLECTION, files=[[remote_path]])
    wait_for_import_done(job_id)

    create_index_and_load(client, COLLECTION)
    print("Index created and collection loaded")

    # Count
    results = client.query(collection_name=COLLECTION, filter="id >= 0", output_fields=["count(*)"])
    count = results[0]["count(*)"]
    assert count == NUM_ROWS, f"Expected {NUM_ROWS} rows, got {count}"
    print(f"[PASS] Count query: {count} rows")

    # Query by PK
    results = client.query(collection_name=COLLECTION, filter="id in [0, 1, 2, 3, 4]", output_fields=["id", "varchar"])
    assert len(results) == 5, f"Expected 5 rows, got {len(results)}"
    returned_ids = sorted([r["id"] for r in results])
    assert returned_ids == [0, 1, 2, 3, 4], f"Unexpected IDs: {returned_ids}"
    for r in results:
        assert r["varchar"] == f"row_{r['id']}", f"varchar mismatch for id={r['id']}"
    print("[PASS] Query by PK: correct field values")

    # Search
    query_vec = RNG.random((1, DIM), dtype=np.float32).tolist()
    search_results = client.search(
        collection_name=COLLECTION,
        data=query_vec,
        anns_field="vec",
        search_params={"metric_type": "L2", "params": {"ef": 64}},
        limit=10,
        output_fields=["id", "varchar"],
    )
    assert len(search_results) == 1, "Expected 1 search result set"
    assert len(search_results[0]) == 10, f"Expected 10 hits, got {len(search_results[0])}"
    for hit in search_results[0]:
        assert 0 <= hit["id"] < NUM_ROWS, f"Unexpected hit id: {hit['id']}"
    print(f"[PASS] Search: top-10 results, best distance={search_results[0][0]['distance']:.4f}")

    client.drop_collection(COLLECTION)
    print("\n[ALL PASSED] test_import_auto_commit")


if __name__ == "__main__":
    test_import_auto_commit()
