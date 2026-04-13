"""
Stability test: 30-minute continuous delete + MVCC verification.
Alternates fresh-collection rounds (odd) and accumulate-collection rounds (even).
"""

import os
import time

from common import (
    get_client, create_collection,
    generate_and_upload_parquet,
    import_create, import_commit, wait_for_import_state, wait_for_import_done,
)

DURATION_MINUTES = int(os.getenv("STABILITY_DURATION_MINUTES", "30"))
NUM_ROWS = 500
ACCUM_COLLECTION = "test_stability_accumulate"


class AccumulateState:
    """Track state for the long-lived accumulate collection."""
    def __init__(self):
        self.committed_total = 0
        self.next_offset = 0
        self.created = False


def _elapsed_str(start):
    e = int(time.time() - start)
    return f"{e // 60}m{e % 60:02d}s"


def run_fresh_round(round_num, start_time):
    """Odd round: new collection, full MVCC + delete verification, then drop."""
    coll = f"test_stability_fresh_{round_num}"
    client = get_client()
    print(f"\n===== Round {round_num} [FRESH] started (elapsed: {_elapsed_str(start_time)}) =====")

    # --- MVCC phase ---
    create_collection(client, coll)
    index_params = client.prepare_index_params()
    index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2",
                           params={"M": 16, "efConstruction": 200})
    client.create_index(collection_name=coll, index_params=index_params)

    remote_path = generate_and_upload_parquet(NUM_ROWS, start_id=0, prefix=f"stab_fresh_{round_num}_mvcc")
    job_id = import_create(coll, files=[[remote_path]], auto_commit=False)
    wait_for_import_state(job_id, "Uncommitted")

    client.load_collection(coll)
    time.sleep(2)
    results = client.query(collection_name=coll, filter="id >= 0", output_fields=["count(*)"])
    count = results[0]["count(*)"]
    assert count == 0, f"[Round {round_num} FRESH MVCC] Expected 0 rows before commit, got {count}"
    print(f"  [MVCC] pre-commit: {count} rows OK")

    import_commit(job_id)
    wait_for_import_done(job_id)
    client.release_collection(coll)
    client.load_collection(coll)
    time.sleep(2)

    results = client.query(collection_name=coll, filter="id >= 0", output_fields=["count(*)"])
    count = results[0]["count(*)"]
    assert count == NUM_ROWS, f"[Round {round_num} FRESH MVCC] Expected {NUM_ROWS} rows after commit, got {count}"
    print(f"  [MVCC] post-commit: {count} rows OK")

    # --- Delete phase (second import on same collection) ---
    remote_path2 = generate_and_upload_parquet(NUM_ROWS, start_id=500, prefix=f"stab_fresh_{round_num}_del")
    job_id2 = import_create(coll, files=[[remote_path2]], auto_commit=False)
    wait_for_import_state(job_id2, "Uncommitted")

    client.release_collection(coll)
    client.load_collection(coll)
    time.sleep(2)

    # Pre-commit delete
    delete_ids = list(range(500, 600))
    client.delete(collection_name=coll, filter=f"id in {delete_ids}")
    time.sleep(1)

    import_commit(job_id2)
    wait_for_import_done(job_id2)
    client.release_collection(coll)
    client.load_collection(coll)
    time.sleep(2)

    # Pre-commit delete should have no effect
    results = client.query(collection_name=coll, filter="id >= 0", output_fields=["count(*)"])
    count = results[0]["count(*)"]
    assert count == NUM_ROWS * 2, \
        f"[Round {round_num} FRESH DELETE] Expected {NUM_ROWS * 2} rows (pre-commit delete no-op), got {count}"
    print(f"  [DELETE] pre-commit delete no-op: {count} rows OK")

    # Verify pre-commit deleted IDs still present
    results = client.query(collection_name=coll, filter="id in [500, 550, 599]", output_fields=["id"])
    assert len(results) == 3, \
        f"[Round {round_num} FRESH DELETE] Expected 3 pre-commit deleted IDs still present, got {len(results)}"

    # Post-commit delete
    delete_ids_post = list(range(600, 700))
    client.delete(collection_name=coll, filter=f"id in {delete_ids_post}")
    time.sleep(2)

    results = client.query(collection_name=coll, filter="id >= 0", output_fields=["count(*)"])
    count = results[0]["count(*)"]
    assert count == NUM_ROWS * 2 - 100, \
        f"[Round {round_num} FRESH DELETE] Expected {NUM_ROWS * 2 - 100} rows after post-commit delete, got {count}"
    print(f"  [DELETE] post-commit delete effective: {count} rows OK")

    # Verify post-commit deleted IDs gone
    results = client.query(collection_name=coll, filter="id in [600, 650, 699]", output_fields=["id"])
    assert len(results) == 0, \
        f"[Round {round_num} FRESH DELETE] Expected 0 for post-commit deleted IDs, got {len(results)}"

    client.drop_collection(coll)
    round_dur = _elapsed_str(start_time)
    print(f"===== Round {round_num} PASSED (elapsed: {round_dur}) =====")


def run_accumulate_round(round_num, start_time, state):
    """Even round: import new batch on long-lived collection, verify MVCC + delete."""
    client = get_client()
    print(f"\n===== Round {round_num} [ACCUMULATE] started (elapsed: {_elapsed_str(start_time)}) =====")
    print(f"  State: committed_total={state.committed_total}, next_offset={state.next_offset}")

    # First accumulate round: create the collection
    if not state.created:
        create_collection(client, ACCUM_COLLECTION)
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2",
                               params={"M": 16, "efConstruction": 200})
        client.create_index(collection_name=ACCUM_COLLECTION, index_params=index_params)
        state.created = True

    offset = state.next_offset
    remote_path = generate_and_upload_parquet(NUM_ROWS, start_id=offset, prefix=f"stab_accum_{round_num}")
    job_id = import_create(ACCUM_COLLECTION, files=[[remote_path]], auto_commit=False)
    wait_for_import_state(job_id, "Uncommitted")

    # --- MVCC verification: new batch should be invisible ---
    client.release_collection(ACCUM_COLLECTION)
    client.load_collection(ACCUM_COLLECTION)
    time.sleep(2)

    results = client.query(collection_name=ACCUM_COLLECTION, filter="id >= 0", output_fields=["count(*)"])
    count = results[0]["count(*)"]
    assert count == state.committed_total, \
        f"[Round {round_num} ACCUM MVCC] Expected {state.committed_total} rows before commit, got {count}"
    print(f"  [MVCC] pre-commit: {count} rows OK (new batch invisible)")

    import_commit(job_id)
    wait_for_import_done(job_id)
    client.release_collection(ACCUM_COLLECTION)
    client.load_collection(ACCUM_COLLECTION)
    time.sleep(2)

    expected_after_commit = state.committed_total + NUM_ROWS
    results = client.query(collection_name=ACCUM_COLLECTION, filter="id >= 0", output_fields=["count(*)"])
    count = results[0]["count(*)"]
    assert count == expected_after_commit, \
        f"[Round {round_num} ACCUM MVCC] Expected {expected_after_commit} rows after commit, got {count}"
    print(f"  [MVCC] post-commit: {count} rows OK (new batch visible)")

    # --- Delete verification: post-commit delete on new batch ---
    delete_ids = list(range(offset, offset + 100))
    client.delete(collection_name=ACCUM_COLLECTION, filter=f"id in {delete_ids}")
    time.sleep(2)

    expected_after_delete = expected_after_commit - 100
    results = client.query(collection_name=ACCUM_COLLECTION, filter="id >= 0", output_fields=["count(*)"])
    count = results[0]["count(*)"]
    assert count == expected_after_delete, \
        f"[Round {round_num} ACCUM DELETE] Expected {expected_after_delete} rows after delete, got {count}"
    print(f"  [DELETE] post-commit delete effective: {count} rows OK")

    # Verify deleted IDs gone
    sample_deleted = [offset, offset + 50, offset + 99]
    results = client.query(collection_name=ACCUM_COLLECTION,
                           filter=f"id in {sample_deleted}", output_fields=["id"])
    assert len(results) == 0, \
        f"[Round {round_num} ACCUM DELETE] Expected 0 for deleted IDs {sample_deleted}, got {len(results)}"

    # Verify old data still intact (sample from first batch if exists)
    if state.committed_total > 0:
        # Previous round kept [prev_offset+100, prev_offset+NUM_ROWS-1]; sample from middle
        prev_offset = offset - NUM_ROWS
        safe_id = prev_offset + NUM_ROWS // 2  # well within the kept range
        if safe_id >= 0:
            results = client.query(collection_name=ACCUM_COLLECTION,
                                   filter=f"id == {safe_id}", output_fields=["id"])
            assert len(results) == 1, \
                f"[Round {round_num} ACCUM DELETE] Expected old id {safe_id} still present, got {len(results)}"
            print(f"  [DELETE] old data intact (sampled id={safe_id}) OK")

    # Update state
    state.committed_total = expected_after_delete
    state.next_offset = offset + NUM_ROWS

    print(f"===== Round {round_num} PASSED (elapsed: {_elapsed_str(start_time)}) =====")


def test_stability():
    print(f"========== STABILITY TEST START (duration: {DURATION_MINUTES}min) ==========")
    start_time = time.time()
    deadline = start_time + DURATION_MINUTES * 60
    state = AccumulateState()
    round_num = 0
    fresh_count = 0
    accum_count = 0

    try:
        while time.time() < deadline:
            round_num += 1
            if round_num % 2 == 1:
                run_fresh_round(round_num, start_time)
                fresh_count += 1
            else:
                run_accumulate_round(round_num, start_time, state)
                accum_count += 1
    except Exception as e:
        elapsed = _elapsed_str(start_time)
        print(f"\n===== STABILITY TEST FAILED at round {round_num} (elapsed: {elapsed}) =====")
        print(f"  Error: {e}")
        print(f"  Rounds completed: {round_num - 1} ({fresh_count} fresh, {accum_count} accumulate)")
        print(f"  Accumulate state: committed_total={state.committed_total}, next_offset={state.next_offset}")
        # Cleanup
        try:
            client = get_client()
            if state.created:
                client.drop_collection(ACCUM_COLLECTION)
        except Exception:
            pass
        raise

    # Cleanup accumulate collection
    if state.created:
        client = get_client()
        client.drop_collection(ACCUM_COLLECTION)

    elapsed = _elapsed_str(start_time)
    print(f"\n========== STABILITY TEST COMPLETE ==========")
    print(f"Duration: {elapsed}")
    print(f"Rounds: {round_num} total ({fresh_count} fresh, {accum_count} accumulate)")
    print(f"Result: ALL PASSED")
    print(f"Accumulate collection final rows: {state.committed_total}")
    print(f"==============================================")


if __name__ == "__main__":
    test_stability()
