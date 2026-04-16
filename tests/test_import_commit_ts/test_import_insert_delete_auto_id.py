"""
Stability test: interleave import, SDK insert, and delete at every import
lifecycle phase, using AUTO-GENERATED IDs.

Since IDs are auto-generated, varchar field is used to identify and operate
on data sources.

NOTE on MVCC with auto-ID:
With auto-ID, pre-commit delete uses expression-based filtering (varchar prefix).
Since import rows don't exist pre-commit, the expression matches nothing — it's a no-op.
This tests a different mechanism than explicit-ID MVCC (where delete records exist but
are skipped due to ts comparison), but the user-visible result is the same.
"""

import os
import time

from common import (
    get_client, create_collection_auto_id,
    generate_and_upload_parquet_auto_id,
    insert_rows_auto_id, create_index_and_load,
    import_create, import_commit, wait_for_import_state, wait_for_import_done,
)

DURATION_MINUTES = int(os.getenv("STABILITY_DURATION_MINUTES", "30"))
NUM_IMPORT_ROWS = 500
INSERT_BATCH_SIZE = 50
INSERT_DELETE_COUNT = 30     # 60% of INSERT_BATCH_SIZE
IMPORT_DELETE_COUNT = 300    # 60% of NUM_IMPORT_ROWS
ACCUM_COLLECTION = "test_stability_insert_delete_auto_accum"


class AccumulateState:
    def __init__(self):
        self.committed_total = 0
        self.round_count = 0
        self.created = False


def _elapsed_str(start):
    e = int(time.time() - start)
    return f"{e // 60}m{e % 60:02d}s"


# ---------- Verification helpers ----------

def _verify_count(client, coll, expected, label):
    """Query count(*) with filter 'id >= 0' and assert it equals expected."""
    results = client.query(collection_name=coll, filter="id >= 0", output_fields=["count(*)"])
    count = results[0]["count(*)"]
    assert count == expected, f"[{label}] Expected count={expected}, got {count}"
    print(f"  [{label}] count={count} OK")
    return count


def _verify_varchar_count(client, coll, varchar_values, expected_count, label):
    """Query specific varchar values, verify expected_count found. Samples up to 10."""
    sample = varchar_values[:10]
    quoted = ", ".join(f'"{v}"' for v in sample)
    filter_expr = f"varchar in [{quoted}]"
    results = client.query(collection_name=coll, filter=filter_expr, output_fields=["varchar"])
    found = len(results)
    assert found == expected_count, \
        f"[{label}] Expected {expected_count} rows for varchar sample, got {found}"
    print(f"  [{label}] varchar sample check: {found}/{len(sample)} found, expected {expected_count} OK")


def _delete_by_varchar(client, coll, varchar_values, label):
    """Delete by filter 'varchar in [...]'. Batches in groups of 100. Returns count targeted."""
    total = len(varchar_values)
    batch_size = 100
    for i in range(0, total, batch_size):
        batch = varchar_values[i:i + batch_size]
        quoted = ", ".join(f'"{v}"' for v in batch)
        filter_expr = f"varchar in [{quoted}]"
        client.delete(collection_name=coll, filter=filter_expr)
    print(f"  [{label}] Deleted by varchar: {total} targeted")
    return total


def _query_ids_by_varchar_prefix(client, coll, prefix, limit=10000):
    """Query 'varchar like \"{prefix}%\"', return list of (id, varchar) tuples."""
    results = client.query(
        collection_name=coll,
        filter=f'varchar like "{prefix}%"',
        output_fields=["id", "varchar"],
        limit=limit,
    )
    return [(r["id"], r["varchar"]) for r in results]


# ---------- Fresh round ----------

def run_fresh_round(round_num, start_time):
    """Odd round: new collection with auto-ID, 3-phase import+insert+delete, then drop."""
    rnd = f"r{round_num}"
    coll = f"test_stability_fresh_auto_{round_num}"
    client = get_client()
    print(f"\n===== Round {round_num} [FRESH] started (elapsed: {_elapsed_str(start_time)}) =====")

    # ---- Phase 1: Before import_create ----
    print(f"  -- Phase 1: Before import_create --")
    create_collection_auto_id(client, coll)
    create_index_and_load(client, coll)

    p1_varchars = [f"{rnd}_insert_p1_{i}" for i in range(INSERT_BATCH_SIZE)]
    insert_rows_auto_id(client, coll, INSERT_BATCH_SIZE, varchar_prefix=f"{rnd}_insert_p1")

    # Delete first INSERT_DELETE_COUNT (30) of the batch
    _delete_by_varchar(client, coll, p1_varchars[:INSERT_DELETE_COUNT], "p1-delete")
    time.sleep(1)

    # Verify: 20 rows remain (50 - 30)
    p1_expected = INSERT_BATCH_SIZE - INSERT_DELETE_COUNT  # 20
    _verify_count(client, coll, p1_expected, "phase1-count")
    # Deleted varchars should be gone
    _verify_varchar_count(client, coll, p1_varchars[:INSERT_DELETE_COUNT], 0, "phase1-deleted-gone")
    # Remaining varchars present — sample from survivors
    _verify_varchar_count(client, coll, p1_varchars[INSERT_DELETE_COUNT:INSERT_DELETE_COUNT + 10],
                          10, "phase1-survivors-present")

    # ---- Phase 2: Uncommitted ----
    print(f"  -- Phase 2: Uncommitted --")
    import_varchars = [f"{rnd}_import_{i}" for i in range(NUM_IMPORT_ROWS)]
    remote_path = generate_and_upload_parquet_auto_id(
        NUM_IMPORT_ROWS,
        varchar_prefix=f"{rnd}_import",
        prefix=f"fresh_auto_{round_num}",
    )
    job_id = import_create(coll, files=[[remote_path]], auto_commit=False)
    wait_for_import_state(job_id, "Uncommitted")

    # Insert batch 2
    p2_varchars = [f"{rnd}_insert_p2_{i}" for i in range(INSERT_BATCH_SIZE)]
    insert_rows_auto_id(client, coll, INSERT_BATCH_SIZE, varchar_prefix=f"{rnd}_insert_p2")
    _delete_by_varchar(client, coll, p2_varchars[:INSERT_DELETE_COUNT], "p2-delete")
    time.sleep(1)

    # Pre-commit delete: first 300 import varchars — matches nothing (rows not committed yet)
    _delete_by_varchar(client, coll, import_varchars[:IMPORT_DELETE_COUNT], "pre-commit-import-delete")
    time.sleep(1)

    # Verify: 20 (p1 survivors) + 20 (p2 survivors) = 40; import invisible
    p2_expected = p1_expected + (INSERT_BATCH_SIZE - INSERT_DELETE_COUNT)  # 40
    _verify_count(client, coll, p2_expected, "phase2-count")
    # Import rows should not be visible
    import_results = _query_ids_by_varchar_prefix(client, coll, f"{rnd}_import")
    assert len(import_results) == 0, \
        f"[phase2-import-invisible] Expected 0 import rows before commit, got {len(import_results)}"
    print(f"  [phase2-import-invisible] import rows before commit: 0 OK")

    # ---- Phase 3: Post-commit ----
    print(f"  -- Phase 3: Post-commit --")
    import_commit(job_id)
    wait_for_import_done(job_id)
    client.refresh_load(coll)
    time.sleep(2)

    # Insert batch 3
    p3_varchars = [f"{rnd}_insert_p3_{i}" for i in range(INSERT_BATCH_SIZE)]
    insert_rows_auto_id(client, coll, INSERT_BATCH_SIZE, varchar_prefix=f"{rnd}_insert_p3")
    _delete_by_varchar(client, coll, p3_varchars[:INSERT_DELETE_COUNT], "p3-delete")
    time.sleep(1)

    # Post-commit delete: last 200 import varchars (import_varchars[300:])
    _delete_by_varchar(client, coll, import_varchars[IMPORT_DELETE_COUNT:], "post-commit-import-delete")
    time.sleep(2)

    # Expected: 20 (p1) + 20 (p2) + 300 (import, first 300 NOT deleted since pre-commit was no-op)
    #           + 20 (p3) = 360
    expected_final = p1_expected + (INSERT_BATCH_SIZE - INSERT_DELETE_COUNT) + \
                     IMPORT_DELETE_COUNT + (INSERT_BATCH_SIZE - INSERT_DELETE_COUNT)
    # = 20 + 20 + 300 + 20 = 360
    _verify_count(client, coll, expected_final, "phase3-final-count")

    # Pre-commit-deleted import varchars (first 300) → PRESENT (pre-commit was no-op)
    _verify_varchar_count(client, coll, import_varchars[:10], 10, "phase3-pre-commit-import-present")

    # Post-commit-deleted import varchars (last 200) → GONE
    _verify_varchar_count(client, coll, import_varchars[IMPORT_DELETE_COUNT:IMPORT_DELETE_COUNT + 10],
                          0, "phase3-post-commit-import-gone")

    # Insert batch remainders present (last few survivors of each batch)
    _verify_varchar_count(client, coll, p1_varchars[INSERT_DELETE_COUNT:INSERT_DELETE_COUNT + 5],
                          5, "phase3-p1-survivors")
    _verify_varchar_count(client, coll, p2_varchars[INSERT_DELETE_COUNT:INSERT_DELETE_COUNT + 5],
                          5, "phase3-p2-survivors")
    _verify_varchar_count(client, coll, p3_varchars[INSERT_DELETE_COUNT:INSERT_DELETE_COUNT + 5],
                          5, "phase3-p3-survivors")

    client.drop_collection(coll)
    print(f"===== Round {round_num} [FRESH] PASSED (elapsed: {_elapsed_str(start_time)}) =====")


# ---------- Accumulate round ----------

def run_accumulate_round(round_num, start_time, state):
    """Even round: import new batch on long-lived collection, verify 3 phases."""
    rnd = f"r{round_num}"
    client = get_client()
    print(f"\n===== Round {round_num} [ACCUMULATE] started (elapsed: {_elapsed_str(start_time)}) =====")
    print(f"  State: committed_total={state.committed_total}")

    # ---- Phase 1: Before import_create ----
    print(f"  -- Phase 1: Before import_create --")
    if not state.created:
        create_collection_auto_id(client, ACCUM_COLLECTION)
        create_index_and_load(client, ACCUM_COLLECTION)
        state.created = True

    p1_varchars = [f"{rnd}_insert_p1_{i}" for i in range(INSERT_BATCH_SIZE)]
    insert_rows_auto_id(client, ACCUM_COLLECTION, INSERT_BATCH_SIZE,
                        varchar_prefix=f"{rnd}_insert_p1")
    _delete_by_varchar(client, ACCUM_COLLECTION, p1_varchars[:INSERT_DELETE_COUNT], "p1-delete")
    time.sleep(1)

    p1_survivors = INSERT_BATCH_SIZE - INSERT_DELETE_COUNT  # 20
    phase1_expected = state.committed_total + p1_survivors
    _verify_count(client, ACCUM_COLLECTION, phase1_expected, "phase1-count")
    _verify_varchar_count(client, ACCUM_COLLECTION, p1_varchars[:INSERT_DELETE_COUNT],
                          0, "phase1-deleted-gone")
    _verify_varchar_count(client, ACCUM_COLLECTION,
                          p1_varchars[INSERT_DELETE_COUNT:INSERT_DELETE_COUNT + 10],
                          10, "phase1-survivors-present")

    # ---- Phase 2: Uncommitted ----
    print(f"  -- Phase 2: Uncommitted --")
    import_varchars = [f"{rnd}_import_{i}" for i in range(NUM_IMPORT_ROWS)]
    remote_path = generate_and_upload_parquet_auto_id(
        NUM_IMPORT_ROWS,
        varchar_prefix=f"{rnd}_import",
        prefix=f"accum_auto_{round_num}",
    )
    job_id = import_create(ACCUM_COLLECTION, files=[[remote_path]], auto_commit=False)
    wait_for_import_state(job_id, "Uncommitted")

    p2_varchars = [f"{rnd}_insert_p2_{i}" for i in range(INSERT_BATCH_SIZE)]
    insert_rows_auto_id(client, ACCUM_COLLECTION, INSERT_BATCH_SIZE,
                        varchar_prefix=f"{rnd}_insert_p2")
    _delete_by_varchar(client, ACCUM_COLLECTION, p2_varchars[:INSERT_DELETE_COUNT], "p2-delete")
    time.sleep(1)

    _delete_by_varchar(client, ACCUM_COLLECTION, import_varchars[:IMPORT_DELETE_COUNT],
                       "pre-commit-import-delete")
    time.sleep(1)

    p2_survivors = INSERT_BATCH_SIZE - INSERT_DELETE_COUNT  # 20
    phase2_expected = state.committed_total + p1_survivors + p2_survivors
    _verify_count(client, ACCUM_COLLECTION, phase2_expected, "phase2-count")
    import_results = _query_ids_by_varchar_prefix(client, ACCUM_COLLECTION, f"{rnd}_import")
    assert len(import_results) == 0, \
        f"[phase2-import-invisible] Expected 0 import rows before commit, got {len(import_results)}"
    print(f"  [phase2-import-invisible] import rows before commit: 0 OK")

    # ---- Phase 3: Post-commit ----
    print(f"  -- Phase 3: Post-commit --")
    import_commit(job_id)
    wait_for_import_done(job_id)
    client.refresh_load(ACCUM_COLLECTION)
    time.sleep(2)

    p3_varchars = [f"{rnd}_insert_p3_{i}" for i in range(INSERT_BATCH_SIZE)]
    insert_rows_auto_id(client, ACCUM_COLLECTION, INSERT_BATCH_SIZE,
                        varchar_prefix=f"{rnd}_insert_p3")
    _delete_by_varchar(client, ACCUM_COLLECTION, p3_varchars[:INSERT_DELETE_COUNT], "p3-delete")
    time.sleep(1)

    _delete_by_varchar(client, ACCUM_COLLECTION, import_varchars[IMPORT_DELETE_COUNT:],
                       "post-commit-import-delete")
    time.sleep(2)

    p3_survivors = INSERT_BATCH_SIZE - INSERT_DELETE_COUNT  # 20
    # committed_total + 20 (p1) + 20 (p2) + 300 (import first 300) + 20 (p3) = committed_total + 360
    round_net = p1_survivors + p2_survivors + IMPORT_DELETE_COUNT + p3_survivors  # 360
    phase3_expected = state.committed_total + round_net
    _verify_count(client, ACCUM_COLLECTION, phase3_expected, "phase3-final-count")

    # Pre-commit-deleted import varchars (first 300) → PRESENT
    _verify_varchar_count(client, ACCUM_COLLECTION, import_varchars[:10],
                          10, "phase3-pre-commit-import-present")

    # Post-commit-deleted import varchars (last 200) → GONE
    _verify_varchar_count(client, ACCUM_COLLECTION,
                          import_varchars[IMPORT_DELETE_COUNT:IMPORT_DELETE_COUNT + 10],
                          0, "phase3-post-commit-import-gone")

    # Insert batch remainders present
    _verify_varchar_count(client, ACCUM_COLLECTION,
                          p1_varchars[INSERT_DELETE_COUNT:INSERT_DELETE_COUNT + 5],
                          5, "phase3-p1-survivors")
    _verify_varchar_count(client, ACCUM_COLLECTION,
                          p2_varchars[INSERT_DELETE_COUNT:INSERT_DELETE_COUNT + 5],
                          5, "phase3-p2-survivors")
    _verify_varchar_count(client, ACCUM_COLLECTION,
                          p3_varchars[INSERT_DELETE_COUNT:INSERT_DELETE_COUNT + 5],
                          5, "phase3-p3-survivors")

    state.committed_total = phase3_expected
    state.round_count += 1

    print(f"===== Round {round_num} [ACCUMULATE] PASSED (elapsed: {_elapsed_str(start_time)}) =====")


# ---------- Main test ----------

def test_import_insert_delete_auto_id():
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
        print(f"  Accumulate state: committed_total={state.committed_total}")
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
    test_import_insert_delete_auto_id()
