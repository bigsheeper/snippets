"""
Stability test: interleave import, SDK insert, and delete at every import lifecycle phase.
Uses explicit IDs with odd/even interleaving:
  - Import rows: even IDs
  - Insert rows: odd IDs
Alternates fresh-collection rounds (odd) and accumulate-collection rounds (even).
"""

import os
import time

from common import (
    get_client, create_collection,
    generate_and_upload_parquet,
    insert_rows,
    import_create, import_commit, wait_for_import_state, wait_for_import_done,
)

DURATION_MINUTES = int(os.getenv("STABILITY_DURATION_MINUTES", "30"))
NUM_IMPORT_ROWS = 500       # even IDs per round: [offset, offset+2, ..., offset+998]
INSERT_BATCH_SIZE = 50      # odd IDs per phase
INSERT_DELETE_COUNT = 30    # 60% of INSERT_BATCH_SIZE deleted per phase
IMPORT_DELETE_COUNT = 300   # first 300 even IDs deleted pre-commit
ACCUM_COLLECTION = "test_stability_insert_delete_accum"


class AccumulateState:
    """Track state for the long-lived accumulate collection."""
    def __init__(self):
        self.committed_total = 0
        self.next_offset = 0
        self.created = False


def _elapsed_str(start):
    e = int(time.time() - start)
    return f"{e // 60}m{e % 60:02d}s"


def _verify_count(client, coll, expected, label):
    """Query count(*) with filter 'id >= 0' and assert equals expected."""
    results = client.query(collection_name=coll, filter="id >= 0", output_fields=["count(*)"])
    count = results[0]["count(*)"]
    assert count == expected, f"[{label}] Expected count={expected}, got {count}"
    print(f"  [{label}] count={count} OK")
    return count


def _verify_ids_present(client, coll, ids, label):
    """Sample up to 5 IDs and assert all are found."""
    sample = ids[:5]
    if not sample:
        return
    results = client.query(collection_name=coll, filter=f"id in {list(sample)}", output_fields=["id"])
    found = {r["id"] for r in results}
    missing = set(sample) - found
    assert not missing, f"[{label}] IDs expected present but missing: {missing}"
    print(f"  [{label}] sampled {len(sample)} IDs present OK")


def _verify_ids_gone(client, coll, ids, label):
    """Sample up to 5 IDs and assert none are found."""
    sample = ids[:5]
    if not sample:
        return
    results = client.query(collection_name=coll, filter=f"id in {list(sample)}", output_fields=["id"])
    found = {r["id"] for r in results}
    assert not found, f"[{label}] IDs expected gone but found: {found}"
    print(f"  [{label}] sampled {len(sample)} IDs gone OK")


def _run_round(round_num, start_time, coll, client, offset, committed_before):
    """
    Core 3-phase logic shared by fresh and accumulate rounds.

    Returns the number of rows in the collection after this round's logic
    (excluding committed_before; i.e., rows added by this round = 360).
    """
    tag = f"Round {round_num}"

    # --- ID allocation ---
    # Import: even IDs [offset, offset+2, ..., offset+998] (500 IDs)
    import_ids = list(range(offset, offset + 1000, 2))          # 500 even IDs
    # Insert batch1: odd [offset+1, offset+3, ..., offset+99]   (50 IDs)
    batch1_ids = list(range(offset + 1, offset + 100, 2))
    # Insert batch2: odd [offset+101, offset+103, ..., offset+199] (50 IDs)
    batch2_ids = list(range(offset + 101, offset + 200, 2))
    # Insert batch3: odd [offset+201, offset+203, ..., offset+299] (50 IDs)
    batch3_ids = list(range(offset + 201, offset + 300, 2))

    # Pre-commit delete targets: first 300 even IDs = [offset, offset+2, ..., offset+598]
    pre_commit_del_ids = import_ids[:IMPORT_DELETE_COUNT]   # 300 IDs
    # Post-commit delete targets: last 200 even IDs = [offset+600, offset+602, ..., offset+998]
    post_commit_del_ids = import_ids[IMPORT_DELETE_COUNT:]  # 200 IDs

    # =========================================================================
    # PHASE 1: Before import_create
    # =========================================================================
    print(f"  [{tag}] Phase 1: pre-import inserts + deletes")

    # Insert batch1 (50 odd IDs)
    cnt = insert_rows(client, coll, batch1_ids, varchar_prefix="insert_b1")
    assert cnt == INSERT_BATCH_SIZE, f"[{tag} P1] Expected {INSERT_BATCH_SIZE} inserted, got {cnt}"

    # Delete first 30 from batch1
    del1_ids = batch1_ids[:INSERT_DELETE_COUNT]
    client.delete(collection_name=coll, filter=f"id in {del1_ids}")
    time.sleep(1)

    # batch1 remaining: last 20
    batch1_remaining = batch1_ids[INSERT_DELETE_COUNT:]
    expected_p1 = committed_before + 20
    _verify_count(client, coll, expected_p1, f"{tag} P1 count")
    _verify_ids_gone(client, coll, del1_ids, f"{tag} P1 batch1 deleted gone")
    _verify_ids_present(client, coll, batch1_remaining, f"{tag} P1 batch1 remaining present")

    # =========================================================================
    # PHASE 2: Uncommitted import — insert+delete, plus pre-commit delete on import
    # =========================================================================
    print(f"  [{tag}] Phase 2: import (uncommitted) + inserts + pre-commit delete")

    remote_path = generate_and_upload_parquet(
        NUM_IMPORT_ROWS, start_id=offset, step=2,
        prefix=f"stab_ins_del_{round_num}",
    )
    job_id = import_create(coll, files=[[remote_path]], auto_commit=False)
    wait_for_import_state(job_id, "Uncommitted")

    # Insert batch2 (50 odd IDs)
    cnt = insert_rows(client, coll, batch2_ids, varchar_prefix="insert_b2")
    assert cnt == INSERT_BATCH_SIZE, f"[{tag} P2] Expected {INSERT_BATCH_SIZE} inserted, got {cnt}"

    # Delete first 30 from batch2
    del2_ids = batch2_ids[:INSERT_DELETE_COUNT]
    client.delete(collection_name=coll, filter=f"id in {del2_ids}")
    time.sleep(1)

    # Pre-commit delete on import IDs (first 300 even IDs)
    client.delete(collection_name=coll, filter=f"id in {pre_commit_del_ids}")
    time.sleep(1)

    batch2_remaining = batch2_ids[INSERT_DELETE_COUNT:]

    # Import is still uncommitted → invisible; only batch1_remaining(20) + batch2_remaining(20) visible
    expected_p2 = committed_before + 20 + 20
    _verify_count(client, coll, expected_p2, f"{tag} P2 count")
    # Even IDs: import uncommitted → should not be found at all (both pre-commit-deleted and others)
    _verify_ids_gone(client, coll, pre_commit_del_ids[:5], f"{tag} P2 even IDs gone (uncommitted)")
    _verify_ids_gone(client, coll, post_commit_del_ids[:5], f"{tag} P2 even IDs gone (uncommitted)")
    _verify_ids_present(client, coll, batch2_remaining, f"{tag} P2 batch2 remaining present")

    # =========================================================================
    # PHASE 3: Post-commit — insert+delete + post-commit delete on import
    # =========================================================================
    print(f"  [{tag}] Phase 3: import commit + inserts + post-commit delete")

    import_commit(job_id)
    wait_for_import_done(job_id)

    # Reload to pick up committed data
    client.release_collection(coll)
    client.load_collection(coll)
    time.sleep(2)

    # Insert batch3 (50 odd IDs)
    cnt = insert_rows(client, coll, batch3_ids, varchar_prefix="insert_b3")
    assert cnt == INSERT_BATCH_SIZE, f"[{tag} P3] Expected {INSERT_BATCH_SIZE} inserted, got {cnt}"

    # Delete first 30 from batch3
    del3_ids = batch3_ids[:INSERT_DELETE_COUNT]
    client.delete(collection_name=coll, filter=f"id in {del3_ids}")
    time.sleep(1)

    # Post-commit delete: last 200 even IDs
    client.delete(collection_name=coll, filter=f"id in {post_commit_del_ids}")
    time.sleep(2)

    batch3_remaining = batch3_ids[INSERT_DELETE_COUNT:]

    # Expected final count for this round's contribution:
    #   batch1 remaining:          20
    #   batch2 remaining:          20
    #   batch3 remaining:          20
    #   import pre-commit-deleted: 300 (MVCC: delete_ts < commit_ts → PRESENT)
    #   import post-commit-deleted: 200 → GONE
    #   total from this round: 20 + 20 + 20 + 300 = 360
    round_contribution = 360
    expected_p3 = committed_before + round_contribution
    _verify_count(client, coll, expected_p3, f"{tag} P3 count")

    # Pre-commit-deleted even IDs should be PRESENT (MVCC: delete_ts < commit_ts)
    _verify_ids_present(client, coll, pre_commit_del_ids, f"{tag} P3 pre-commit-deleted even IDs present (MVCC)")

    # Post-commit-deleted even IDs should be GONE
    _verify_ids_gone(client, coll, post_commit_del_ids, f"{tag} P3 post-commit-deleted even IDs gone")

    # All 3 insert batch remainders should be present
    _verify_ids_present(client, coll, batch1_remaining, f"{tag} P3 batch1 remaining present")
    _verify_ids_present(client, coll, batch2_remaining, f"{tag} P3 batch2 remaining present")
    _verify_ids_present(client, coll, batch3_remaining, f"{tag} P3 batch3 remaining present")

    return round_contribution


def run_fresh_round(round_num, start_time):
    """Odd round: new collection, full 3-phase logic, then drop."""
    coll = f"test_stab_ins_del_fresh_{round_num}"
    client = get_client()
    print(f"\n===== Round {round_num} [FRESH] started (elapsed: {_elapsed_str(start_time)}) =====")

    # Create collection + index + load
    create_collection(client, coll)
    index_params = client.prepare_index_params()
    index_params.add_index(field_name="vec", index_type="HNSW", metric_type="L2",
                           params={"M": 16, "efConstruction": 200})
    client.create_index(collection_name=coll, index_params=index_params)
    client.load_collection(coll)

    offset = 0
    committed_before = 0
    _run_round(round_num, start_time, coll, client, offset, committed_before)

    client.drop_collection(coll)
    print(f"===== Round {round_num} PASSED (elapsed: {_elapsed_str(start_time)}) =====")


def run_accumulate_round(round_num, start_time, state):
    """Even round: run 3-phase logic on long-lived collection, accumulate state."""
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
        client.load_collection(ACCUM_COLLECTION)
        state.created = True

    offset = state.next_offset
    committed_before = state.committed_total

    round_contribution = _run_round(
        round_num, start_time, ACCUM_COLLECTION, client, offset, committed_before
    )

    # Update state
    state.committed_total += round_contribution
    state.next_offset += 1000  # each round consumes IDs [offset, offset+1000)

    print(f"===== Round {round_num} PASSED (elapsed: {_elapsed_str(start_time)}) =====")
    print(f"  State updated: committed_total={state.committed_total}, next_offset={state.next_offset}")


def test_import_insert_delete():
    print(f"========== IMPORT+INSERT+DELETE STABILITY TEST START (duration: {DURATION_MINUTES}min) ==========")
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
        print(f"\n===== TEST FAILED at round {round_num} (elapsed: {elapsed}) =====")
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
    print(f"\n========== IMPORT+INSERT+DELETE STABILITY TEST COMPLETE ==========")
    print(f"Duration: {elapsed}")
    print(f"Rounds: {round_num} total ({fresh_count} fresh, {accum_count} accumulate)")
    print(f"Result: ALL PASSED")
    print(f"Accumulate collection final rows: {state.committed_total}")
    print(f"==================================================================")


if __name__ == "__main__":
    test_import_insert_delete()
