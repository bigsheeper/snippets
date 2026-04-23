"""Interleaved 2PC import + SDK insert + delete, each phase verified on A and B.

ID layout (per round, at offset):
  - import rows:   even IDs [offset, offset+2, ..., offset+998]   (500 IDs)
  - insert batch1: odd  IDs [offset+1, ..., offset+99]             (50 IDs)
  - insert batch2: odd  IDs [offset+101, ..., offset+199]          (50 IDs)
  - insert batch3: odd  IDs [offset+201, ..., offset+299]          (50 IDs)

Per-round 3 phases:
  P1 (pre-import):  insert batch1, delete first 30 → 20 survivors
  P2 (Uncommitted): import_create, insert batch2 + delete 30 of batch2,
                    pre-commit delete first 300 import evens →
                    evens invisible, visible count = 20 (p1) + 20 (p2) = 40
  P3 (Committed):   import_commit, insert batch3 + delete 30 of batch3,
                    post-commit delete last 200 import evens →
                    visible count = 20 (p1) + 20 (p2) + 20 (p3) + 300 (pre-commit-deleted evens) = 360

A and B must agree on state at the end of each phase (wait_for_query_consistent).
Alternates fresh (new collection per round, dropped at end) ↔ accumulate
(long-lived collection) rounds for STABILITY_DURATION_MINUTES (default 30).
"""
import os
import sys
import time

import _path_setup  # noqa: F401

import importlib.util as _ilu
_here = os.path.dirname(os.path.abspath(__file__))
_spec = _ilu.spec_from_file_location("import_common", os.path.join(_here, "common.py"))
imp_common = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(imp_common)

from loguru import logger

from common import (
    cluster_A_client, cluster_B_client,
    init_replication_a_to_b,
    setup_collection, cleanup_collection, drop_if_exists,
    query_all, wait_for_query_consistent, wait_for_row_count,
)

generate_and_upload_parquet = imp_common.generate_and_upload_parquet
insert_rows = imp_common.insert_rows
import_create_on_a = imp_common.import_create_on_a
import_commit_on_a = imp_common.import_commit_on_a
wait_for_import_state_on_a = imp_common.wait_for_import_state_on_a
wait_for_import_done_on_a = imp_common.wait_for_import_done_on_a
count_rows = imp_common.count_rows

DURATION_MINUTES = int(os.getenv("STABILITY_DURATION_MINUTES", "30"))
NUM_IMPORT_ROWS = 500
INSERT_BATCH_SIZE = 50
INSERT_DELETE_COUNT = 30     # first 30 of each 50-row batch deleted
IMPORT_DELETE_COUNT = 300    # first 300 even import IDs pre-commit-deleted
ROUND_CONTRIBUTION = (
    (INSERT_BATCH_SIZE - INSERT_DELETE_COUNT) * 3  # 3 insert batches → 3×20 survivors
    + IMPORT_DELETE_COUNT                          # pre-commit-deleted evens PRESENT after commit
)  # = 60 + 300 = 360
ACCUM_COLLECTION = "test_import_repl_insert_delete_accum"


class AccumulateState:
    def __init__(self):
        self.committed_total = 0
        self.next_offset = 0
        self.created = False


def _elapsed_str(start):
    e = int(time.time() - start)
    return f"{e // 60}m{e % 60:02d}s"


def _query_ids(client, collection, ids):
    if not ids:
        return []
    results = client.query(
        collection_name=collection,
        filter=f"id in {list(ids)}",
        output_fields=["id"],
    )
    return sorted(r["id"] for r in results)


def _assert_consistent(primary, standby, collection, expected_count, label):
    """Both A and B must show expected_count rows with matching PK sets.

    Waits for A to reach expected_count first — commit→visible has a
    sealing/flushing lag, and delete-against-not-yet-visible rows is
    a no-op on the visible count.
    """
    wait_for_row_count(primary, collection, expected_count, timeout=120)
    a_rows = query_all(primary, collection)
    wait_for_query_consistent(collection, a_rows, standby)
    logger.info(f"  [{label}] A=B={expected_count} OK")


def _run_round(round_num, start_time, coll, primary, standby, offset, committed_before):
    tag = f"Round {round_num}"
    logger.info(f"--- {tag} offset={offset} committed_before={committed_before} ---")

    # --- ID allocation ---
    import_ids = list(range(offset, offset + 1000, 2))           # 500 even IDs
    batch1_ids = list(range(offset + 1, offset + 100, 2))        # 50 odd IDs
    batch2_ids = list(range(offset + 101, offset + 200, 2))      # 50 odd IDs
    batch3_ids = list(range(offset + 201, offset + 300, 2))      # 50 odd IDs
    pre_commit_del_ids = import_ids[:IMPORT_DELETE_COUNT]        # 300 evens
    post_commit_del_ids = import_ids[IMPORT_DELETE_COUNT:]       # 200 evens

    # =====================================================================
    # PHASE 1: pre-import inserts + deletes
    # =====================================================================
    logger.info(f"  [{tag}] Phase 1: pre-import inserts + deletes")
    cnt = insert_rows(primary, coll, batch1_ids)
    assert cnt == INSERT_BATCH_SIZE, f"[{tag} P1] expected {INSERT_BATCH_SIZE} inserted, got {cnt}"
    primary.delete(collection_name=coll, filter=f"id in {batch1_ids[:INSERT_DELETE_COUNT]}")
    time.sleep(1)

    p1_survivors = batch1_ids[INSERT_DELETE_COUNT:]
    _assert_consistent(primary, standby, coll,
                       committed_before + len(p1_survivors), f"{tag} P1")

    # =====================================================================
    # PHASE 2: Uncommitted import + inserts + pre-commit delete on evens
    # =====================================================================
    logger.info(f"  [{tag}] Phase 2: Uncommitted import + inserts + pre-commit delete on evens")
    remote_key = generate_and_upload_parquet(
        NUM_IMPORT_ROWS, start_id=offset, step=2,
        prefix=f"repl_ins_del_{round_num}",
    )
    job_id = import_create_on_a(coll, [[remote_key]], auto_commit=False)
    wait_for_import_state_on_a(job_id, "Uncommitted")

    cnt = insert_rows(primary, coll, batch2_ids)
    assert cnt == INSERT_BATCH_SIZE
    primary.delete(collection_name=coll, filter=f"id in {batch2_ids[:INSERT_DELETE_COUNT]}")
    time.sleep(1)
    primary.delete(collection_name=coll, filter=f"id in {pre_commit_del_ids}")
    time.sleep(1)

    p2_survivors = batch2_ids[INSERT_DELETE_COUNT:]
    # Import still Uncommitted → import evens invisible on both sides
    p2_expected = committed_before + len(p1_survivors) + len(p2_survivors)
    _assert_consistent(primary, standby, coll, p2_expected, f"{tag} P2")

    assert _query_ids(primary, coll, pre_commit_del_ids[:5]) == [], \
        f"[{tag} P2] pre-commit-deleted import evens should be invisible on A (Uncommitted)"
    assert _query_ids(standby, coll, pre_commit_del_ids[:5]) == [], \
        f"[{tag} P2] pre-commit-deleted import evens should be invisible on B (Uncommitted)"
    assert _query_ids(primary, coll, post_commit_del_ids[:5]) == [], \
        f"[{tag} P2] other import evens should also be invisible on A (Uncommitted)"
    assert _query_ids(standby, coll, post_commit_del_ids[:5]) == [], \
        f"[{tag} P2] other import evens should also be invisible on B (Uncommitted)"

    # =====================================================================
    # PHASE 3: Committed + inserts + post-commit delete on evens
    # =====================================================================
    logger.info(f"  [{tag}] Phase 3: import commit + inserts + post-commit delete on evens")
    import_commit_on_a(job_id)
    wait_for_import_done_on_a(job_id)

    cnt = insert_rows(primary, coll, batch3_ids)
    assert cnt == INSERT_BATCH_SIZE
    primary.delete(collection_name=coll, filter=f"id in {batch3_ids[:INSERT_DELETE_COUNT]}")
    time.sleep(1)
    primary.delete(collection_name=coll, filter=f"id in {post_commit_del_ids}")
    time.sleep(2)

    # Round net: 20 + 20 + 20 + 300 = 360
    #   pre-commit-deleted evens (300): PRESENT  (MVCC: delete_ts < commit_ts = no-op)
    #   post-commit-deleted evens (200): GONE   (MVCC: delete_ts > commit_ts = effective)
    expected_p3 = committed_before + ROUND_CONTRIBUTION
    _assert_consistent(primary, standby, coll, expected_p3, f"{tag} P3")

    # Pre-commit-deleted evens should be PRESENT on both
    sample_pre = pre_commit_del_ids[:5]
    assert _query_ids(primary, coll, sample_pre) == sample_pre, \
        f"[{tag} P3] pre-commit-deleted evens {sample_pre} should be PRESENT on A (MVCC)"
    assert _query_ids(standby, coll, sample_pre) == sample_pre, \
        f"[{tag} P3] pre-commit-deleted evens {sample_pre} should be PRESENT on B (MVCC)"

    # Post-commit-deleted evens should be GONE on both
    sample_post = post_commit_del_ids[:5]
    assert _query_ids(primary, coll, sample_post) == [], \
        f"[{tag} P3] post-commit-deleted evens {sample_post} should be gone on A"
    assert _query_ids(standby, coll, sample_post) == [], \
        f"[{tag} P3] post-commit-deleted evens {sample_post} should be gone on B"

    return ROUND_CONTRIBUTION


def run_fresh_round(round_num, start_time):
    coll = f"test_import_repl_ins_del_fresh_{round_num}"
    primary, standby = cluster_A_client, cluster_B_client
    logger.info(f"===== Round {round_num} [FRESH] start (elapsed: {_elapsed_str(start_time)}) =====")

    drop_if_exists(primary, coll, "A")
    drop_if_exists(standby, coll, "B")
    setup_collection(coll, primary, standby)

    _run_round(round_num, start_time, coll, primary, standby, offset=0, committed_before=0)

    cleanup_collection(coll, primary, standby)
    logger.info(f"===== Round {round_num} [FRESH] PASSED =====")


def run_accumulate_round(round_num, start_time, state):
    primary, standby = cluster_A_client, cluster_B_client
    logger.info(f"===== Round {round_num} [ACCUMULATE] start (elapsed: {_elapsed_str(start_time)}) =====")
    logger.info(f"  State: committed_total={state.committed_total}, next_offset={state.next_offset}")

    if not state.created:
        drop_if_exists(primary, ACCUM_COLLECTION, "A")
        drop_if_exists(standby, ACCUM_COLLECTION, "B")
        setup_collection(ACCUM_COLLECTION, primary, standby)
        state.created = True

    offset = state.next_offset
    round_contribution = _run_round(
        round_num, start_time, ACCUM_COLLECTION, primary, standby,
        offset=offset, committed_before=state.committed_total,
    )

    state.committed_total += round_contribution
    state.next_offset += 1000  # each round consumes IDs [offset, offset+1000)

    logger.info(
        f"===== Round {round_num} [ACCUMULATE] PASSED: "
        f"committed_total={state.committed_total}, next_offset={state.next_offset} ====="
    )


def test_import_replication_insert_delete():
    logger.info(f"========== START (duration={DURATION_MINUTES}m) ==========")
    init_replication_a_to_b()

    start = time.time()
    deadline = start + DURATION_MINUTES * 60
    state = AccumulateState()
    round_num = 0
    fresh_count = 0
    accum_count = 0

    try:
        while time.time() < deadline:
            round_num += 1
            if round_num % 2 == 1:
                run_fresh_round(round_num, start)
                fresh_count += 1
            else:
                run_accumulate_round(round_num, start, state)
                accum_count += 1
    except Exception:
        logger.error(
            f"Round {round_num} FAILED after {_elapsed_str(start)}; "
            f"{round_num - 1} rounds completed "
            f"({fresh_count} fresh, {accum_count} accumulate)"
        )
        try:
            if state.created:
                cleanup_collection(ACCUM_COLLECTION, cluster_A_client, cluster_B_client)
        except Exception:
            pass
        raise

    if state.created:
        cleanup_collection(ACCUM_COLLECTION, cluster_A_client, cluster_B_client)

    logger.info(
        f"========== COMPLETE: {_elapsed_str(start)}, {round_num} rounds "
        f"({fresh_count} fresh, {accum_count} accumulate) =========="
    )
    logger.info("PASSED: test_import_replication_insert_delete")


if __name__ == "__main__":
    test_import_replication_insert_delete()
