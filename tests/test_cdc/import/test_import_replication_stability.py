"""Long-running 2PC import + delete across A→B replication.

Alternates fresh rounds (new collection per round, dropped at end) and
accumulate rounds (long-lived collection accumulating state across rounds).

Unlike test_import_replication_insert_delete, there is no SDK-insert
interleaving — just import + pre-commit delete + post-commit delete +
MVCC checks. Designed to run 30+ minutes to catch slow drift / leaks.

Per-round expectations (both A and B after each state transition):

FRESH round:
  - Import 500 rows (auto_commit=false), Uncommitted:  A=B=0
  - Commit first import:                               A=B=500
  - Second import (ids 500-999), Uncommitted; issue
    pre-commit delete on [500..599]; commit.
    Pre-commit delete is a no-op:                      A=B=1000
    Sample [500, 550, 599] → present on both.
  - Post-commit delete [600..699]:                     A=B=900
    Sample [600, 650, 699] → gone on both.
    Sample [0, 50, 200, 300, 499] → present on both.

ACCUMULATE round (long-lived collection, starting at offset=next_offset):
  - Import 500 rows at offset, Uncommitted:            A=B=committed_total
  - Commit:                                            A=B=committed_total + 500
  - Post-commit delete [offset..offset+99]:            A=B=committed_total + 400
  - Sample an old-round ID (prev_offset + 250) → present on both.
  - Update state: committed_total += 400, next_offset += 500.
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
    query_all, wait_for_query_consistent,
)

generate_and_upload_parquet = imp_common.generate_and_upload_parquet
import_create_on_a = imp_common.import_create_on_a
import_commit_on_a = imp_common.import_commit_on_a
wait_for_import_state_on_a = imp_common.wait_for_import_state_on_a
wait_for_import_done_on_a = imp_common.wait_for_import_done_on_a
count_rows = imp_common.count_rows

DURATION_MINUTES = int(os.getenv("STABILITY_DURATION_MINUTES", "30"))
NUM_ROWS = 500
ACCUM_COLLECTION = "test_import_repl_stability_accum"


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
    actual_a = count_rows(primary, collection)
    assert actual_a == expected_count, \
        f"[{label}] A count expected={expected_count}, got={actual_a}"
    a_rows = query_all(primary, collection)
    wait_for_query_consistent(collection, a_rows, standby)
    logger.info(f"  [{label}] A=B={expected_count} OK")


def run_fresh_round(round_num, start_time):
    coll = f"test_import_repl_stab_fresh_{round_num}"
    primary, standby = cluster_A_client, cluster_B_client
    logger.info(f"===== Round {round_num} [FRESH] start (elapsed: {_elapsed_str(start_time)}) =====")

    drop_if_exists(primary, coll, "A")
    drop_if_exists(standby, coll, "B")
    setup_collection(coll, primary, standby)

    # --- MVCC phase: first 500 rows (ids 0..499) ---
    remote1 = generate_and_upload_parquet(NUM_ROWS, start_id=0,
                                          prefix=f"repl_stab_fresh_{round_num}_mvcc")
    job1 = import_create_on_a(coll, [[remote1]], auto_commit=False)
    wait_for_import_state_on_a(job1, "Uncommitted")
    _assert_consistent(primary, standby, coll, 0, f"Round {round_num} MVCC pre-commit")

    import_commit_on_a(job1)
    wait_for_import_done_on_a(job1)
    _assert_consistent(primary, standby, coll, NUM_ROWS,
                       f"Round {round_num} MVCC post-commit")

    # --- Delete phase: second 500 rows (ids 500..999) + pre/post-commit deletes ---
    remote2 = generate_and_upload_parquet(NUM_ROWS, start_id=500,
                                          prefix=f"repl_stab_fresh_{round_num}_del")
    job2 = import_create_on_a(coll, [[remote2]], auto_commit=False)
    wait_for_import_state_on_a(job2, "Uncommitted")

    # Pre-commit delete on ids [500..599] — should be no-op after commit
    pre_del = list(range(500, 600))
    primary.delete(collection_name=coll, filter=f"id in {pre_del}")
    time.sleep(1)

    import_commit_on_a(job2)
    wait_for_import_done_on_a(job2)
    _assert_consistent(primary, standby, coll, 2 * NUM_ROWS,
                       f"Round {round_num} DEL pre-commit no-op")

    sample_pre = [500, 550, 599]
    assert _query_ids(primary, coll, sample_pre) == sample_pre, \
        f"[Round {round_num}] pre-commit-deleted IDs should be present on A"
    assert _query_ids(standby, coll, sample_pre) == sample_pre, \
        f"[Round {round_num}] pre-commit-deleted IDs should be present on B"

    # Post-commit delete on ids [600..699] — should be effective
    post_del = list(range(600, 700))
    primary.delete(collection_name=coll, filter=f"id in {post_del}")
    _assert_consistent(primary, standby, coll, 2 * NUM_ROWS - 100,
                       f"Round {round_num} DEL post-commit effective")

    sample_post = [600, 650, 699]
    assert _query_ids(primary, coll, sample_post) == [], \
        f"[Round {round_num}] post-commit-deleted IDs should be gone on A"
    assert _query_ids(standby, coll, sample_post) == [], \
        f"[Round {round_num}] post-commit-deleted IDs should be gone on B"

    survivors = [0, 50, 200, 300, 499]
    assert _query_ids(standby, coll, survivors) == sorted(survivors), \
        f"[Round {round_num}] non-deleted IDs should be present on B"

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

    # Import 500 rows at offset
    remote = generate_and_upload_parquet(NUM_ROWS, start_id=offset,
                                         prefix=f"repl_stab_accum_{round_num}")
    job_id = import_create_on_a(ACCUM_COLLECTION, [[remote]], auto_commit=False)
    wait_for_import_state_on_a(job_id, "Uncommitted")
    _assert_consistent(primary, standby, ACCUM_COLLECTION, state.committed_total,
                       f"Round {round_num} ACCUM pre-commit")

    import_commit_on_a(job_id)
    wait_for_import_done_on_a(job_id)
    expected_after_commit = state.committed_total + NUM_ROWS
    _assert_consistent(primary, standby, ACCUM_COLLECTION, expected_after_commit,
                       f"Round {round_num} ACCUM post-commit")

    # Post-commit delete on first 100 of this batch → 400 remain
    del_ids = list(range(offset, offset + 100))
    primary.delete(collection_name=ACCUM_COLLECTION, filter=f"id in {del_ids}")
    expected_after_delete = expected_after_commit - 100
    _assert_consistent(primary, standby, ACCUM_COLLECTION, expected_after_delete,
                       f"Round {round_num} ACCUM post-delete")

    # Sample an old-round survivor. Previous accumulate round landed at
    # offset = state.next_offset - 500 (NUM_ROWS). Pick id prev_offset + 250
    # which is in the uninteresting middle, guaranteed non-deleted.
    if state.committed_total > 0:
        prev_offset = offset - NUM_ROWS
        sample_id = prev_offset + NUM_ROWS // 2  # middle of prior batch
        if sample_id >= 0:
            assert _query_ids(standby, ACCUM_COLLECTION, [sample_id]) == [sample_id], \
                f"[Round {round_num} ACCUM] old id {sample_id} should still exist on B"
            logger.info(f"  Old-round id {sample_id} still present on B")

    state.committed_total = expected_after_delete
    state.next_offset = offset + NUM_ROWS

    logger.info(
        f"===== Round {round_num} [ACCUMULATE] PASSED: "
        f"committed_total={state.committed_total}, next_offset={state.next_offset} ====="
    )


def test_import_replication_stability():
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
            f"({fresh_count} fresh, {accum_count} accumulate); "
            f"committed_total={state.committed_total}"
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
    logger.info(f"Accumulate collection final rows: {state.committed_total}")
    logger.info("PASSED: test_import_replication_stability")


if __name__ == "__main__":
    test_import_replication_stability()
