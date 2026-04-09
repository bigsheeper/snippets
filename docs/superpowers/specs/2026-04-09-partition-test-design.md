# Partition CDC Test Design

## Goal
Add `tests/test_cdc/testcases/test_partition.py` following the orchestration style of `test_collection.py`, while validating partition-specific CDC replication between the primary and standby clusters.

## Scope
- Add a new top-level testcase file: `test_partition.py`
- Add a small partition helper module for repeated partition operations
- Reuse existing clients, collection helpers, and insert data generation
- Verify partition lifecycle replication plus data insertion into partitions

## Out of Scope
- Refactoring existing collection helpers
- Adding search/query correctness assertions beyond partition existence and lifecycle state
- Expanding unrelated CDC scenarios

## Recommended Approach
Add a dedicated `partition.py` helper module and keep `test_partition.py` as a short orchestration test, matching the current pattern used by `test_collection.py`.

Why this approach:
- Keeps the test file readable and aligned with existing testcase style
- Avoids duplicating polling and partition operations inline
- Limits changes to the smallest set of files needed

## Test Flow
1. Create a batch of collections on the primary cluster
2. Wait until those collections appear on the standby cluster
3. Create one partition per collection on the primary cluster
4. Wait until each partition appears on the standby cluster
5. Insert one batch of rows into each named partition on the primary cluster
6. Flush affected collections so CDC visibility is not timing-dependent
7. Load each partition on the primary cluster
8. Wait until the corresponding partition is loaded on the standby cluster
9. Release each partition on the primary cluster
10. Wait until the corresponding partition is released on the standby cluster
11. Drop each partition on the primary cluster
12. Wait until each partition disappears from the standby cluster
13. Drop collections on the primary cluster and wait for standby cleanup

## Helper Responsibilities
A new `partition.py` module should provide:
- Partition name generation from collection names
- `create_partition_on_primary` and batched wrapper
- `load_partition_on_primary` and batched wrapper
- `release_partition_on_primary` and batched wrapper
- `drop_partition_on_primary` and batched wrapper
- Standby polling helpers for create/load/release/drop
- A small helper to insert into a named partition using existing generated row data
- A helper to flush collections after partition inserts

## Data and Naming
- Reuse `get_collection_names()` from `collection.py`
- Derive partition names deterministically, e.g. `partition_<i>` aligned by index
- Reuse `generate_data()` or `insert`-module patterns for inserted rows
- Keep one partition per collection to match the current simple orchestration style

## Error Handling
- Follow current helper conventions: poll until success or `TIMEOUT`, then raise `TimeoutError`
- For destructive cleanup operations, tolerate benign races only if the surrounding helper pattern already does so in this test suite
- Do not add extra fallback behavior beyond what the test needs

## Testing Strategy
This is an end-to-end testcase, so validation is the test itself:
- First add `test_partition.py` as the failing test entrypoint importing a not-yet-existing `partition` helper module
- Run only that testcase to confirm the expected failure
- Add the minimum helper implementation needed to make the testcase pass
- Re-run the testcase and keep changes scoped to the new partition flow

## Files Expected To Change
- `tests/test_cdc/testcases/test_partition.py`
- `tests/test_cdc/testcases/partition.py`
- Possibly `tests/test_cdc/testcases/insert.py` only if a tiny reusable partition insert helper is needed there instead of `partition.py`

## Open Decisions Resolved
- Coverage level: use the middle-ground flow confirmed by the user: lifecycle + insert into partition
- Validation style: mirror `test_collection.py` orchestration instead of introducing abstractions or broad refactors
