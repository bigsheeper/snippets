---
name: Failover Tests Without Pytest
description: Remove pytest usage from failover tests, make them runnable via python, and lock incomplete broadcast DDL test into a fixed scenario flow.
---

# Failover Tests Without Pytest — Design

## Scope & Goals
- Remove pytest dependencies from `snippets/tests/test_cdc/failover/` tests.
- Make each test runnable directly via `python <file>.py` from the failover directory.
- Convert `test_incomplete_broadcast_ddl.py` into a **single fixed flow** (no optional DDL parameterization).
- Keep changes minimal and localized; reuse existing helpers.

## Non-Goals
- No new test frameworks or runners.
- No behavior changes outside failover tests.
- No refactors of shared `testcases/` helpers unless strictly required by direct-run flow.

## Files In Scope
- `snippets/tests/test_cdc/failover/test_restart_b_during_force_promote.py`
- `snippets/tests/test_cdc/failover/test_restart_a_during_force_promote.py`
- `snippets/tests/test_cdc/failover/test_fastlock_contention.py`
- `snippets/tests/test_cdc/failover/test_incomplete_broadcast_ddl.py`
- `snippets/tests/test_cdc/failover/conftest.py` (remove pytest fixtures or delete file)
- `snippets/tests/test_cdc/failover/README.md` (update run instructions)

## Design
### 1) Remove pytest usage
- Delete `import pytest` and any `pytestmark` or `@pytest.fixture` usage.
- Ensure each file can run directly without pytest.
- `conftest.py` becomes unused; remove it or leave as an empty/no-op if preferred.

### 2) Standardize direct execution
- Each test file exposes a single entry function (`main()` or existing test function) and runs it under `if __name__ == "__main__":`.
- Keep setup/teardown explicit function calls, not fixtures.

### 3) Fixed flow for incomplete broadcast DDL
`test_incomplete_broadcast_ddl.py` becomes a single deterministic scenario:
1. create collection
2. create index
3. load collection
4. release collection
5. force promote / failover
6. create new collection (on B)
7. load new collection
8. insert & verify
9. teardown

Notes:
- Use existing `utils` and `testcases` helpers where possible.
- Avoid optional DDL selection and branching.

## Error Handling
- Keep current tolerant behavior for NotFound scenarios (already handled in helpers).
- Do not introduce complex retries beyond existing timeouts.

## Readability
- Keep file headers consistent and minimal.
- Prefer linear, imperative flow for the fixed DDL test.

## Testing/Verification
- Manual execution from the failover directory:
  - `python test_restart_b_during_force_promote.py`
  - `python test_restart_a_during_force_promote.py`
  - `python test_fastlock_contention.py`
  - `python test_incomplete_broadcast_ddl.py`

## Risks
- Removing pytest fixtures requires explicit setup/teardown in direct-run paths.
- Any missing setup will cause intermittent failures; verify all tests call setup/teardown locally.
