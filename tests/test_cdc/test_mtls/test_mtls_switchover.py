"""
CDC mTLS E2E Test — Phase 4: Switchover (B becomes primary).

After Phase 3 (A → B, A → C) completes:
  1. Reconfigure topology: B → A, B → C
  2. CDC now uses cluster-b-client cert for outbound connections
  3. Create collection on B, verify replication to A and C
  4. Insert data, verify consistency across all 3 clusters
  5. Cleanup

This validates that:
  - Per-cluster client certificates work (B's cert is different from A's)
  - Switchover reconfiguration works over mTLS
  - CDC on B can authenticate to A and C using B's own client cert

Usage:
  python test_mtls_switchover.py
"""
import traceback
from loguru import logger
from common import (
    CLUSTER_A_ID, CLUSTER_B_ID, CLUSTER_C_ID,
    CLUSTER_B_CLIENT_PEM, CLUSTER_B_CLIENT_KEY,
    cluster_A_client, cluster_B_client, cluster_C_client,
    update_replicate_config,
)
from collection_helpers import (
    setup_collection, insert_and_verify, cleanup_collection,
)

COLLECTION_NAME = "test_mtls_switchover"


def main():
    # ---- Phase 4: Switchover — B becomes primary ----
    logger.info("=" * 60)
    logger.info("Phase 4: Switchover — B becomes primary (mTLS)")
    logger.info("=" * 60)

    logger.info("Reconfiguring topology: B → A, B → C (using B's client cert)")
    update_replicate_config(
        source_id=CLUSTER_B_ID,
        target_ids=[CLUSTER_A_ID, CLUSTER_C_ID],
        client_pem=CLUSTER_B_CLIENT_PEM,
        client_key=CLUSTER_B_CLIENT_KEY,
    )
    logger.info("Replicate config updated: B → A, B → C")

    # B is now primary; A and C are standbys
    standbys = [cluster_A_client, cluster_C_client]

    # Step 1: Create collection on B, wait for A and C
    logger.info("Step 1: Create collection on B")
    setup_collection(COLLECTION_NAME, cluster_B_client, standbys)

    # Step 2: Insert 1000 rows on B, verify on A and C
    logger.info("Step 2: Insert 1000 rows on B, verify replication")
    next_id = insert_and_verify(
        COLLECTION_NAME, cluster_B_client, standbys,
        start_id=1, count=1000,
    )

    # Step 3: Insert 1000 more rows, verify total 2000
    logger.info("Step 3: Insert 1000 more rows, verify total 2000")
    insert_and_verify(
        COLLECTION_NAME, cluster_B_client, standbys,
        start_id=next_id, count=1000,
    )

    # Step 4: Cleanup
    logger.info("Step 4: Cleanup — drop collection on B, verify propagation")
    cleanup_collection(COLLECTION_NAME, cluster_B_client, standbys)

    logger.info("=" * 60)
    logger.info("Phase 4 PASSED: Switchover verified (B → A, B → C with B's cert)")
    logger.info("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.error(traceback.format_exc())
        raise
