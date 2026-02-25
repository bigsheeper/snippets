"""
CDC mTLS E2E Test — Phase 2 & 3: Configure and verify replication over mTLS.

Topology: A → B, A → C (star with A as center)
All 3 clusters run with tlsMode=2 (mTLS).
CDC uses cluster-a-client cert for outbound connections to B and C.

Test flow:
  1. Configure replicate topology A → B, A → C (with mTLS cert paths)
  2. Create collection on A, wait for B and C
  3. Insert 1000 rows on A, verify replication to B and C
  4. Insert 1000 more rows, verify total 2000 on B and C
  5. Drop collection on A, verify dropped on B and C

Usage:
  python test_mtls_replication.py
"""
import traceback
from loguru import logger
from common import (
    CLUSTER_A_ID, CLUSTER_B_ID, CLUSTER_C_ID,
    CLUSTER_A_CLIENT_PEM, CLUSTER_A_CLIENT_KEY,
    cluster_A_client, cluster_B_client, cluster_C_client,
    update_replicate_config,
)
from collection_helpers import (
    setup_collection, insert_and_verify, cleanup_collection,
)

COLLECTION_NAME = "test_mtls_replication"


def main():
    # ---- Phase 2: Configure CDC replication A → B, A → C ----
    logger.info("=" * 60)
    logger.info("Phase 2: Configure CDC replication A → B, A → C (mTLS)")
    logger.info("=" * 60)

    update_replicate_config(
        source_id=CLUSTER_A_ID,
        target_ids=[CLUSTER_B_ID, CLUSTER_C_ID],
        client_pem=CLUSTER_A_CLIENT_PEM,
        client_key=CLUSTER_A_CLIENT_KEY,
    )
    logger.info("Replicate config applied to all 3 clusters")

    # ---- Phase 3: Verify replication over mTLS ----
    logger.info("=" * 60)
    logger.info("Phase 3: Verify replication over mTLS")
    logger.info("=" * 60)

    standbys = [cluster_B_client, cluster_C_client]

    # Step 1: Create collection on A, wait for replication to B and C
    logger.info("Step 1: Create collection on A")
    setup_collection(COLLECTION_NAME, cluster_A_client, standbys)

    # Step 2: Insert 1000 rows on A, verify on B and C
    logger.info("Step 2: Insert 1000 rows on A, verify replication")
    next_id = insert_and_verify(
        COLLECTION_NAME, cluster_A_client, standbys,
        start_id=1, count=1000,
    )

    # Step 3: Insert 1000 more rows, verify total 2000
    logger.info("Step 3: Insert 1000 more rows, verify total 2000")
    insert_and_verify(
        COLLECTION_NAME, cluster_A_client, standbys,
        start_id=next_id, count=1000,
    )

    # Step 4: Cleanup — drop collection on A, verify propagation
    logger.info("Step 4: Cleanup — drop collection, verify propagation")
    cleanup_collection(COLLECTION_NAME, cluster_A_client, standbys)

    logger.info("=" * 60)
    logger.info("Phase 3 PASSED: mTLS replication verified (A → B, A → C)")
    logger.info("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.error(traceback.format_exc())
        raise
