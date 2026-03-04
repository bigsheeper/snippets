"""
CDC mTLS E2E Test -- Switchover (B becomes primary).

After replication (A -> B, A -> C) is established:
  1. Reconfigure topology: B -> A, B -> C
  2. Create collection on B, verify replication to A and C
  3. Insert data, verify consistency across all 3 clusters
  4. Cleanup

CDC reads per-cluster client certs from paramtable via
tls.clusters.<clusterID>.{caPemPath,clientPemPath,clientKeyPath}, injected
by start_clusters.sh. The replicate config only carries uri + token.

This validates that:
  - Switchover reconfiguration works over mTLS
  - CDC on each cluster can authenticate to others using per-cluster certs

Usage:
  python test_mtls_switchover.py
"""
import os
import traceback
from loguru import logger
from common import (
    CLUSTER_A_ID, CLUSTER_B_ID, CLUSTER_C_ID,
    cluster_A_client, cluster_B_client, cluster_C_client,
    update_replicate_config,
    verify_per_cluster_certs_in_cdc_log,
)
from collection_helpers import (
    setup_collection, insert_and_verify, cleanup_collection,
)

COLLECTION_NAME = "test_mtls_switchover"


def main():
    # ---- Switchover -- B becomes primary ----
    logger.info("=" * 60)
    logger.info("Switchover -- B becomes primary (mTLS)")
    logger.info("=" * 60)

    logger.info("Reconfiguring topology: B -> A, B -> C")
    update_replicate_config(
        source_id=CLUSTER_B_ID,
        target_ids=[CLUSTER_A_ID, CLUSTER_C_ID],
    )
    logger.info("Replicate config updated: B -> A, B -> C")

    # Verify B's CDC uses correct per-cluster certs
    log_dir = os.environ.get("MILVUS_LOG_DIR", "")
    if not log_dir:
        vol_dir = os.environ.get("MILVUS_VOLUME_DIRECTORY", "")
        ws_tag = os.environ.get("WORKSPACE_TAG", "")
        if vol_dir and ws_tag:
            log_dir = os.path.join(vol_dir, ws_tag, "milvus-logs")

    if log_dir and os.path.isdir(log_dir):
        cdc_log = os.path.join(log_dir, "b-cdc.stdout.log")
        if os.path.exists(cdc_log):
            verify_per_cluster_certs_in_cdc_log(
                cdc_log, CLUSTER_B_ID, [CLUSTER_A_ID, CLUSTER_C_ID]
            )
            logger.info("Per-cluster cert differentiation verified for B's CDC")
        else:
            logger.warning(f"CDC log not found: {cdc_log}, skipping cert verification")
    else:
        logger.warning("MILVUS_LOG_DIR not set, skipping per-cluster cert verification")

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
    logger.info("Step 4: Cleanup -- drop collection on B, verify propagation")
    cleanup_collection(COLLECTION_NAME, cluster_B_client, standbys)

    logger.info("=" * 60)
    logger.info("PASSED: Switchover verified (B -> A, B -> C)")
    logger.info("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.error(traceback.format_exc())
        raise
