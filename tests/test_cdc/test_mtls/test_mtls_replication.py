"""
CDC mTLS E2E Test -- Configure and verify replication over mTLS.

Topology: A -> B, A -> C (star with A as center)
All 3 clusters run with tlsMode=2 (mTLS).

CDC reads per-cluster client certs from milvus.yaml paramtable via
tls.clusters.<clusterID>.{caPemPath,clientPemPath,clientKeyPath}, injected
by start_clusters.sh. Each target cluster has a distinct client cert
(client-dev1.pem, client-dev2.pem, client-dev3.pem) with different CNs.
The replicate config only carries uri + token.

Test flow:
  1. Configure replicate topology A -> B, A -> C
  2. Verify CDC uses correct per-cluster cert for each target
  3. Create collection on A, wait for B and C
  4. Insert 1000 rows on A, verify replication to B and C
  5. Insert 1000 more rows, verify total 2000 on B and C
  6. Drop collection on A, verify dropped on B and C

Usage:
  python test_mtls_replication.py
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

COLLECTION_NAME = "test_mtls_replication"


def main():
    # ---- Configure CDC replication A -> B, A -> C ----
    logger.info("=" * 60)
    logger.info("Configure CDC replication A -> B, A -> C (mTLS)")
    logger.info("=" * 60)

    update_replicate_config(
        source_id=CLUSTER_A_ID,
        target_ids=[CLUSTER_B_ID, CLUSTER_C_ID],
    )
    logger.info("Replicate config applied to all 3 clusters")

    # ---- Verify per-cluster cert differentiation ----
    log_dir = os.environ.get("MILVUS_LOG_DIR", "")
    if not log_dir:
        # Infer from MILVUS_VOLUME_DIRECTORY + WORKSPACE_TAG
        vol_dir = os.environ.get("MILVUS_VOLUME_DIRECTORY", "")
        ws_tag = os.environ.get("WORKSPACE_TAG", "")
        if vol_dir and ws_tag:
            log_dir = os.path.join(vol_dir, ws_tag, "milvus-logs")

    if log_dir and os.path.isdir(log_dir):
        cdc_log = os.path.join(log_dir, "a-cdc.stdout.log")
        if os.path.exists(cdc_log):
            verify_per_cluster_certs_in_cdc_log(
                cdc_log, CLUSTER_A_ID, [CLUSTER_B_ID, CLUSTER_C_ID]
            )
            logger.info("Per-cluster cert differentiation verified in CDC log")
        else:
            logger.warning(f"CDC log not found: {cdc_log}, skipping cert verification")
    else:
        logger.warning("MILVUS_LOG_DIR not set, skipping per-cluster cert verification")

    # ---- Verify replication over mTLS ----
    logger.info("=" * 60)
    logger.info("Verify replication over mTLS")
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

    # Step 4: Cleanup -- drop collection on A, verify propagation
    logger.info("Step 4: Cleanup -- drop collection, verify propagation")
    cleanup_collection(COLLECTION_NAME, cluster_A_client, standbys)

    logger.info("=" * 60)
    logger.info("PASSED: mTLS replication verified (A -> B, A -> C)")
    logger.info("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.error(traceback.format_exc())
        raise
