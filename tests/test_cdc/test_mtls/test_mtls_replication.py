"""CDC mTLS E2E: Configure and verify replication over mTLS (A -> B, A -> C)."""
import os
import traceback
import _path_setup  # noqa: F401
from loguru import logger
from common import setup_collection, insert_and_verify, cleanup_collection
from test_mtls.common import (
    CLUSTER_A_ID, CLUSTER_B_ID, CLUSTER_C_ID,
    cluster_A_client, cluster_B_client, cluster_C_client,
    update_replicate_config, verify_per_cluster_certs_in_cdc_log,
)

COLLECTION_NAME = "test_mtls_replication"


def main():
    logger.info("Configure CDC replication A -> B, A -> C (mTLS)")
    update_replicate_config(CLUSTER_A_ID, [CLUSTER_B_ID, CLUSTER_C_ID])

    # Verify per-cluster cert differentiation if logs available
    log_dir = os.environ.get("MILVUS_LOG_DIR", "")
    if not log_dir:
        vol_dir = os.environ.get("MILVUS_VOLUME_DIRECTORY", "")
        ws_tag = os.environ.get("WORKSPACE_TAG", "")
        if vol_dir and ws_tag:
            log_dir = os.path.join(vol_dir, ws_tag, "milvus-logs")
    if log_dir and os.path.isdir(log_dir):
        cdc_log = os.path.join(log_dir, "a-cdc.stdout.log")
        if os.path.exists(cdc_log):
            verify_per_cluster_certs_in_cdc_log(cdc_log, CLUSTER_A_ID, [CLUSTER_B_ID, CLUSTER_C_ID])
        else:
            logger.warning(f"CDC log not found: {cdc_log}")
    else:
        logger.warning("MILVUS_LOG_DIR not set, skipping cert verification")

    standbys = [cluster_B_client, cluster_C_client]

    setup_collection(COLLECTION_NAME, cluster_A_client, standbys)
    next_id = insert_and_verify(COLLECTION_NAME, cluster_A_client, standbys, start_id=1, count=1000)
    insert_and_verify(COLLECTION_NAME, cluster_A_client, standbys, start_id=next_id, count=1000)
    cleanup_collection(COLLECTION_NAME, cluster_A_client, standbys)

    logger.info("PASSED: mTLS replication (A -> B, A -> C)")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.error(traceback.format_exc())
        raise
