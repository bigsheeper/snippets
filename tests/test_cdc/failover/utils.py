"""Shared failover test utilities."""
import time
from loguru import logger
import _path_setup  # noqa: F401
from common import (
    cluster_A_client, cluster_B_client,
    CLUSTER_A_ID, CLUSTER_B_ID, CLUSTER_B_PROXY_HEALTH,
    build_replicate_config_2, build_standalone_config,
    stop_cluster_b, start_cluster_b, wait_for_health,
    start_async, wait_for_row_count,
)

REPL_A_TO_B = build_replicate_config_2(CLUSTER_A_ID, CLUSTER_B_ID)
STANDALONE_A = build_standalone_config(CLUSTER_A_ID)
STANDALONE_B = build_standalone_config(CLUSTER_B_ID)


def ensure_secondary_b():
    """Reset B to secondary: write config, restart."""
    logger.info("Resetting B to secondary via config + restart")
    cluster_B_client.update_replicate_configuration(**REPL_A_TO_B)
    cluster_A_client.update_replicate_configuration(**REPL_A_TO_B)
    stop_cluster_b()
    time.sleep(2)
    start_cluster_b()
    try:
        wait_for_health(CLUSTER_B_PROXY_HEALTH, "Cluster B")
    except Exception:
        pass


def init_replication_a_to_b():
    cluster_B_client.update_replicate_configuration(**REPL_A_TO_B)
    cluster_A_client.update_replicate_configuration(**REPL_A_TO_B)
    logger.info("Replication initialized: A -> B")


def force_promote_b():
    cluster_B_client.update_replicate_configuration(
        **STANDALONE_B, force_promote=True)
    logger.info("Force promote called on B")


def await_rows(client, collection, count, timeout=120):
    wait_for_row_count(client, collection, count, timeout=timeout)
