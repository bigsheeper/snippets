"""Cluster lifecycle: stop, start, restart, health check."""
import os
import subprocess
import time
from loguru import logger
from common.constants import MILVUS_CONTROL, HEALTH_CHECK_TIMEOUT, HEALTH_CHECK_INTERVAL
from common.port_layout import CLUSTER_A_PROXY_HEALTH, CLUSTER_B_PROXY_HEALTH


def _run(cmd, env_extra=None):
    env = os.environ.copy()
    if env_extra:
        env.update(env_extra)
    logger.info(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True, env=env, capture_output=True, text=True)
    if result.stdout.strip():
        logger.info(result.stdout.strip())
    if result.stderr.strip():
        logger.warning(result.stderr.strip())
    return result


def wait_for_health(metrics_url, label, timeout=HEALTH_CHECK_TIMEOUT):
    """Poll health endpoint until OK or timeout."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            result = subprocess.run(
                f"curl -s {metrics_url}/healthz",
                shell=True, capture_output=True, text=True, timeout=5,
            )
            if result.stdout.strip() == "OK":
                logger.info(f"{label} is healthy ({time.time() - start:.0f}s)")
                return True
        except Exception:
            pass
        time.sleep(HEALTH_CHECK_INTERVAL)
    raise TimeoutError(f"{label} failed health check after {timeout}s")


def stop_cluster_a():
    logger.info("Stopping cluster A...")
    _run(f"{MILVUS_CONTROL} -m -c -s -f --primary stop_milvus")
    time.sleep(2)


def stop_cluster_b():
    logger.info("Stopping cluster B...")
    _run(f"{MILVUS_CONTROL} -m -c -s -f --standby stop_milvus")
    time.sleep(2)


def start_cluster_a(dml_channel_num=16):
    logger.info(f"Starting cluster A with dmlChannelNum={dml_channel_num}...")
    _run(
        f"{MILVUS_CONTROL} -m -c -s -u --primary start_milvus",
        env_extra={
            "MILVUS_STREAMING_SERVICE_ENABLED": "1",
            "ROOTCOORD_DMLCHANNELNUM": str(dml_channel_num),
        },
    )
    wait_for_health(CLUSTER_A_PROXY_HEALTH, "Cluster A")


def start_cluster_b(dml_channel_num=16, replica_number=2):
    logger.info(f"Starting cluster B with dmlChannelNum={dml_channel_num}, replica={replica_number}...")
    _run(
        f"{MILVUS_CONTROL} -m -c -s -u --standby start_milvus",
        env_extra={
            "MILVUS_STREAMING_SERVICE_ENABLED": "1",
            "ROOTCOORD_DMLCHANNELNUM": str(dml_channel_num),
            "QUERYCOORD_CLUSTERLEVELLOADREPLICANUMBER": str(replica_number),
        },
    )
    wait_for_health(CLUSTER_B_PROXY_HEALTH, "Cluster B")


def restart_both_clusters(dml_channel_num=16, cluster_b_replica_number=2):
    logger.info(f"=== Restarting both clusters: dmlChannelNum={dml_channel_num} ===")
    stop_cluster_a()
    stop_cluster_b()
    time.sleep(3)
    start_cluster_a(dml_channel_num=dml_channel_num)
    start_cluster_b(dml_channel_num=dml_channel_num, replica_number=cluster_b_replica_number)
    logger.info(f"=== Both clusters restarted ===")
