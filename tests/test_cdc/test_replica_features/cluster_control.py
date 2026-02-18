"""
Cluster lifecycle control: stop, restart with new config, health check.
Uses milvus_control script for process management.
"""

import os
import subprocess
import time
from loguru import logger

MILVUS_CONTROL = os.path.expanduser("~/workspace/snippets/milvus_control/milvus_control")
HEALTH_CHECK_TIMEOUT = 120  # seconds
HEALTH_CHECK_INTERVAL = 2  # seconds

# Metrics ports for health check (proxy metrics in cluster+streaming mode)
CLUSTER_A_METRICS = "http://localhost:19101"
CLUSTER_B_METRICS = "http://localhost:19201"


def _run(cmd, env_extra=None):
    """Run a shell command with optional extra env vars."""
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


def _wait_for_health(metrics_url, label, timeout=HEALTH_CHECK_TIMEOUT):
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
    """Stop primary cluster (by-dev1)."""
    logger.info("Stopping cluster A (by-dev1)...")
    _run(f"{MILVUS_CONTROL} -m -c -s -f --primary stop_milvus")
    time.sleep(2)


def stop_cluster_b():
    """Stop standby cluster (by-dev2)."""
    logger.info("Stopping cluster B (by-dev2)...")
    _run(f"{MILVUS_CONTROL} -m -c -s -f --standby stop_milvus")
    time.sleep(2)


def start_cluster_a(dml_channel_num=16):
    """Start primary cluster with given pchannel count."""
    logger.info(f"Starting cluster A (by-dev1) with dmlChannelNum={dml_channel_num}...")
    env_extra = {
        "MILVUS_STREAMING_SERVICE_ENABLED": "1",
        "ROOTCOORD_DMLCHANNELNUM": str(dml_channel_num),
    }
    _run(f"{MILVUS_CONTROL} -m -c -s -u --primary start_milvus", env_extra=env_extra)
    _wait_for_health(CLUSTER_A_METRICS, "Cluster A")


def start_cluster_b(dml_channel_num=16, replica_number=2):
    """Start standby cluster with given pchannel count and local replica config."""
    logger.info(f"Starting cluster B (by-dev2) with dmlChannelNum={dml_channel_num}, localReplica={replica_number}...")
    env_extra = {
        "MILVUS_STREAMING_SERVICE_ENABLED": "1",
        "ROOTCOORD_DMLCHANNELNUM": str(dml_channel_num),
        "QUERYCOORD_CLUSTERLEVELLOADREPLICANUMBER": str(replica_number),
    }
    _run(f"{MILVUS_CONTROL} -m -c -s -u --standby start_milvus", env_extra=env_extra)
    _wait_for_health(CLUSTER_B_METRICS, "Cluster B")


def restart_both_clusters(dml_channel_num=16, cluster_b_replica_number=2):
    """Stop and restart both clusters with new pchannel count."""
    logger.info(f"=== Restarting both clusters: dmlChannelNum={dml_channel_num} ===")
    stop_cluster_a()
    stop_cluster_b()
    time.sleep(3)
    start_cluster_a(dml_channel_num=dml_channel_num)
    start_cluster_b(dml_channel_num=dml_channel_num, replica_number=cluster_b_replica_number)
    logger.info(f"=== Both clusters restarted with dmlChannelNum={dml_channel_num} ===")
