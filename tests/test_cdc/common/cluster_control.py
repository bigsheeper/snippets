"""Cluster lifecycle: stop, start, restart, health check."""
import os
import subprocess
import time
from loguru import logger
from common.constants import MILVUS_CONTROL, HEALTH_CHECK_TIMEOUT, HEALTH_CHECK_INTERVAL
from common.port_layout import CLUSTER_A_PROXY_HEALTH, CLUSTER_B_PROXY_HEALTH, CLUSTER_C_PROXY_HEALTH

# Cluster C is managed by a standalone bash script (not via milvus_control).
_START_CLUSTER_C_SCRIPT = os.path.expanduser(
    "~/workspace/snippets/tests/test_cdc/test_replica_features/start_cluster_c.sh"
)


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


def _find_pids_by_etcd_rootpath(etcd_rootpath):
    """Return list of pids whose env has ETCD_ROOTPATH=<etcd_rootpath>.
    Works on both Linux (/proc) and macOS (ps eww).
    """
    pids = subprocess.run(
        ["pgrep", "-f", "milvus run"],
        capture_output=True, text=True,
    ).stdout.split()
    matched = []
    for pid in pids:
        env_output = ""
        proc_env = f"/proc/{pid}/environ"
        if os.access(proc_env, os.R_OK):
            try:
                with open(proc_env, "rb") as f:
                    env_output = f.read().decode(errors="ignore").replace("\0", "\n")
            except Exception:
                pass
        else:
            env_output = subprocess.run(
                ["ps", "eww", "-p", pid],
                capture_output=True, text=True,
            ).stdout
        if f"ETCD_ROOTPATH={etcd_rootpath}" in env_output:
            matched.append(pid)
    return matched


def stop_cluster_c():
    """Stop cluster C (by-dev3) by killing processes with ETCD_ROOTPATH=by-dev3."""
    logger.info("Stopping cluster C...")
    pids = _find_pids_by_etcd_rootpath("by-dev3")
    if not pids:
        logger.info("No cluster C processes found")
    else:
        for pid in pids:
            try:
                os.kill(int(pid), 9)
            except Exception as e:
                logger.warning(f"Failed to kill pid {pid}: {e}")
        logger.info(f"Killed cluster C pids: {pids}")
    time.sleep(2)
    # Verify none remain
    remaining = _find_pids_by_etcd_rootpath("by-dev3")
    if remaining:
        raise RuntimeError(f"Cluster C processes still alive after stop: {remaining}")


def start_cluster_c(dml_channel_num=16):
    """Start cluster C (by-dev3) via the standalone start_cluster_c.sh script."""
    logger.info(f"Starting cluster C with dmlChannelNum={dml_channel_num}...")
    _run(
        f"bash {_START_CLUSTER_C_SCRIPT}",
        env_extra={
            "MILVUS_STREAMING_SERVICE_ENABLED": "1",
            "ROOTCOORD_DMLCHANNELNUM": str(dml_channel_num),
        },
    )
    wait_for_health(CLUSTER_C_PROXY_HEALTH, "Cluster C")


def restart_all_three_clusters(dml_channel_num=16, cluster_b_replica_number=2):
    """Stop and restart A, B, C together with the same dml_channel_num."""
    logger.info(f"=== Restarting all three clusters: dmlChannelNum={dml_channel_num} ===")
    stop_cluster_a()
    stop_cluster_b()
    stop_cluster_c()
    time.sleep(3)
    start_cluster_a(dml_channel_num=dml_channel_num)
    start_cluster_b(dml_channel_num=dml_channel_num, replica_number=cluster_b_replica_number)
    start_cluster_c(dml_channel_num=dml_channel_num)
    logger.info(f"=== All three clusters restarted ===")
