"""
Common constants and utilities for CDC mTLS E2E tests.

All 3 clusters run with tlsMode=2 (mTLS), each with its own CA and server cert
(SAN=localhost). CDC reads per-cluster client certs from paramtable via
tls.clusters.<clusterID>.{caPemPath,clientPemPath,clientKeyPath}, configured
in milvus.yaml (injected by start_clusters.sh via MILVUSCONF).

Each cluster has a separate CA (ca-dev{N}.pem), so using the wrong CA for a
target cluster will fail TLS verification. This validates that the per-cluster
TLS config selection is correct.

The replicate config only carries uri + token — no TLS fields in connection_param.
"""
import os
import time
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
from pymilvus import MilvusClient

# --- Timeouts ---
TIMEOUT = 180

# --- Auth ---
TOKEN = "root:Milvus"

# --- Cluster IDs ---
CLUSTER_A_ID = "by-dev1"
CLUSTER_B_ID = "by-dev2"
CLUSTER_C_ID = "by-dev3"

# --- Cluster Addresses (TLS) ---
CLUSTER_A_ADDR = "https://localhost:19530"
CLUSTER_B_ADDR = "https://localhost:19531"
CLUSTER_C_ADDR = "https://localhost:19532"

# --- Certificate Directory ---
CERT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "certs")

# --- PChannel config ---
PCHANNEL_NUM = 16


def create_tls_client(uri, dev_num):
    """Create a PyMilvus client with mTLS (mutual TLS) using per-cluster certs.

    Args:
        uri: Cluster address (e.g. https://localhost:19530)
        dev_num: Cluster number (1, 2, or 3) — selects the matching CA and client cert

    Note: Must use ca_pem_path (not server_pem_path) to trigger PyMilvus mTLS branch
    which reads client cert/key. server_pem_path only does one-way TLS.
    """
    return MilvusClient(
        uri=uri, token=TOKEN,
        ca_pem_path=os.path.join(CERT_DIR, f"ca-dev{dev_num}.pem"),
        client_pem_path=os.path.join(CERT_DIR, f"pymilvus-dev{dev_num}.pem"),
        client_key_path=os.path.join(CERT_DIR, f"pymilvus-dev{dev_num}.key"),
    )


# --- Global clients (initialized at import) ---
cluster_A_client = create_tls_client(CLUSTER_A_ADDR, 1)
cluster_B_client = create_tls_client(CLUSTER_B_ADDR, 2)
cluster_C_client = create_tls_client(CLUSTER_C_ADDR, 3)


def reconnect_clients():
    """Reconnect all 3 clients after cluster restart (gRPC connections go stale)."""
    global cluster_A_client, cluster_B_client, cluster_C_client
    for c in [cluster_A_client, cluster_B_client, cluster_C_client]:
        try:
            c.close()
        except Exception:
            pass
    cluster_A_client = create_tls_client(CLUSTER_A_ADDR, 1)
    cluster_B_client = create_tls_client(CLUSTER_B_ADDR, 2)
    cluster_C_client = create_tls_client(CLUSTER_C_ADDR, 3)
    logger.info("Reconnected all 3 TLS clients")


def generate_pchannels(cluster_id, pchannel_num=PCHANNEL_NUM):
    """Generate pchannel names for a cluster."""
    return [f"{cluster_id}-rootcoord-dml_{i}" for i in range(pchannel_num)]


def build_replicate_config(source_id, target_ids, pchannel_num=PCHANNEL_NUM):
    """Build replicate config for star topology.

    CDC reads per-cluster client certs from paramtable via
    tls.clusters.<clusterID>.*, so connection_param only needs uri + token.

    Args:
        source_id: Source cluster ID (center of star)
        target_ids: List of target cluster IDs
        pchannel_num: Number of pchannels per cluster
    """
    cluster_addrs = {
        CLUSTER_A_ID: CLUSTER_A_ADDR,
        CLUSTER_B_ID: CLUSTER_B_ADDR,
        CLUSTER_C_ID: CLUSTER_C_ADDR,
    }

    clusters = []
    for cid, addr in cluster_addrs.items():
        clusters.append({
            "cluster_id": cid,
            "connection_param": {"uri": addr, "token": TOKEN},
            "pchannels": generate_pchannels(cid, pchannel_num),
        })

    topology = [
        {"source_cluster_id": source_id, "target_cluster_id": tid}
        for tid in target_ids
    ]

    return {"clusters": clusters, "cross_cluster_topology": topology}


def update_replicate_config(source_id, target_ids, pchannel_num=PCHANNEL_NUM):
    """Update replicate config on all 3 clusters in parallel."""
    config = build_replicate_config(source_id, target_ids, pchannel_num)
    clients = [cluster_A_client, cluster_B_client, cluster_C_client]
    labels = ["Cluster A", "Cluster B", "Cluster C"]

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(c.update_replicate_configuration, **config)
            for c in clients
        ]
        for f, label in zip(futures, labels):
            f.result()
            logger.info(f"Replicate config updated on {label}")

    logger.info(f"Replicate config applied: {source_id} -> {target_ids}")


def verify_per_cluster_certs_in_cdc_log(log_path, source_id, target_ids):
    """Verify CDC log shows distinct per-cluster client certs for each target.

    Checks that the 'CDC outbound TLS enabled' log line for each target
    references the correct per-cluster cert (client-dev{N}.pem) and the
    correct per-cluster CA (ca-dev{N}.pem).

    Args:
        log_path: Path to the CDC stdout log file
        source_id: Source cluster ID (e.g. by-dev1)
        target_ids: List of target cluster IDs to verify
    """
    import re

    with open(log_path) as f:
        content = f.read()

    for tid in target_ids:
        # Extract the clientPemPath logged for this target cluster
        pattern = rf'\["CDC outbound TLS enabled"\].*?\[targetCluster={re.escape(tid)}\].*?\[clientPemPath=([^\]]+)\]'
        matches = re.findall(pattern, content)
        if not matches:
            raise AssertionError(
                f"No 'CDC outbound TLS enabled' log found for target {tid} in {log_path}"
            )

        # Extract cluster number suffix (e.g. "1" from "by-dev1")
        dev_num = tid.split("dev")[-1]
        expected_client_suffix = f"client-dev{dev_num}.pem"
        expected_ca_suffix = f"ca-dev{dev_num}.pem"

        # Check that at least one log entry uses the correct per-cluster client cert
        found_correct = any(expected_client_suffix in m for m in matches)
        if not found_correct:
            raise AssertionError(
                f"Target {tid}: expected cert path containing '{expected_client_suffix}', "
                f"but found: {matches[0]}"
            )

        logger.info(f"Verified per-cluster client cert for {tid}: {expected_client_suffix}")

        # Check per-cluster CA if logged
        ca_pattern = rf'\["CDC outbound TLS enabled"\].*?\[targetCluster={re.escape(tid)}\].*?\[caPemPath=([^\]]+)\]'
        ca_matches = re.findall(ca_pattern, content)
        if ca_matches:
            found_ca = any(expected_ca_suffix in m for m in ca_matches)
            if not found_ca:
                raise AssertionError(
                    f"Target {tid}: expected CA path containing '{expected_ca_suffix}', "
                    f"but found: {ca_matches[0]}"
                )
            logger.info(f"Verified per-cluster CA for {tid}: {expected_ca_suffix}")
