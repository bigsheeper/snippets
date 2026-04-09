"""
mTLS-specific constants and client creation for 3-cluster CDC tests.

All clusters run with tlsMode=2 (mTLS), each with its own CA.
CDC reads per-cluster client certs from tls.clusters.<clusterID>.* in milvus.yaml.
"""
import os
import re
import _path_setup  # noqa: F401
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
from pymilvus import MilvusClient
from common import (
    TIMEOUT, TOKEN,
    CLUSTER_A_ID, CLUSTER_B_ID, CLUSTER_C_ID,
    PCHANNEL_NUM,
    build_replicate_config_star, update_replicate_config_on_clients,
)

# --- mTLS Cluster Addresses ---
CLUSTER_A_ADDR = "https://localhost:19530"
CLUSTER_B_ADDR = "https://localhost:19531"
CLUSTER_C_ADDR = "https://localhost:19532"

# --- Certificate Directory ---
CERT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "certs")


def create_tls_client(uri, dev_num):
    """Create a PyMilvus client with mTLS using per-cluster certs."""
    return MilvusClient(
        uri=uri, token=TOKEN,
        ca_pem_path=os.path.join(CERT_DIR, f"ca-dev{dev_num}.pem"),
        client_pem_path=os.path.join(CERT_DIR, f"pymilvus-dev{dev_num}.pem"),
        client_key_path=os.path.join(CERT_DIR, f"pymilvus-dev{dev_num}.key"),
    )


# --- Global mTLS clients ---
cluster_A_client = create_tls_client(CLUSTER_A_ADDR, 1)
cluster_B_client = create_tls_client(CLUSTER_B_ADDR, 2)
cluster_C_client = create_tls_client(CLUSTER_C_ADDR, 3)


def reconnect_clients():
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


def update_replicate_config(source_id, target_ids, pchannel_num=PCHANNEL_NUM):
    """Update replicate config on all 3 clusters in parallel."""
    # Override cluster addrs for mTLS (https)
    from common.config import _cluster_entry
    clusters = []
    addr_map = {CLUSTER_A_ID: CLUSTER_A_ADDR, CLUSTER_B_ID: CLUSTER_B_ADDR, CLUSTER_C_ID: CLUSTER_C_ADDR}
    for cid, addr in addr_map.items():
        clusters.append(_cluster_entry(cid, addr, pchannel_num))
    topology = [{"source_cluster_id": source_id, "target_cluster_id": tid} for tid in target_ids]
    config = {"clusters": clusters, "cross_cluster_topology": topology}

    clients = [cluster_A_client, cluster_B_client, cluster_C_client]
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(c.update_replicate_configuration, **config) for c in clients]
        for f in futures:
            f.result(timeout=60)
    logger.info(f"Replicate config applied: {source_id} -> {target_ids}")


def verify_per_cluster_certs_in_cdc_log(log_path, source_id, target_ids):
    """Verify CDC log shows distinct per-cluster client certs for each target."""
    with open(log_path) as f:
        content = f.read()

    for tid in target_ids:
        pattern = rf'\["CDC outbound TLS enabled"\].*?\[targetCluster={re.escape(tid)}\].*?\[clientPemPath=([^\]]+)\]'
        matches = re.findall(pattern, content)
        if not matches:
            raise AssertionError(f"No 'CDC outbound TLS enabled' log for target {tid}")

        dev_num = tid.split("dev")[-1]
        expected_suffix = f"client-dev{dev_num}.pem"
        assert any(expected_suffix in m for m in matches), \
            f"Target {tid}: expected cert containing '{expected_suffix}', got: {matches[0]}"
        logger.info(f"Verified per-cluster cert for {tid}: {expected_suffix}")

        ca_pattern = rf'\["CDC outbound TLS enabled"\].*?\[targetCluster={re.escape(tid)}\].*?\[caPemPath=([^\]]+)\]'
        ca_matches = re.findall(ca_pattern, content)
        if ca_matches:
            expected_ca = f"ca-dev{dev_num}.pem"
            assert any(expected_ca in m for m in ca_matches), \
                f"Target {tid}: expected CA containing '{expected_ca}', got: {ca_matches[0]}"
            logger.info(f"Verified per-cluster CA for {tid}: {expected_ca}")
