"""
Common constants and utilities for CDC mTLS E2E tests.

All 3 clusters run with tlsMode=2 (mTLS). PyMilvus test clients use one-way TLS
(CA verification only) since they are not CDC — CDC handles mTLS internally via
connection_param cert paths.
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

# --- Certificate Paths ---
CERT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "certs")
CA_PEM = os.path.join(CERT_DIR, "ca.pem")

# Per-cluster client certs (used in CDC connection_param for mTLS)
CLUSTER_A_CLIENT_PEM = os.path.join(CERT_DIR, "cluster-a-client.pem")
CLUSTER_A_CLIENT_KEY = os.path.join(CERT_DIR, "cluster-a-client.key")
CLUSTER_B_CLIENT_PEM = os.path.join(CERT_DIR, "cluster-b-client.pem")
CLUSTER_B_CLIENT_KEY = os.path.join(CERT_DIR, "cluster-b-client.key")
CLUSTER_C_CLIENT_PEM = os.path.join(CERT_DIR, "cluster-c-client.pem")
CLUSTER_C_CLIENT_KEY = os.path.join(CERT_DIR, "cluster-c-client.key")

# --- PChannel config ---
PCHANNEL_NUM = 16


def create_tls_client(uri):
    """Create a PyMilvus client with TLS CA verification (one-way TLS)."""
    return MilvusClient(uri=uri, token=TOKEN, server_pem_path=CA_PEM)


# --- Global clients (initialized at import) ---
cluster_A_client = create_tls_client(CLUSTER_A_ADDR)
cluster_B_client = create_tls_client(CLUSTER_B_ADDR)
cluster_C_client = create_tls_client(CLUSTER_C_ADDR)


def reconnect_clients():
    """Reconnect all 3 clients after cluster restart (gRPC connections go stale)."""
    global cluster_A_client, cluster_B_client, cluster_C_client
    for c in [cluster_A_client, cluster_B_client, cluster_C_client]:
        try:
            c.close()
        except Exception:
            pass
    cluster_A_client = create_tls_client(CLUSTER_A_ADDR)
    cluster_B_client = create_tls_client(CLUSTER_B_ADDR)
    cluster_C_client = create_tls_client(CLUSTER_C_ADDR)
    logger.info("Reconnected all 3 TLS clients")


def generate_pchannels(cluster_id, pchannel_num=PCHANNEL_NUM):
    """Generate pchannel names for a cluster."""
    return [f"{cluster_id}-rootcoord-dml_{i}" for i in range(pchannel_num)]


def build_replicate_config(source_id, target_ids, pchannel_num=PCHANNEL_NUM,
                           client_pem=None, client_key=None):
    """Build replicate config for star topology with mTLS cert paths.

    Args:
        source_id: Source cluster ID (center of star)
        target_ids: List of target cluster IDs
        pchannel_num: Number of pchannels per cluster
        client_pem: Client cert path for CDC outbound connections
        client_key: Client key path for CDC outbound connections
    """
    cluster_info = {
        CLUSTER_A_ID: CLUSTER_A_ADDR,
        CLUSTER_B_ID: CLUSTER_B_ADDR,
        CLUSTER_C_ID: CLUSTER_C_ADDR,
    }

    clusters = []
    for cid, addr in cluster_info.items():
        conn_param = {"uri": addr, "token": TOKEN}
        # Add TLS cert paths for CDC mTLS connections
        if client_pem and client_key:
            conn_param["ca_pem_path"] = CA_PEM
            conn_param["client_pem_path"] = client_pem
            conn_param["client_key_path"] = client_key
        clusters.append({
            "cluster_id": cid,
            "connection_param": conn_param,
            "pchannels": generate_pchannels(cid, pchannel_num),
        })

    topology = [
        {"source_cluster_id": source_id, "target_cluster_id": tid}
        for tid in target_ids
    ]

    return {"clusters": clusters, "cross_cluster_topology": topology}


def update_replicate_config(source_id, target_ids, client_pem=None,
                            client_key=None, pchannel_num=PCHANNEL_NUM):
    """Update replicate config on all 3 clusters in parallel."""
    config = build_replicate_config(source_id, target_ids, pchannel_num,
                                    client_pem, client_key)
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
