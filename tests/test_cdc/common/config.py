"""Replicate configuration builders and update helpers."""
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
from common.constants import (
    CLUSTER_A_ID, CLUSTER_B_ID, CLUSTER_C_ID,
    CLUSTER_A_ADDR, CLUSTER_B_ADDR, CLUSTER_C_ADDR,
    TOKEN, PCHANNEL_NUM,
)


def generate_pchannels(cluster_id, pchannel_num=PCHANNEL_NUM):
    return [f"{cluster_id}-rootcoord-dml_{i}" for i in range(pchannel_num)]


def normalize_http_uri(addr):
    """Normalize addr to http(s):// for replicate configuration."""
    if addr.startswith("tcp://"):
        return "http://" + addr[len("tcp://"):]
    if addr.startswith("http://") or addr.startswith("https://"):
        return addr
    if "://" not in addr:
        return "http://" + addr
    return addr


def _cluster_entry(cluster_id, addr, pchannel_num=PCHANNEL_NUM, token=TOKEN):
    return {
        "cluster_id": cluster_id,
        "connection_param": {"uri": normalize_http_uri(addr), "token": token},
        "pchannels": generate_pchannels(cluster_id, pchannel_num),
    }


# --- Pre-built cluster address map ---
CLUSTER_ADDRS = {
    CLUSTER_A_ID: CLUSTER_A_ADDR,
    CLUSTER_B_ID: CLUSTER_B_ADDR,
    CLUSTER_C_ID: CLUSTER_C_ADDR,
}


def build_replicate_config_2(source_id, target_id, pchannel_num=PCHANNEL_NUM):
    """Build 2-cluster replicate config (A <-> B)."""
    return {
        "clusters": [
            _cluster_entry(CLUSTER_A_ID, CLUSTER_A_ADDR, pchannel_num),
            _cluster_entry(CLUSTER_B_ID, CLUSTER_B_ADDR, pchannel_num),
        ],
        "cross_cluster_topology": [
            {"source_cluster_id": source_id, "target_cluster_id": target_id},
        ],
    }


def build_standalone_config(cluster_id, addr=None, pchannel_num=PCHANNEL_NUM):
    """Build standalone (single-cluster, no topology) config for force promote."""
    if addr is None:
        addr = CLUSTER_ADDRS.get(cluster_id, CLUSTER_B_ADDR)
    return {
        "clusters": [_cluster_entry(cluster_id, addr, pchannel_num)],
        "cross_cluster_topology": [],
    }


def build_replicate_config_star(source_id, target_ids, pchannel_num=PCHANNEL_NUM):
    """Build star-topology replicate config (1 source -> N targets).

    Includes ALL known clusters in the clusters list.
    """
    clusters = [
        _cluster_entry(cid, addr, pchannel_num)
        for cid, addr in CLUSTER_ADDRS.items()
        if cid == source_id or cid in target_ids
    ]
    topology = [
        {"source_cluster_id": source_id, "target_cluster_id": tid}
        for tid in target_ids
    ]
    return {"clusters": clusters, "cross_cluster_topology": topology}


def update_replicate_config_on_clients(clients, config):
    """Update replicate config on multiple clients in parallel."""
    with ThreadPoolExecutor(max_workers=len(clients)) as executor:
        futures = [executor.submit(c.update_replicate_configuration, **config) for c in clients]
        for f in futures:
            f.result(timeout=60)
    logger.info("Replicate config applied to all clients")


def init_replication_a_to_b(pchannel_num=PCHANNEL_NUM):
    """Initialize replication A -> B (update secondary first, then primary)."""
    from common.clients import cluster_A_client, cluster_B_client
    config = build_replicate_config_2(CLUSTER_A_ID, CLUSTER_B_ID, pchannel_num)
    cluster_B_client.update_replicate_configuration(**config)
    cluster_A_client.update_replicate_configuration(**config)
    logger.info("Replication initialized: A -> B")


def get_primary_and_standby(source_id):
    """Return (primary_client, standby_client) based on source cluster ID."""
    from common.clients import cluster_A_client, cluster_B_client
    if source_id == CLUSTER_A_ID:
        return cluster_A_client, cluster_B_client
    return cluster_B_client, cluster_A_client
