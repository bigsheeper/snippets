import time
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
from pymilvus import MilvusClient

# Common constants
TIMEOUT = 180
CLUSTER_A_ADDR = "http://localhost:19530"
CLUSTER_B_ADDR = "http://localhost:19531"
TOKEN = "root:Milvus"
CLUSTER_A_ID = "by-dev1"
CLUSTER_B_ID = "by-dev2"

# Initialize clients
cluster_A_client = MilvusClient(uri=CLUSTER_A_ADDR, token=TOKEN)
cluster_B_client = MilvusClient(uri=CLUSTER_B_ADDR, token=TOKEN)


def reconnect_clients():
    """Reconnect both clients after cluster restart (gRPC connections go stale)."""
    global cluster_A_client, cluster_B_client
    try:
        cluster_A_client.close()
    except Exception:
        pass
    try:
        cluster_B_client.close()
    except Exception:
        pass
    cluster_A_client = MilvusClient(uri=CLUSTER_A_ADDR, token=TOKEN)
    cluster_B_client = MilvusClient(uri=CLUSTER_B_ADDR, token=TOKEN)
    logger.info("Reconnected both clients")


def generate_pchannels(cluster_id: str, pchannel_num: int):
    return [f"{cluster_id}-rootcoord-dml_{i}" for i in range(pchannel_num)]


def build_replicate_config(source_id, target_id, pchannel_num):
    """Build replicate config with given topology and pchannel count."""
    cluster_a_pchannels = generate_pchannels(CLUSTER_A_ID, pchannel_num)
    cluster_b_pchannels = generate_pchannels(CLUSTER_B_ID, pchannel_num)

    return {
        "clusters": [
            {
                "cluster_id": CLUSTER_A_ID,
                "connection_param": {
                    "uri": CLUSTER_A_ADDR,
                    "token": TOKEN,
                },
                "pchannels": cluster_a_pchannels,
            },
            {
                "cluster_id": CLUSTER_B_ID,
                "connection_param": {
                    "uri": CLUSTER_B_ADDR,
                    "token": TOKEN,
                },
                "pchannels": cluster_b_pchannels,
            },
        ],
        "cross_cluster_topology": [
            {
                "source_cluster_id": source_id,
                "target_cluster_id": target_id,
            }
        ],
    }


def update_replicate_config(source_id, target_id, pchannel_num):
    """Update replicate config on both clusters."""
    config = build_replicate_config(source_id, target_id, pchannel_num)
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_a = executor.submit(cluster_A_client.update_replicate_configuration, **config)
        future_b = executor.submit(cluster_B_client.update_replicate_configuration, **config)
        future_a.result()
        logger.info(
            f"Replicate config updated to A: {source_id} -> {target_id}, "
            f"pchannels={pchannel_num}"
        )
        future_b.result()
        logger.info(
            f"Replicate config updated to B: {source_id} -> {target_id}, "
            f"pchannels={pchannel_num}"
        )


def get_primary_and_standby(source_id):
    """Return (primary_client, standby_client) based on current source."""
    if source_id == CLUSTER_A_ID:
        return cluster_A_client, cluster_B_client
    else:
        return cluster_B_client, cluster_A_client
