"""Replica features test common utilities.

Re-exports from common/ plus replica-specific helpers.
"""
import _path_setup  # noqa: F401
from common import *
from common.config import build_replicate_config_2, get_primary_and_standby
from common.clients import reconnect_clients


def update_replicate_config(source_id, target_id, pchannel_num):
    """Update replicate config on both A and B clusters."""
    from common.clients import cluster_A_client, cluster_B_client
    config = build_replicate_config_2(source_id, target_id, pchannel_num=pchannel_num)
    update_replicate_config_on_clients([cluster_A_client, cluster_B_client], config)
    logger.info(f"Replicate config: {source_id} -> {target_id}, pchannels={pchannel_num}")


def get_replica_count(collection_name, client):
    """Get the number of replicas for a loaded collection."""
    try:
        replicas = client.describe_replica(collection_name)
        return len(replicas)
    except Exception as e:
        logger.warning(f"Failed to get replicas for {collection_name}: {e}")
        return -1
