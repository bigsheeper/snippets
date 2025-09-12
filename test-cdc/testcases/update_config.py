import sys
from pymilvus import MilvusClient
from common import *


def generate_pchannels(cluster_id: str, pchannel_num: int = 16):
    pchannels = []
    for i in range(pchannel_num):
        pchannel_name = f"{cluster_id}-rootcoord-dml_{i}"
        pchannels.append(pchannel_name)
    return pchannels


def print_usage():
    print("Usage:")
    print("  python update_config.py init   - Init replicate configuration: A -> B")
    print("  python update_config.py switch - Switch primary-standby: B -> A")
    print("")


def update_replicate_configuration(addr: str, config: dict):
    """Update replicate configuration for a cluster"""
    client = MilvusClient(uri=addr, token=TOKEN)
    client.update_replicate_configuration(**config)
    print("Replicate source configuration updated successfully")
    client.close()


def main():
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)

    mode = sys.argv[1]
    if mode not in ["init", "switch"]:
        print_usage()
        sys.exit(1)

    clusterA_addr = CLUSTER_A_ADDR
    clusterB_addr = CLUSTER_B_ADDR
    clusterA_id = "by-dev1"
    clusterB_id = "by-dev2"
    pchannel_num = 16

    # Generate PChannels for both clusters
    clusterA_pchannels = generate_pchannels(clusterA_id, pchannel_num)
    clusterB_pchannels = generate_pchannels(clusterB_id, pchannel_num)

    # Create replication configuration based on mode
    if mode == "init":
        print("Init replicate configuration: A -> B")
        source_cluster_id = clusterA_id
        target_cluster_id = clusterB_id
    else:  # switch
        print("Switch primary-standby: B -> A")
        source_cluster_id = clusterB_id
        target_cluster_id = clusterA_id

    # Build configuration
    config = {
        "clusters": [
            {
                "cluster_id": clusterA_id,
                "connection_param": {
                    "uri": clusterA_addr,
                    "token": TOKEN
                },
                "pchannels": clusterA_pchannels
            },
            {
                "cluster_id": clusterB_id,
                "connection_param": {
                    "uri": clusterB_addr,
                    "token": TOKEN
                },
                "pchannels": clusterB_pchannels
            }
        ],
        "cross_cluster_topology": [
            {
                "source_cluster_id": source_cluster_id,
                "target_cluster_id": target_cluster_id
            }
        ]
    }

    # Update both clusters
    update_replicate_configuration(clusterA_addr, config)
    update_replicate_configuration(clusterB_addr, config)


if __name__ == "__main__":
    main()