import sys
import time
from loguru import logger
from datetime import datetime, timedelta
from pymilvus import MilvusClient
from common import *


def generate_pchannels(cluster_id: str, pchannel_num: int = 16):
    """Generate PChannels for a cluster"""
    pchannels = []
    for i in range(pchannel_num):
        pchannel_name = f"{cluster_id}-rootcoord-dml_{i}"
        pchannels.append(pchannel_name)
    return pchannels


def update_replicate_configuration(addr: str, config: dict):
    """Update replicate configuration for a cluster"""
    client = MilvusClient(uri=addr, token=TOKEN)
    client.update_replicate_configuration(**config)
    logger.info(f"Replicate configuration updated successfully: {addr}")
    client.close()


def execute_config_update(mode: str, clusterA_addr: str, clusterB_addr: str, 
                         clusterA_id: str, clusterB_id: str, pchannel_num: int):
    """Execute configuration update based on mode"""
    logger.info(f"Executing {mode} configuration update...")
    
    # Generate PChannels for both clusters
    clusterA_pchannels = generate_pchannels(clusterA_id, pchannel_num)
    clusterB_pchannels = generate_pchannels(clusterB_id, pchannel_num)

    # Create replication configuration based on mode
    if mode == "init":
        logger.info("Init replicate configuration: A -> B")
        source_cluster_id = clusterA_id
        target_cluster_id = clusterB_id
    else:  # switch
        logger.info("Switch primary-standby: B -> A")
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
    if source_cluster_id == clusterB_id:
        update_replicate_configuration(clusterA_addr, config)
        update_replicate_configuration(clusterB_addr, config)
    else:
        update_replicate_configuration(clusterB_addr, config)
        update_replicate_configuration(clusterA_addr, config)


def main():    
    # Configuration
    clusterA_addr = CLUSTER_A_ADDR
    clusterB_addr = CLUSTER_B_ADDR
    clusterA_id = "by-dev1"
    clusterB_id = "by-dev2"
    pchannel_num = 16
    
    # Test duration: 10 minutes
    test_duration = timedelta(minutes=10)
    start_time = datetime.now()
    end_time = start_time + test_duration
    
    logger.info(f"Starting configuration switch test for {test_duration}")
    logger.info(f"Test will run from {start_time} to {end_time}")
    logger.info("No sleep - continuous switching")
    
    # Initialize mode
    current_mode = "init"
    cycle_count = 0
    
    while datetime.now() < end_time:
        cycle_count += 1
        current_time = datetime.now()
        remaining_time = end_time - current_time
        
        logger.info(f"=== Cycle {cycle_count} - Mode: {current_mode} ===")
        
        # Execute configuration update
        execute_config_update(
            current_mode, clusterA_addr, clusterB_addr,
            clusterA_id, clusterB_id, pchannel_num
        )
        
        logger.info(f"Configuration update '{current_mode}' completed")
        
        # Switch mode for next iteration
        current_mode = "switch" if current_mode == "init" else "init"
    
    logger.info(f"Test completed successfully after {cycle_count} cycles")
    logger.info("Configuration switch test finished")


if __name__ == "__main__":
    main()
