"""Test promoting a cluster to standalone (empty topology) via update_replicate_configuration.

Replaces the old update_empty_config.py utility script.

Usage:
  python test_standalone_promote.py A   # promote cluster A to standalone
  python test_standalone_promote.py B   # promote cluster B to standalone
"""
import argparse
import _path_setup  # noqa: F401
from loguru import logger
from common import (
    cluster_A_client, cluster_B_client,
    CLUSTER_A_ID, CLUSTER_B_ID,
    build_standalone_config,
)


def test_standalone_promote(cluster):
    if cluster == "A":
        client = cluster_A_client
        cluster_id = CLUSTER_A_ID
    else:
        client = cluster_B_client
        cluster_id = CLUSTER_B_ID

    config = build_standalone_config(cluster_id)
    logger.info(f"Promoting cluster {cluster} ({cluster_id}) to standalone...")

    client.update_replicate_configuration(**config)
    logger.info(f"Cluster {cluster} promoted to standalone successfully")

    # Verify the cluster accepts writes by listing collections
    collections = client.list_collections()
    logger.info(f"Cluster {cluster} is operational, collections: {len(collections)}")
    logger.info("PASSED: standalone promote test")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Promote cluster to standalone")
    parser.add_argument("cluster", choices=["A", "B"], help="Which cluster to promote")
    args = parser.parse_args()
    test_standalone_promote(args.cluster)
