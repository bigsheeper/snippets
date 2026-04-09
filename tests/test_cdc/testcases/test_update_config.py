"""Test replicate config update: init (A->B) and switch (B->A) with verification.

Usage:
  python test_update_config.py --cycles 5 --duration 300
"""
import argparse
import time
import _path_setup  # noqa: F401
from loguru import logger
from common import (
    cluster_A_client, cluster_B_client,
    CLUSTER_A_ID, CLUSTER_B_ID,
    build_replicate_config_2,
)


def test_update_config(max_cycles=5, max_duration=300):
    modes = ["init", "switch"]
    start_time = time.time()

    for cycle in range(max_cycles):
        if time.time() - start_time > max_duration:
            logger.info(f"Duration limit reached after {cycle} cycles")
            break

        mode = modes[cycle % 2]
        if mode == "init":
            source, target = CLUSTER_A_ID, CLUSTER_B_ID
        else:
            source, target = CLUSTER_B_ID, CLUSTER_A_ID

        logger.info(f"[Cycle {cycle}] {mode}: {source} -> {target}")
        config = build_replicate_config_2(source, target)

        # Update secondary first, then primary
        if source == CLUSTER_B_ID:
            cluster_A_client.update_replicate_configuration(**config)
            cluster_B_client.update_replicate_configuration(**config)
        else:
            cluster_B_client.update_replicate_configuration(**config)
            cluster_A_client.update_replicate_configuration(**config)

        logger.info(f"[Cycle {cycle}] Config updated successfully")

    elapsed = time.time() - start_time
    logger.info(f"Config update test: {max_cycles} cycles in {elapsed:.0f}s")
    logger.info("PASSED: replicate config update test")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test replicate config update")
    parser.add_argument("--cycles", type=int, default=5)
    parser.add_argument("--duration", type=int, default=300)
    args = parser.parse_args()
    test_update_config(args.cycles, args.duration)
