"""Test switchover: toggle topology A->B / B->A with data integrity checks.

Usage:
  python test_switchover.py --cycles 10 --duration 600
"""
import argparse
import time
import _path_setup  # noqa: F401
from loguru import logger
from common import (
    cluster_A_client, cluster_B_client,
    CLUSTER_A_ID, CLUSTER_B_ID,
    build_replicate_config_2, get_primary_and_standby,
    setup_collection, insert_and_verify, cleanup_collection,
)


def test_switchover(max_cycles=10, max_duration=600):
    source, target = CLUSTER_A_ID, CLUSTER_B_ID
    start_time = time.time()
    success = 0

    for cycle in range(max_cycles):
        if time.time() - start_time > max_duration:
            logger.info(f"Duration limit reached after {cycle} cycles")
            break

        # Switch topology
        if cycle > 0:
            source, target = target, source
            logger.info(f"[Cycle {cycle}] Switchover: {source} -> {target}")

        config = build_replicate_config_2(source, target)
        # Update secondary first, then primary
        if source == CLUSTER_A_ID:
            cluster_B_client.update_replicate_configuration(**config)
            cluster_A_client.update_replicate_configuration(**config)
        else:
            cluster_A_client.update_replicate_configuration(**config)
            cluster_B_client.update_replicate_configuration(**config)

        # Create, insert, verify, cleanup
        primary, standby = get_primary_and_standby(source)
        name = f"switchover_cycle_{cycle}"

        setup_collection(name, primary, standby)
        insert_and_verify(name, primary, standby, start_id=1, count=100)
        cleanup_collection(name, primary, standby)

        success += 1
        logger.info(f"[Cycle {cycle}] PASSED ({source} -> {target})")

    elapsed = time.time() - start_time
    logger.info(f"Switchover test: {success}/{max_cycles} cycles in {elapsed:.0f}s")
    assert success > 0, "No cycles completed"
    logger.info("PASSED: switchover test")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Switchover test with data integrity")
    parser.add_argument("--cycles", type=int, default=10)
    parser.add_argument("--duration", type=int, default=600)
    args = parser.parse_args()
    test_switchover(args.cycles, args.duration)
