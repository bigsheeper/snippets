"""Stress test: switchover + replica heterogeneity.

Each cycle: switchover (every 10 cycles), create collection with varying replicas,
insert, verify replication, check standby uses local replica config, cleanup.

Usage:
  python test_stress.py --cycles 1000 --duration 3600
"""
import argparse
import time
import traceback
import _path_setup  # noqa: F401
from loguru import logger
from common import (
    CLUSTER_A_ID, CLUSTER_B_ID, PCHANNEL_NUM,
    setup_collection, insert_and_verify, cleanup_collection,
    drop_if_exists,
)
from test_replica_features.common import (
    update_replicate_config, get_primary_and_standby, get_replica_count,
)

SWITCHOVER_INTERVAL = 10


def run_stress_test(max_cycles=1000, max_duration=None):
    source, target = CLUSTER_A_ID, CLUSTER_B_ID
    update_replicate_config(source, target, PCHANNEL_NUM)

    start_time = time.time()
    success, fail = 0, 0

    for cycle in range(max_cycles):
        if max_duration and (time.time() - start_time) > max_duration:
            logger.info(f"Time limit after {cycle} cycles")
            break

        try:
            # Switchover every N cycles
            if cycle > 0 and cycle % SWITCHOVER_INTERVAL == 0:
                source, target = target, source
                logger.info(f"[Cycle {cycle}] Switchover: {source} -> {target}")
                update_replicate_config(source, target, PCHANNEL_NUM)

            replica_num = (cycle % 3) + 1
            primary, standby = get_primary_and_standby(source)
            name = f"stress_cycle_{cycle}"

            setup_collection(name, primary, standby, replica_num=replica_num)
            insert_and_verify(name, primary, standby)

            # Check replicas
            p_replicas = get_replica_count(name, primary)
            s_replicas = get_replica_count(name, standby)
            if p_replicas != replica_num:
                logger.warning(f"[Cycle {cycle}] Primary replica: expected={replica_num}, got={p_replicas}")
                fail += 1
            if s_replicas != 1:
                logger.warning(f"[Cycle {cycle}] Standby replica: expected=1, got={s_replicas}")
                fail += 1

            cleanup_collection(name, primary, standby)
            success += 1
            logger.info(f"[Cycle {cycle}] SUCCESS")

        except Exception as e:
            fail += 1
            logger.error(f"[Cycle {cycle}] FAILED: {e}")
            logger.error(traceback.format_exc())
            primary, standby = get_primary_and_standby(source)
            drop_if_exists(primary, f"stress_cycle_{cycle}")

    logger.info(f"Stress test: {success} success, {fail} failed in {time.time() - start_time:.0f}s")
    if fail > 0:
        raise RuntimeError(f"{fail} failures")
    logger.info("PASSED: stress test")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cycles", type=int, default=1000)
    parser.add_argument("--duration", type=int, default=None)
    args = parser.parse_args()
    run_stress_test(args.cycles, args.duration)
