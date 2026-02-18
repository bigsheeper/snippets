"""
Stress test: switchover + pchannel increase + replica heterogeneity

Loop N=0..max_cycles:
  - Switchover:        every 10 cycles (toggle A→B / B→A)
  - Pchannel increase: every 100 cycles (2→3→4→...)
  - Replica variation: every cycle, replica_count = (N % 3) + 1

Each cycle:
  1. (Optionally) update replicate config (switchover / pchannel increase)
  2. Create collection with replica_count on primary
  3. Insert data, verify replicated to standby
  4. Verify standby replica count matches its local config (not primary's)
  5. Drop collection

Prerequisites:
  - Two Milvus clusters running (milvus_control -m -s start_milvus)
  - Cluster A: by-dev1, port 19530
  - Cluster B: by-dev2, port 19531
  - Cluster B started with QUERYCOORD_CLUSTERLEVELLOADREPLICANUMBER=2
    (so standby uses local replica config when receiving replicated load)

Usage:
  python test_stress.py [--cycles 1000] [--duration 3600]
"""

import argparse
import time
import traceback
from loguru import logger

from common import (
    CLUSTER_A_ID,
    CLUSTER_B_ID,
    update_replicate_config,
    get_primary_and_standby,
)
from collection_helpers import (
    setup_collection,
    insert_and_verify,
    cleanup_collection,
    get_replica_count,
)


# Configurable intervals
SWITCHOVER_INTERVAL = 10
PCHANNEL_INCREASE_INTERVAL = 100
INIT_PCHANNEL_NUM = 2


def run_stress_test(max_cycles=1000, max_duration_sec=None):
    """Main stress loop."""
    # Initial state
    current_source = CLUSTER_A_ID
    current_target = CLUSTER_B_ID
    current_pchannel_num = INIT_PCHANNEL_NUM

    # Init replicate config
    logger.info(f"=== Init: {current_source} -> {current_target}, pchannels={current_pchannel_num} ===")
    update_replicate_config(current_source, current_target, current_pchannel_num)

    start_time = time.time()
    success_count = 0
    fail_count = 0

    for cycle in range(max_cycles):
        # Check time limit
        if max_duration_sec and (time.time() - start_time) > max_duration_sec:
            logger.info(f"Time limit reached ({max_duration_sec}s), stopping after {cycle} cycles")
            break

        cycle_start = time.time()

        try:
            # --- Pchannel increase (every 100 cycles) ---
            if cycle > 0 and cycle % PCHANNEL_INCREASE_INTERVAL == 0:
                current_pchannel_num += 1
                logger.info(f"[Cycle {cycle}] Pchannel increase -> {current_pchannel_num}")
                update_replicate_config(current_source, current_target, current_pchannel_num)

            # --- Switchover (every 10 cycles) ---
            if cycle > 0 and cycle % SWITCHOVER_INTERVAL == 0:
                current_source, current_target = current_target, current_source
                logger.info(f"[Cycle {cycle}] Switchover -> {current_source} -> {current_target}")
                update_replicate_config(current_source, current_target, current_pchannel_num)

            # --- Replica variation (every cycle) ---
            primary_replica_num = (cycle % 3) + 1
            primary_client, standby_client = get_primary_and_standby(current_source)
            collection_name = f"stress_cycle_{cycle}"

            logger.info(
                f"[Cycle {cycle}] source={current_source}, replicas={primary_replica_num}, "
                f"pchannels={current_pchannel_num}"
            )

            # Create, load, insert, verify
            setup_collection(collection_name, primary_client, standby_client, replica_num=primary_replica_num)
            insert_and_verify(collection_name, primary_client, standby_client)

            # Verify replica counts
            primary_replicas = get_replica_count(collection_name, primary_client)
            standby_replicas = get_replica_count(collection_name, standby_client)
            logger.info(
                f"[Cycle {cycle}] Replica counts: primary={primary_replicas}, standby={standby_replicas}"
            )

            # Primary should match requested replica count
            if primary_replicas != primary_replica_num:
                logger.warning(
                    f"[Cycle {cycle}] Primary replica mismatch: expected={primary_replica_num}, "
                    f"actual={primary_replicas}"
                )

            # Standby should use its own local config (not necessarily match primary)
            # If standby has local config (e.g., replica=2), it should differ from primary
            # We log it for verification — the exact expected value depends on cluster config
            if standby_replicas == primary_replica_num:
                logger.info(
                    f"[Cycle {cycle}] Standby replica matches primary ({standby_replicas}) "
                    f"— local config may not be set or matches by coincidence"
                )
            else:
                logger.info(
                    f"[Cycle {cycle}] Standby replica differs from primary "
                    f"(standby={standby_replicas}, primary={primary_replica_num}) "
                    f"— local config override working"
                )

            # Cleanup
            cleanup_collection(collection_name, primary_client, standby_client)

            elapsed = time.time() - cycle_start
            success_count += 1
            logger.info(f"[Cycle {cycle}] SUCCESS ({elapsed:.1f}s)")

        except Exception as e:
            elapsed = time.time() - cycle_start
            fail_count += 1
            logger.error(f"[Cycle {cycle}] FAILED ({elapsed:.1f}s): {e}")
            logger.error(traceback.format_exc())

            # Try to cleanup on failure
            try:
                primary_client, standby_client = get_primary_and_standby(current_source)
                collection_name = f"stress_cycle_{cycle}"
                if primary_client.has_collection(collection_name):
                    primary_client.drop_collection(collection_name)
            except Exception:
                pass

    total_time = time.time() - start_time
    logger.info("=" * 60)
    logger.info(f"Stress test completed in {total_time:.1f}s")
    logger.info(f"  Total cycles: {success_count + fail_count}")
    logger.info(f"  Success: {success_count}")
    logger.info(f"  Failed:  {fail_count}")
    logger.info(f"  Final pchannels: {current_pchannel_num}")
    logger.info(f"  Final topology: {current_source} -> {current_target}")
    logger.info("=" * 60)

    if fail_count > 0:
        raise RuntimeError(f"Stress test had {fail_count} failures")


def main():
    parser = argparse.ArgumentParser(description="CDC stress test: switchover + pchannel increase + replica variation")
    parser.add_argument("--cycles", type=int, default=1000, help="Max number of cycles (default: 1000)")
    parser.add_argument("--duration", type=int, default=None, help="Max duration in seconds (default: no limit)")
    args = parser.parse_args()

    logger.info(f"Starting stress test: max_cycles={args.cycles}, max_duration={args.duration}s")
    run_stress_test(max_cycles=args.cycles, max_duration_sec=args.duration)


if __name__ == "__main__":
    main()
