"""
Pchannel increase test with random switchover and vchannel allocation guard.

Restart clusters multiple times, increasing pchannels each time.
Random switchover can happen at two points:
  - Before restart: ensures pchannel increase works regardless of current topology
  - After pchannel increase: tests switchover right after pchannels grow

Vchannel allocation guard test:
  After restart with more pchannels but BEFORE updating replicate config,
  create a collection. The fix ensures vchannels are only allocated to
  pchannels listed in the current replicate config, preventing replication
  failures on the secondary which hasn't received the config update yet.

Flow (multiple rounds):
  Round 0: pchannels=16 (default, clusters already running), maybe switchover
  Round 1+:
    1. Maybe switchover (before restart, exercises pchannel increase from either direction)
    2. Restart both clusters with new dmlChannelNum
    3. Create collection BEFORE config update (vchannel allocation guard test)
    4. Update replicate config with new pchannel count
    5. Maybe switchover (after increase, exercises switchover with new pchannels)
    6. Create collection, insert, verify replication
    7. Drop collections

Prerequisites:
  - Two Milvus clusters running with default 16 pchannels
  - Infrastructure (etcd/minio/pulsar) running

Usage:
  python test_pchannel_increase.py
"""

import random
import time
import traceback
from loguru import logger

from common import (
    CLUSTER_A_ID,
    CLUSTER_B_ID,
    update_replicate_config,
    get_primary_and_standby,
    reconnect_clients,
)
from collection_helpers import (
    setup_collection,
    insert_and_verify,
    cleanup_collection,
)
from cluster_control import restart_both_clusters

INIT_PCHANNEL_NUM = 16
NUM_ROUNDS = 10
NUM_GUARD_ROUNDS = 5
CLUSTER_B_REPLICA_NUMBER = 2


def run_vchannel_allocation_guard_test():
    """Test that vchannel allocation respects replicate config boundaries.

    Exercises the fix for the race condition: after cluster restart with more
    pchannels but before replicate config update, new operations (CreateCollection)
    should NOT allocate vchannels to the new pchannels.

    Without the fix, creating a collection in this window could allocate vchannels
    to the new pchannel. When replicated to the secondary (whose config hasn't been
    updated yet), the secondary's overwriteCreateCollectionMessage would fail because
    it doesn't recognize the new pchannel.

    Runs NUM_GUARD_ROUNDS iterations, each increasing pchannels by 1:
      Round 0: baseline verification with initial config
      Round 1+:
        1. Restart clusters with +1 pchannel
        2. Create collection BEFORE config update (the critical test)
           -> vchannels only allocated to pchannels in current config
        3. Verify replication works
        4. Update replicate config to include new pchannels
        5. Create collection AFTER config update
           -> vchannels can now use all pchannels
        6. Verify replication works
    """
    current_source = CLUSTER_A_ID
    current_target = CLUSTER_B_ID
    current_pchannel_num = INIT_PCHANNEL_NUM

    start_time = time.time()
    success_count = 0
    fail_count = 0

    for round_num in range(NUM_GUARD_ROUNDS):
        round_start = time.time()
        pre_update_name = f"guard_pre_{round_num}"
        post_update_name = f"guard_post_{round_num}"

        try:
            if round_num == 0:
                # --- Round 0: set initial config and verify baseline ---
                logger.info(f"[Guard {round_num}] Setting initial replicate config: "
                            f"{current_source} -> {current_target}, pchannels={current_pchannel_num}")
                update_replicate_config(current_source, current_target, current_pchannel_num)

                primary_client, standby_client = get_primary_and_standby(current_source)
                setup_collection(post_update_name, primary_client, standby_client)
                insert_and_verify(post_update_name, primary_client, standby_client)
                cleanup_collection(post_update_name, primary_client, standby_client)
                logger.info(f"[Guard {round_num}] Baseline replication verified")

            else:
                # --- Restart with more pchannels ---
                current_pchannel_num += 1
                logger.info(f"[Guard {round_num}] Restarting clusters with dmlChannelNum={current_pchannel_num} "
                            f"(config still has {current_pchannel_num - 1})")
                restart_both_clusters(
                    dml_channel_num=current_pchannel_num,
                    cluster_b_replica_number=CLUSTER_B_REPLICA_NUMBER,
                )
                reconnect_clients()
                primary_client, standby_client = get_primary_and_standby(current_source)

                # --- Create collection BEFORE config update (the critical test) ---
                # New pchannels exist on both clusters but replicate config still lists
                # the old count. With the fix, AllocVirtualChannels skips the new pchannel.
                logger.info(f"[Guard {round_num}] Creating collection BEFORE replicate config update "
                            f"(pchannels={current_pchannel_num} exist, config has {current_pchannel_num - 1})")
                setup_collection(pre_update_name, primary_client, standby_client)
                insert_and_verify(pre_update_name, primary_client, standby_client)
                logger.info(f"[Guard {round_num}] Pre-config-update collection replicated successfully")
                cleanup_collection(pre_update_name, primary_client, standby_client)

                # --- Update replicate config to include new pchannels ---
                logger.info(f"[Guard {round_num}] Updating replicate config to pchannels={current_pchannel_num}")
                update_replicate_config(current_source, current_target, current_pchannel_num)

                # --- Create collection AFTER config update ---
                logger.info(f"[Guard {round_num}] Creating collection AFTER replicate config update "
                            f"(pchannels={current_pchannel_num})")
                setup_collection(post_update_name, primary_client, standby_client)
                insert_and_verify(post_update_name, primary_client, standby_client)
                logger.info(f"[Guard {round_num}] Post-config-update collection replicated successfully")
                cleanup_collection(post_update_name, primary_client, standby_client)

            elapsed = time.time() - round_start
            success_count += 1
            logger.info(f"[Guard {round_num}] SUCCESS ({elapsed:.1f}s) pchannels={current_pchannel_num}")

        except Exception as e:
            elapsed = time.time() - round_start
            fail_count += 1
            logger.error(f"[Guard {round_num}] FAILED ({elapsed:.1f}s): {e}")
            logger.error(traceback.format_exc())

            # Try cleanup
            try:
                primary_client, standby_client = get_primary_and_standby(current_source)
                for name in [pre_update_name, post_update_name]:
                    if primary_client.has_collection(name):
                        primary_client.drop_collection(name)
            except Exception:
                pass

    total_time = time.time() - start_time
    logger.info("=" * 60)
    logger.info(f"Vchannel allocation guard test completed in {total_time:.1f}s")
    logger.info(f"  Rounds: {success_count + fail_count}")
    logger.info(f"  Success: {success_count}")
    logger.info(f"  Failed:  {fail_count}")
    logger.info(f"  Pchannels: {INIT_PCHANNEL_NUM} -> {current_pchannel_num}")
    logger.info("=" * 60)

    if fail_count > 0:
        raise RuntimeError(f"Vchannel allocation guard test had {fail_count} failures")

    return current_pchannel_num


def run_pchannel_increase_test(init_pchannel_num=INIT_PCHANNEL_NUM):
    """Run pchannel increase test with cluster restarts and random switchover."""
    current_source = CLUSTER_A_ID
    current_target = CLUSTER_B_ID
    current_pchannel_num = init_pchannel_num

    start_time = time.time()
    success_count = 0
    fail_count = 0
    switchover_count = 0
    guard_test_count = 0

    for round_num in range(NUM_ROUNDS):
        round_start = time.time()
        collection_name = f"pchannel_round_{round_num}"
        pre_update_name = f"pchannel_pre_update_{round_num}"

        try:
            # --- Restart clusters with new pchannel count (skip round 0, clusters already running) ---
            if round_num > 0:
                # --- Random switchover before restart ---
                # This ensures the pchannel increase happens from a random topology direction.
                if random.choice([True, False]):
                    current_source, current_target = current_target, current_source
                    switchover_count += 1
                    logger.info(f"[Round {round_num}] Pre-restart switchover -> "
                                f"{current_source} -> {current_target}")
                    update_replicate_config(current_source, current_target, current_pchannel_num)

                current_pchannel_num += 1
                logger.info(f"[Round {round_num}] Restarting clusters with dmlChannelNum={current_pchannel_num}")
                restart_both_clusters(
                    dml_channel_num=current_pchannel_num,
                    cluster_b_replica_number=CLUSTER_B_REPLICA_NUMBER,
                )
                reconnect_clients()

                # --- Vchannel allocation guard: create collection BEFORE config update ---
                # New pchannels exist but replicate config still has the old count.
                # With the fix, vchannels are only allocated to config-listed pchannels.
                primary_client, standby_client = get_primary_and_standby(current_source)
                logger.info(f"[Round {round_num}] Guard test: creating collection before config update "
                            f"(pchannels={current_pchannel_num} exist, config has {current_pchannel_num - 1})")
                setup_collection(pre_update_name, primary_client, standby_client)
                insert_and_verify(pre_update_name, primary_client, standby_client)
                cleanup_collection(pre_update_name, primary_client, standby_client)
                guard_test_count += 1
                logger.info(f"[Round {round_num}] Guard test passed")

            # --- Update replicate config (pchannel increase) ---
            logger.info(f"[Round {round_num}] Updating replicate config: "
                        f"{current_source} -> {current_target}, pchannels={current_pchannel_num}")
            update_replicate_config(current_source, current_target, current_pchannel_num)

            # --- Random switchover after pchannel increase ---
            if random.choice([True, False]):
                current_source, current_target = current_target, current_source
                switchover_count += 1
                logger.info(f"[Round {round_num}] Post-increase switchover -> "
                            f"{current_source} -> {current_target}")
                update_replicate_config(current_source, current_target, current_pchannel_num)

            # --- Create, insert, verify ---
            primary_client, standby_client = get_primary_and_standby(current_source)

            setup_collection(collection_name, primary_client, standby_client)
            insert_and_verify(collection_name, primary_client, standby_client)

            # --- Cleanup ---
            cleanup_collection(collection_name, primary_client, standby_client)

            elapsed = time.time() - round_start
            success_count += 1
            logger.info(f"[Round {round_num}] SUCCESS ({elapsed:.1f}s) "
                        f"pchannels={current_pchannel_num}, source={current_source}")

        except Exception as e:
            elapsed = time.time() - round_start
            fail_count += 1
            logger.error(f"[Round {round_num}] FAILED ({elapsed:.1f}s): {e}")
            logger.error(traceback.format_exc())

            # Try cleanup
            try:
                primary_client, standby_client = get_primary_and_standby(current_source)
                for name in [collection_name, pre_update_name]:
                    if primary_client.has_collection(name):
                        primary_client.drop_collection(name)
            except Exception:
                pass

    total_time = time.time() - start_time
    logger.info("=" * 60)
    logger.info(f"Pchannel increase test completed in {total_time:.1f}s")
    logger.info(f"  Rounds: {success_count + fail_count}")
    logger.info(f"  Success: {success_count}")
    logger.info(f"  Failed:  {fail_count}")
    logger.info(f"  Switchovers: {switchover_count}")
    logger.info(f"  Guard tests (pre-config-update create): {guard_test_count}")
    logger.info(f"  Pchannels: {init_pchannel_num} -> {current_pchannel_num}")
    logger.info(f"  Final topology: {current_source} -> {current_target}")
    logger.info("=" * 60)

    if fail_count > 0:
        raise RuntimeError(f"Pchannel increase test had {fail_count} failures")


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("Phase 1: Vchannel allocation guard test")
    logger.info("=" * 60)
    pchannel_num = run_vchannel_allocation_guard_test()

    logger.info("")
    logger.info("=" * 60)
    logger.info(f"Phase 2: Pchannel increase test (10 rounds, starting from pchannels={pchannel_num})")
    logger.info("=" * 60)
    run_pchannel_increase_test(init_pchannel_num=pchannel_num)
