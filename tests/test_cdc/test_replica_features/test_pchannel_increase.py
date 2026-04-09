"""PChannel increase test with random switchover and vchannel allocation guard.

Phase 1: Vchannel allocation guard (create collection BEFORE config update)
Phase 2: Pchannel increase with random switchover (10 rounds)

Usage:
  python test_pchannel_increase.py
"""
import random
import time
import traceback
import _path_setup  # noqa: F401
from loguru import logger
from common import (
    CLUSTER_A_ID, CLUSTER_B_ID,
    setup_collection, insert_and_verify, drop_if_exists,
    reconnect_clients, restart_both_clusters,
)
from test_replica_features.common import update_replicate_config, get_primary_and_standby

INIT_PCHANNEL_NUM = 16
NUM_GUARD_ROUNDS = 5
NUM_ROUNDS = 10
CLUSTER_B_REPLICA = 2


def run_vchannel_allocation_guard_test():
    """Test vchannel allocation respects replicate config boundaries."""
    source, target = CLUSTER_A_ID, CLUSTER_B_ID
    pchannel_num = INIT_PCHANNEL_NUM
    success, fail = 0, 0

    for r in range(NUM_GUARD_ROUNDS):
        pre_name = f"guard_pre_{r}"
        post_name = f"guard_post_{r}"

        try:
            if r == 0:
                update_replicate_config(source, target, pchannel_num)
                primary, standby = get_primary_and_standby(source)
                setup_collection(post_name, primary, standby, shard_num=INIT_PCHANNEL_NUM)
                insert_and_verify(post_name, primary, standby)
            else:
                pchannel_num += 1
                logger.info(f"[Guard {r}] Restart with dmlChannelNum={pchannel_num}")
                restart_both_clusters(dml_channel_num=pchannel_num, cluster_b_replica_number=CLUSTER_B_REPLICA)
                reconnect_clients()
                primary, standby = get_primary_and_standby(source)

                # Create BEFORE config update (the critical test)
                logger.info(f"[Guard {r}] Create collection BEFORE config update")
                setup_collection(pre_name, primary, standby, shard_num=INIT_PCHANNEL_NUM)
                insert_and_verify(pre_name, primary, standby)

                # Update config
                update_replicate_config(source, target, pchannel_num)

                # Create AFTER config update
                setup_collection(post_name, primary, standby, shard_num=INIT_PCHANNEL_NUM)
                insert_and_verify(post_name, primary, standby)

            success += 1
            logger.info(f"[Guard {r}] SUCCESS pchannels={pchannel_num}")

        except Exception as e:
            fail += 1
            logger.error(f"[Guard {r}] FAILED: {e}")
            logger.error(traceback.format_exc())
            primary, standby = get_primary_and_standby(source)
            for n in [pre_name, post_name]:
                drop_if_exists(primary, n)
            break

    logger.info(f"Guard test: {success} success, {fail} failed, pchannels {INIT_PCHANNEL_NUM}->{pchannel_num}")
    if fail > 0:
        raise RuntimeError(f"Guard test had {fail} failures")
    return pchannel_num


def run_pchannel_increase_test(init_pchannel_num=INIT_PCHANNEL_NUM):
    """PChannel increase with cluster restarts and random switchover."""
    source, target = CLUSTER_A_ID, CLUSTER_B_ID
    pchannel_num = init_pchannel_num
    success, fail, switchovers = 0, 0, 0

    for r in range(NUM_ROUNDS):
        name = f"pchannel_round_{r}"
        pre_name = f"pchannel_pre_update_{r}"

        try:
            if r > 0:
                # Random switchover before restart
                if random.choice([True, False]):
                    source, target = target, source
                    switchovers += 1
                    logger.info(f"[Round {r}] Pre-restart switchover: {source} -> {target}")
                    update_replicate_config(source, target, pchannel_num)

                pchannel_num += 1
                restart_both_clusters(dml_channel_num=pchannel_num, cluster_b_replica_number=CLUSTER_B_REPLICA)
                reconnect_clients()

                # Guard test: create before config update
                primary, standby = get_primary_and_standby(source)
                setup_collection(pre_name, primary, standby, shard_num=INIT_PCHANNEL_NUM)
                insert_and_verify(pre_name, primary, standby)

            # Update config
            update_replicate_config(source, target, pchannel_num)

            # Random switchover after increase
            if random.choice([True, False]):
                source, target = target, source
                switchovers += 1
                logger.info(f"[Round {r}] Post-increase switchover: {source} -> {target}")
                update_replicate_config(source, target, pchannel_num)

            primary, standby = get_primary_and_standby(source)
            setup_collection(name, primary, standby, shard_num=INIT_PCHANNEL_NUM)
            insert_and_verify(name, primary, standby)

            success += 1
            logger.info(f"[Round {r}] SUCCESS pchannels={pchannel_num}, source={source}")

        except Exception as e:
            fail += 1
            logger.error(f"[Round {r}] FAILED: {e}")
            logger.error(traceback.format_exc())
            primary, standby = get_primary_and_standby(source)
            for n in [name, pre_name]:
                drop_if_exists(primary, n)
            break

    logger.info(
        f"PChannel test: {success} success, {fail} failed, "
        f"{switchovers} switchovers, pchannels {init_pchannel_num}->{pchannel_num}"
    )
    if fail > 0:
        raise RuntimeError(f"PChannel test had {fail} failures")


if __name__ == "__main__":
    logger.info("Phase 1: Vchannel allocation guard test")
    pchannel_num = run_vchannel_allocation_guard_test()

    logger.info(f"Phase 2: PChannel increase test (starting from {pchannel_num})")
    run_pchannel_increase_test(init_pchannel_num=pchannel_num)

    logger.info("PASSED: all pchannel increase tests")
