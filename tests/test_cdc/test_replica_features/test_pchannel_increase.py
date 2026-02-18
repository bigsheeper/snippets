"""
Pchannel increase test: restart clusters 5 times, increasing pchannels each time.

Flow (5 rounds):
  Round 0: pchannels=16 (default, clusters already running)
  Round 1: restart both clusters with dmlChannelNum=17, update config
  Round 2: restart both clusters with dmlChannelNum=18, update config
  Round 3: restart both clusters with dmlChannelNum=19, update config
  Round 4: restart both clusters with dmlChannelNum=20, update config

Each round:
  1. (Round > 0) Restart both clusters with new dmlChannelNum
  2. Update replicate config with new pchannel count
  3. Create collection, insert, verify replication
  4. Drop collection

Prerequisites:
  - Two Milvus clusters running with default 16 pchannels
  - Infrastructure (etcd/minio/pulsar) running

Usage:
  python test_pchannel_increase.py
"""

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
NUM_ROUNDS = 5
CLUSTER_B_REPLICA_NUMBER = 2


def run_pchannel_increase_test():
    """Run pchannel increase test with cluster restarts."""
    current_source = CLUSTER_A_ID
    current_target = CLUSTER_B_ID
    current_pchannel_num = INIT_PCHANNEL_NUM

    start_time = time.time()
    success_count = 0
    fail_count = 0

    for round_num in range(NUM_ROUNDS):
        round_start = time.time()
        collection_name = f"pchannel_round_{round_num}"

        try:
            # --- Restart clusters with new pchannel count (skip round 0, clusters already running) ---
            if round_num > 0:
                current_pchannel_num += 1
                logger.info(f"[Round {round_num}] Restarting clusters with dmlChannelNum={current_pchannel_num}")
                restart_both_clusters(
                    dml_channel_num=current_pchannel_num,
                    cluster_b_replica_number=CLUSTER_B_REPLICA_NUMBER,
                )
                reconnect_clients()

            # --- Update replicate config ---
            logger.info(f"[Round {round_num}] Updating replicate config: "
                        f"{current_source} -> {current_target}, pchannels={current_pchannel_num}")
            update_replicate_config(current_source, current_target, current_pchannel_num)

            # --- Create, insert, verify ---
            primary_client, standby_client = get_primary_and_standby(current_source)

            setup_collection(collection_name, primary_client, standby_client)
            insert_and_verify(collection_name, primary_client, standby_client)

            # --- Cleanup ---
            cleanup_collection(collection_name, primary_client, standby_client)

            elapsed = time.time() - round_start
            success_count += 1
            logger.info(f"[Round {round_num}] SUCCESS ({elapsed:.1f}s) pchannels={current_pchannel_num}")

        except Exception as e:
            elapsed = time.time() - round_start
            fail_count += 1
            logger.error(f"[Round {round_num}] FAILED ({elapsed:.1f}s): {e}")
            logger.error(traceback.format_exc())

            # Try cleanup
            try:
                primary_client, standby_client = get_primary_and_standby(current_source)
                if primary_client.has_collection(collection_name):
                    primary_client.drop_collection(collection_name)
            except Exception:
                pass

    total_time = time.time() - start_time
    logger.info("=" * 60)
    logger.info(f"Pchannel increase test completed in {total_time:.1f}s")
    logger.info(f"  Rounds: {success_count + fail_count}")
    logger.info(f"  Success: {success_count}")
    logger.info(f"  Failed:  {fail_count}")
    logger.info(f"  Pchannels: {INIT_PCHANNEL_NUM} -> {current_pchannel_num}")
    logger.info("=" * 60)

    if fail_count > 0:
        raise RuntimeError(f"Pchannel increase test had {fail_count} failures")


if __name__ == "__main__":
    logger.info("Starting pchannel increase test (5 rounds, restart each round)")
    run_pchannel_increase_test()
