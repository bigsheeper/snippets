import os, sys, time, threading

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE not in sys.path:
    sys.path.insert(0, BASE)
TESTCASES = os.path.join(BASE, "testcases")
if TESTCASES not in sys.path:
    sys.path.insert(0, TESTCASES)

from testcases.common import cluster_A_client, cluster_B_client
from testcases.collection import (
    create_collection_on_primary, wait_for_standby_create_collection,
    load_collection_on_primary, wait_for_standby_load_collection,
    drop_collection_on_primary, release_collection_on_primary,
)
from testcases.index import create_index_on_primary, wait_for_standby_create_index
from testcases.insert import INSERT_COUNT, generate_data, insert_into_primary
from failover import utils

COL = "failover_fastlock"


def setup_replication_with_data():
    utils.ensure_secondary_b(); utils.init_replication_a_to_b()
    create_collection_on_primary(COL, cluster_A_client)
    wait_for_standby_create_collection(COL, cluster_B_client)
    create_index_on_primary(COL, cluster_A_client)
    wait_for_standby_create_index(COL, cluster_B_client)
    load_collection_on_primary(COL, cluster_A_client)
    wait_for_standby_load_collection(COL, cluster_B_client)
    insert_into_primary(1, COL, cluster_A_client)
    utils.await_rows(cluster_B_client, COL, INSERT_COUNT)


def _contention_loop(stop):
    while not stop.is_set():
        try:
            cluster_B_client.update_replicate_configuration(**utils.REPL_A_TO_B)
        except Exception:
            pass
        time.sleep(0.2)


def test_fastlock_contention_promotion_completes():
    setup_replication_with_data()
    stop = threading.Event(); th = threading.Thread(target=_contention_loop, args=(stop,), daemon=True); th.start()
    try:
        utils.force_promote_b()
    finally:
        stop.set(); th.join(timeout=10)
    new_data = generate_data(500, INSERT_COUNT + 1)
    cluster_B_client.insert(collection_name=COL, data=new_data)
    utils.await_rows(cluster_B_client, COL, INSERT_COUNT + 500)


if __name__ == "__main__":
    # 直接运行：python test_fastlock_contention.py
    test_fastlock_contention_promotion_completes()
    print("OK")
