import os, sys, time

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
from test_replica_features import cluster_control as cc

COL = "failover_restart_a"

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


def _cleanup():
    try:
        release_collection_on_primary(COL, cluster_B_client)
        cluster_B_client.drop_collection(COL)
    except Exception:
        pass
    try:
        drop_collection_on_primary(COL, cluster_A_client)
    except Exception:
        pass


def test_restart_a_mid_promotion():
    setup_replication_with_data()
    t, join = utils.start_async(utils.force_promote_b)
    time.sleep(2)
    cc.stop_cluster_a(); time.sleep(2); cc.start_cluster_a()
    import os
    timeout = int(os.getenv("FAILOVER_TIMEOUT", "180"))
    join(timeout=timeout)
    new_data = generate_data(500, INSERT_COUNT + 1)
    cluster_B_client.insert(collection_name=COL, data=new_data)
    utils.await_rows(cluster_B_client, COL, INSERT_COUNT + 500)
    _cleanup()


if __name__ == "__main__":
    # 直接运行：python test_restart_a_during_force_promote.py
    test_restart_a_mid_promotion()
    print("OK")
