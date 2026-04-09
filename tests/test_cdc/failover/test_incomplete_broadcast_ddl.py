import os, sys

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
from testcases.insert import INSERT_COUNT, insert_into_primary
from failover import utils

COL = "failover_broadcast_ddl"

def _setup():
    utils.ensure_secondary_b(); utils.init_replication_a_to_b()


def _teardown():
    try:
        release_collection_on_primary(COL, cluster_B_client)
        cluster_B_client.drop_collection(COL)
    except Exception:
        pass
    try:
        drop_collection_on_primary(COL, cluster_A_client)
    except Exception:
        pass
    try:
        new_col = f"{COL}_new"
        release_collection_on_primary(new_col, cluster_B_client)
        cluster_B_client.drop_collection(new_col)
    except Exception:
        pass



def run_incomplete_broadcast_ddl_flow():
    # 1) create collection on A, wait on B
    create_collection_on_primary(COL, cluster_A_client)
    wait_for_standby_create_collection(COL, cluster_B_client)

    # 2) create index on A, wait on B
    create_index_on_primary(COL, cluster_A_client)
    wait_for_standby_create_index(COL, cluster_B_client)

    # 3) load collection on A, wait on B
    load_collection_on_primary(COL, cluster_A_client)
    wait_for_standby_load_collection(COL, cluster_B_client)

    # 4) release collection on A
    release_collection_on_primary(COL, cluster_A_client)

    # 5) force promote / failover
    utils.force_promote_b()

    # 6) create new collection on B
    new_col = f"{COL}_new"
    create_collection_on_primary(new_col, cluster_B_client)

    # 7) load new collection on B
    cluster_B_client.load_collection(new_col)

    # 8) insert + verify on B
    insert_into_primary(1, new_col, cluster_B_client)
    utils.await_rows(cluster_B_client, new_col, INSERT_COUNT)


if __name__ == "__main__":
    # 直接运行：python test_incomplete_broadcast_ddl.py
    _setup()
    try:
        run_incomplete_broadcast_ddl_flow()
        print("OK")
    finally:
        _teardown()
