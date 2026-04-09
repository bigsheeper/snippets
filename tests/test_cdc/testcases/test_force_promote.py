"""Force promote failover tests.

Test scenarios:
  primary_fails          - force promote on primary cluster should fail
  clusters_fails         - force promote with multiple clusters should fail
  topology_fails         - force promote with non-empty topology should fail
  success                - force promote happy path with data integrity
  promoted_primary_fails - force promote on already-promoted cluster should fail

Usage:
  python test_force_promote.py               # run all
  python test_force_promote.py success       # run specific test
"""
import os
import sys
import time
import _path_setup  # noqa: F401
from loguru import logger
from common import (
    cluster_A_client, cluster_B_client,
    CLUSTER_A_ID, CLUSTER_B_ID, CLUSTER_A_ADDR, CLUSTER_B_ADDR,
    TOKEN, INSERT_COUNT, PCHANNEL_NUM,
    generate_pchannels, build_replicate_config_2, build_standalone_config,
    setup_collection, drop_if_exists, insert_and_verify,
    generate_data, query_all, wait_for_query_consistent,
    stop_cluster_b, start_cluster_b, wait_for_health,
    CLUSTER_B_PROXY_HEALTH,
)


def init_replication_a_to_b():
    config = build_replicate_config_2(CLUSTER_A_ID, CLUSTER_B_ID)
    cluster_B_client.update_replicate_configuration(**config)
    cluster_A_client.update_replicate_configuration(**config)
    logger.info("Replication initialized: A -> B")


def force_promote_b():
    config = build_standalone_config(CLUSTER_B_ID)
    cluster_B_client.update_replicate_configuration(**config, force_promote=True)
    logger.info("Force promote called on B")


def restart_cluster_b_as_secondary():
    """Reset B to secondary: write config, restart."""
    logger.info("=== Restarting B as secondary ===")
    init_replication_a_to_b()
    stop_cluster_b()
    start_cluster_b()
    time.sleep(5)


def _expect_error(fn, *keywords):
    """Call fn(), expect it raises. Verify error msg contains at least one keyword."""
    try:
        fn()
        raise AssertionError(f"Expected error containing {keywords}, but call succeeded")
    except AssertionError:
        raise
    except Exception as e:
        msg = str(e).lower()
        assert any(k in msg for k in keywords), f"Expected {keywords} in error, got: {e}"
        logger.info(f"Got expected error: {e}")


# ---- Test cases ----

def test_force_promote_on_primary_fails():
    logger.info("=== Test: force promote on primary should fail ===")
    config = build_standalone_config(CLUSTER_A_ID)
    _expect_error(
        lambda: cluster_A_client.update_replicate_configuration(**config, force_promote=True),
        "secondary", "force promote",
    )
    logger.info("PASSED")


def test_force_promote_with_non_empty_clusters_fails():
    logger.info("=== Test: force promote with multiple clusters should fail ===")
    _expect_error(
        lambda: cluster_B_client.update_replicate_configuration(
            force_promote=True,
            clusters=[
                {"cluster_id": CLUSTER_A_ID, "connection_param": {"uri": CLUSTER_A_ADDR, "token": TOKEN},
                 "pchannels": generate_pchannels(CLUSTER_A_ID)},
                {"cluster_id": CLUSTER_B_ID, "connection_param": {"uri": CLUSTER_B_ADDR, "token": TOKEN},
                 "pchannels": generate_pchannels(CLUSTER_B_ID)},
            ],
            cross_cluster_topology=[],
        ),
        "cluster", "secondary",
    )
    logger.info("PASSED")


def test_force_promote_with_topology_fails():
    logger.info("=== Test: force promote with topology should fail ===")
    _expect_error(
        lambda: cluster_B_client.update_replicate_configuration(
            force_promote=True,
            clusters=[
                {"cluster_id": CLUSTER_B_ID, "connection_param": {"uri": CLUSTER_B_ADDR, "token": TOKEN},
                 "pchannels": generate_pchannels(CLUSTER_B_ID)},
            ],
            cross_cluster_topology=[
                {"source_cluster_id": CLUSTER_A_ID, "target_cluster_id": CLUSTER_B_ID},
            ],
        ),
        "topology", "empty", "secondary",
    )
    logger.info("PASSED")


def test_force_promote_success():
    logger.info("=== Test: force promote success with data integrity ===")
    name = "test_force_promote_success"

    restart_cluster_b_as_secondary()
    drop_if_exists(cluster_A_client, name, "A")
    drop_if_exists(cluster_B_client, name, "B")

    init_replication_a_to_b()
    setup_collection(name, cluster_A_client, cluster_B_client)

    insert_and_verify(name, cluster_A_client, cluster_B_client, start_id=1)
    logger.info("Pre-promote: data verified on B")

    force_promote_b()
    time.sleep(5)

    # B is now standalone primary — insert directly
    new_data = generate_data(500, INSERT_COUNT + 1)
    cluster_B_client.insert(collection_name=name, data=new_data)
    logger.info("Inserted 500 rows directly on B")

    res = query_all(cluster_B_client, name)
    expected = INSERT_COUNT + 500
    assert len(res) == expected, f"Expected {expected} rows on B, got {len(res)}"
    logger.info(f"Data integrity verified: B has {expected} rows")

    # Cleanup
    drop_if_exists(cluster_B_client, name, "B")
    drop_if_exists(cluster_A_client, name, "A")
    logger.info("PASSED")


def test_force_promote_on_promoted_primary_fails():
    logger.info("=== Test: force promote on already-promoted cluster ===")
    _expect_error(force_promote_b, "secondary", "primary")
    logger.info("PASSED")


# ---- Runner ----

TESTS = {
    "primary_fails": test_force_promote_on_primary_fails,
    "clusters_fails": test_force_promote_with_non_empty_clusters_fails,
    "topology_fails": test_force_promote_with_topology_fails,
    "success": test_force_promote_success,
    "promoted_primary_fails": test_force_promote_on_promoted_primary_fails,
}

if __name__ == "__main__":
    if len(sys.argv) > 1:
        name = sys.argv[1]
        if name not in TESTS:
            print(f"Unknown test: {name}. Available: {list(TESTS.keys())}")
            sys.exit(1)
        TESTS[name]()
    else:
        logger.info("Running all force promote tests...")
        for name, fn in TESTS.items():
            logger.info(f"--- {name} ---")
            fn()
        logger.info("All force promote tests PASSED!")
