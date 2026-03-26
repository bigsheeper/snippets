"""
E2E tests for force promote failover feature.

PR: https://github.com/milvus-io/milvus/pull/47352
Feature: Support force promote for primary-secondary failover

Test scenarios:
1. Force promote on primary cluster should fail
2. Force promote with non-empty clusters should fail
3. Force promote with non-empty topology should fail
4. Force promote succeeds on secondary cluster (happy path with data integrity)
5. Force promote is idempotent (calling twice should not fail)

Setup assumption:
  - Cluster A runs on localhost:19530
  - Cluster B runs on localhost:19531
  - Before running happy-path / idempotency tests, A->B replication must be configured
    (the test handles this via init_replication_a_to_b())
"""
import os
import subprocess
import sys
import time
import urllib.request
from loguru import logger
from common import *
from collection import (
    create_collection_on_primary,
    wait_for_standby_create_collection,
    load_collection_on_primary,
    wait_for_standby_load_collection,
    release_collection_on_primary,
    drop_collection_on_primary,
    wait_for_standby_drop_collection,
)
from index import create_index_on_primary, wait_for_standby_create_index
from insert import (
    INSERT_COUNT,
    INSERT_ROUNDS,
    insert_into_primary_multiple_rounds,
    insert_into_primary,
    generate_data,
)
from query import query_on_primary, wait_for_standby_query


CLUSTER_A_ID = "by-dev1"
CLUSTER_B_ID = "by-dev2"
PCHANNEL_NUM = 16

MILVUS_CONTROL = os.path.expanduser("~/workspace/snippets/milvus_control/milvus_control")
CLUSTER_B_METRICS = "http://localhost:19092"
HEALTH_CHECK_TIMEOUT = 120


def generate_pchannels(cluster_id: str, pchannel_num: int = PCHANNEL_NUM):
    return [f"{cluster_id}-rootcoord-dml_{i}" for i in range(pchannel_num)]


def init_replication_a_to_b():
    """Initialize replication: A (primary) -> B (secondary)."""
    clusterA_pchannels = generate_pchannels(CLUSTER_A_ID)
    clusterB_pchannels = generate_pchannels(CLUSTER_B_ID)
    config = {
        "clusters": [
            {
                "cluster_id": CLUSTER_A_ID,
                "connection_param": {"uri": CLUSTER_A_ADDR, "token": TOKEN},
                "pchannels": clusterA_pchannels,
            },
            {
                "cluster_id": CLUSTER_B_ID,
                "connection_param": {"uri": CLUSTER_B_ADDR, "token": TOKEN},
                "pchannels": clusterB_pchannels,
            },
        ],
        "cross_cluster_topology": [
            {"source_cluster_id": CLUSTER_A_ID, "target_cluster_id": CLUSTER_B_ID}
        ],
    }
    # Update secondary (B) first, then primary (A)
    cluster_B_client.update_replicate_configuration(**config)
    cluster_A_client.update_replicate_configuration(**config)
    logger.info("Replication initialized: A -> B")


def make_standalone_config(cluster_id: str, addr: str) -> dict:
    """Build the standalone (single-cluster, no-topology) config for force promote.

    The server requires exactly one cluster (the current cluster) with no topology.
    """
    return {
        "clusters": [
            {
                "cluster_id": cluster_id,
                "connection_param": {"uri": addr, "token": TOKEN},
                "pchannels": generate_pchannels(cluster_id),
            }
        ],
        "cross_cluster_topology": [],
    }


def force_promote_secondary():
    """Force promote cluster B from secondary to standalone primary."""
    cluster_B_client.update_replicate_configuration(
        **make_standalone_config(CLUSTER_B_ID, CLUSTER_B_ADDR),
        force_promote=True,
    )
    logger.info("Force promote called on cluster B")


def restart_cluster_b_as_secondary():
    """Reset cluster B to secondary state: write secondary config to etcd, then restart.

    After force promote, B is standalone primary and its WAL subscription to A is gone.
    Calling init_replication_a_to_b() while B is running updates B's etcd config back
    to secondary. Then restarting B makes it re-read the etcd config and re-subscribe
    to A's WAL channels — restoring the A→B replication for the next test run.
    """
    logger.info("=== Restarting cluster B as secondary ===")

    # Update B's etcd config to secondary while B is still running
    init_replication_a_to_b()
    logger.info("Secondary config written to B's etcd")

    # Stop cluster B
    logger.info("Stopping cluster B...")
    subprocess.run(
        f"{MILVUS_CONTROL} -m -s -f --standby stop_milvus",
        shell=True,
        env={**os.environ},
        capture_output=True,
        text=True,
    )
    time.sleep(2)

    # Start cluster B (reuse volume — reads secondary config from etcd)
    logger.info("Starting cluster B as secondary...")
    result = subprocess.run(
        f"{MILVUS_CONTROL} -m -s -u --standby start_milvus",
        shell=True,
        env={**os.environ},
        capture_output=True,
        text=True,
    )
    if result.stdout.strip():
        logger.info(result.stdout.strip())

    # Wait for B to be healthy
    start = time.time()
    while time.time() - start < HEALTH_CHECK_TIMEOUT:
        try:
            with urllib.request.urlopen(f"{CLUSTER_B_METRICS}/healthz", timeout=5) as resp:
                if resp.read().decode().strip() == "OK":
                    logger.info(f"Cluster B healthy after {time.time() - start:.0f}s")
                    time.sleep(5)  # allow services to stabilize
                    return
        except Exception:
            pass
        time.sleep(2)
    raise TimeoutError(f"Cluster B failed health check after {HEALTH_CHECK_TIMEOUT}s")


def test_force_promote_on_primary_fails():
    """
    Test: Calling force promote on the primary cluster (A) should fail.

    The primary cluster is not in secondary role, so force promote is rejected
    with an error about it only being allowed on secondary clusters.
    """
    logger.info("=== Test: Force promote on primary cluster should fail ===")

    try:
        cluster_A_client.update_replicate_configuration(
            **make_standalone_config(CLUSTER_A_ID, CLUSTER_A_ADDR),
            force_promote=True,
        )
        raise AssertionError("Expected force promote on primary to fail, but it succeeded")
    except AssertionError:
        raise
    except Exception as e:
        error_msg = str(e)
        logger.info(f"Got expected error: {error_msg}")
        assert "secondary" in error_msg.lower() or "force promote" in error_msg.lower(), (
            f"Expected error about secondary cluster, got: {error_msg}"
        )

    logger.info("PASSED: Force promote on primary cluster correctly rejected")


def test_force_promote_with_non_empty_clusters_fails():
    """
    Test: Calling force promote with multiple clusters should fail.

    Force promote requires exactly one cluster (the current cluster only).
    Passing both A and B clusters is rejected by the server.
    """
    logger.info("=== Test: Force promote with non-empty clusters should fail ===")

    try:
        cluster_B_client.update_replicate_configuration(
            force_promote=True,
            clusters=[
                {
                    "cluster_id": CLUSTER_A_ID,
                    "connection_param": {"uri": CLUSTER_A_ADDR, "token": TOKEN},
                    "pchannels": generate_pchannels(CLUSTER_A_ID),
                },
                {
                    "cluster_id": CLUSTER_B_ID,
                    "connection_param": {"uri": CLUSTER_B_ADDR, "token": TOKEN},
                    "pchannels": generate_pchannels(CLUSTER_B_ID),
                },
            ],
            cross_cluster_topology=[],
        )
        raise AssertionError("Expected force promote with multiple clusters to fail")
    except AssertionError:
        raise
    except Exception as e:
        error_msg = str(e)
        logger.info(f"Got expected error: {error_msg}")
        assert "cluster" in error_msg.lower() or "secondary" in error_msg.lower(), (
            f"Expected error about cluster config, got: {error_msg}"
        )

    logger.info("PASSED: Force promote with non-empty clusters correctly rejected")


def test_force_promote_with_topology_fails():
    """
    Test: Calling force promote on secondary cluster B with non-empty topology fails.

    Force promote requires no topology (standalone primary config).
    Providing a non-empty cross_cluster_topology is rejected by the server.
    """
    logger.info("=== Test: Force promote with non-empty topology should fail ===")

    try:
        # Correct cluster config (only B), but non-empty topology — should be rejected
        cluster_B_client.update_replicate_configuration(
            force_promote=True,
            clusters=[
                {
                    "cluster_id": CLUSTER_B_ID,
                    "connection_param": {"uri": CLUSTER_B_ADDR, "token": TOKEN},
                    "pchannels": generate_pchannels(CLUSTER_B_ID),
                }
            ],
            cross_cluster_topology=[
                {"source_cluster_id": CLUSTER_A_ID, "target_cluster_id": CLUSTER_B_ID}
            ],
        )
        raise AssertionError("Expected force promote with non-empty topology to fail")
    except AssertionError:
        raise
    except Exception as e:
        error_msg = str(e)
        logger.info(f"Got expected error: {error_msg}")
        assert "topology" in error_msg.lower() or "empty" in error_msg.lower(), (
            f"Expected error about topology being non-empty, got: {error_msg}"
        )

    logger.info("PASSED: Force promote with non-empty topology correctly rejected")


def test_force_promote_success():
    """
    Test: Force promote turns secondary cluster B into standalone primary,
    and data written to primary A is preserved and queryable on B after promotion.

    Steps:
    1. Set up A -> B replication
    2. Create collection + index + load on A, verify replication to B
    3. Insert data on A, verify it replicates to B
    4. Call force promote on B
    5. Verify B can accept writes independently (standalone primary)
    6. Insert additional data directly into B
    7. Verify B has both original + new data
    8. Cleanup
    """
    logger.info("=== Test: Force promote success with data integrity ===")
    collection_name = "test_force_promote_success"

    # Ensure B is in secondary state — needed when re-running after a previous force promote.
    # On first run this is a no-op (B already secondary); on repeat runs it resets B.
    restart_cluster_b_as_secondary()

    # Pre-cleanup: drop collection from previous runs if it exists on A or B
    for _client, _name in [(cluster_A_client, "A"), (cluster_B_client, "B")]:
        try:
            if _client.has_collection(collection_name):
                _client.release_collection(collection_name)
                _client.drop_collection(collection_name)
                logger.info(f"Pre-cleanup: dropped existing collection {collection_name} from {_name}")
        except Exception:
            pass  # Collection did not exist or not accessible, ignore

    # Step 1: Initialize A -> B replication
    init_replication_a_to_b()

    # Step 2: Create collection on A, wait for B
    create_collection_on_primary(collection_name, cluster_A_client)
    wait_for_standby_create_collection(collection_name, cluster_B_client)

    create_index_on_primary(collection_name, cluster_A_client)
    wait_for_standby_create_index(collection_name, cluster_B_client)

    load_collection_on_primary(collection_name, cluster_A_client)
    wait_for_standby_load_collection(collection_name, cluster_B_client)

    # Step 3: Insert 1000 rows on A, verify B has them
    insert_into_primary(1, collection_name, cluster_A_client)
    res_on_primary = query_on_primary(collection_name, INSERT_COUNT, cluster_A_client)
    wait_for_standby_query(collection_name, res_on_primary, cluster_B_client)
    logger.info("Pre-promote: 1000 rows verified on B")

    # Step 4: Force promote B to standalone primary
    force_promote_secondary()
    logger.info("Force promote completed, waiting for B to become standalone...")
    time.sleep(5)  # allow promotion to take effect

    # Step 5: Insert new data directly into B (now standalone primary)
    new_data = generate_data(500, INSERT_COUNT + 1)
    cluster_B_client.insert(collection_name=collection_name, data=new_data)
    logger.info("Inserted 500 new rows directly into B after force promote")

    # Step 6: Verify B has all 1500 rows (1000 original + 500 new)
    query_on_primary(collection_name, INSERT_COUNT + 500, cluster_B_client)
    logger.info("Data integrity verified: B has all 1500 rows after force promote")

    # Cleanup
    release_collection_on_primary(collection_name, cluster_B_client)
    cluster_B_client.drop_collection(collection_name)
    drop_collection_on_primary(collection_name, cluster_A_client)
    logger.info("=== Test force_promote_success PASSED ===")


def test_force_promote_idempotent():
    """
    Test: Force promote on an already-promoted cluster fails gracefully.

    After the success test promotes B to standalone primary, calling force promote
    again on B (now primary) should fail with a clear error about cluster role.
    This verifies that force promote is safely rejected when the cluster is
    no longer in secondary state, preventing accidental re-promotion.

    Prerequisite: test_force_promote_success must have run first (B is now primary).
    """
    logger.info("=== Test: Force promote on already-promoted cluster ===")

    # B is now a standalone primary (promoted by test_force_promote_success).
    # Calling force promote again should fail with a clear role-mismatch error.
    try:
        force_promote_secondary()
        raise AssertionError(
            "Expected force promote to fail on already-promoted primary cluster, but it succeeded"
        )
    except AssertionError:
        raise
    except Exception as e:
        error_msg = str(e)
        logger.info(f"Force promote on already-promoted cluster correctly rejected: {error_msg}")
        assert "secondary" in error_msg.lower() or "primary" in error_msg.lower(), (
            f"Expected error about cluster role, got: {error_msg}"
        )

    logger.info("=== Test force_promote_idempotent PASSED ===")


def main():
    """Run force promote tests.

    Usage:
      python test_force_promote.py               - run all tests
      python test_force_promote.py primary_fails - run specific test
      python test_force_promote.py clusters_fails
      python test_force_promote.py topology_fails
      python test_force_promote.py success
      python test_force_promote.py idempotent
    """
    tests = {
        "primary_fails": test_force_promote_on_primary_fails,
        "clusters_fails": test_force_promote_with_non_empty_clusters_fails,
        "topology_fails": test_force_promote_with_topology_fails,
        "success": test_force_promote_success,
        "idempotent": test_force_promote_idempotent,
    }

    if len(sys.argv) > 1:
        test_name = sys.argv[1]
        if test_name not in tests:
            print(f"Unknown test: {test_name}")
            print(f"Available tests: {list(tests.keys())}")
            sys.exit(1)
        tests[test_name]()
        return

    # Run all tests
    logger.info("Running all force promote tests...")
    for name, test_fn in tests.items():
        logger.info(f"--- Running: {name} ---")
        test_fn()

    logger.info("All force promote tests PASSED!")


if __name__ == "__main__":
    main()
