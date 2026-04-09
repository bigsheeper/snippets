"""Test RBAC: read-only user cannot call update_replicate_configuration."""
import _path_setup  # noqa: F401
from loguru import logger
from pymilvus import MilvusClient
from common import (
    cluster_A_client, CLUSTER_A_ADDR, CLUSTER_B_ADDR,
    CLUSTER_A_ID, CLUSTER_B_ID, TOKEN,
    build_replicate_config_2,
)

USER = "test_rbac_user"
PASSWORD = "P@ssw0rd"
ROLE = "test_rbac_readonly"


def test_rbac():
    root = cluster_A_client

    # Setup: create user + role with read-only privileges
    try:
        root.drop_user(USER)
    except Exception:
        pass
    try:
        root.drop_role(ROLE)
    except Exception:
        pass

    root.create_user(user_name=USER, password=PASSWORD)
    root.create_role(role_name=ROLE)

    for priv in [
        {"object_type": "Global", "object_name": "*", "privilege": "DescribeCollection"},
        {"object_type": "Global", "object_name": "*", "privilege": "ShowCollections"},
        {"object_type": "Collection", "object_name": "*", "privilege": "Search"},
        {"object_type": "Collection", "object_name": "*", "privilege": "Query"},
    ]:
        root.grant_privilege(role_name=ROLE, **priv)

    root.grant_role(user_name=USER, role_name=ROLE)
    logger.info(f"Created user={USER} with role={ROLE}")

    # Test: read-only user should fail to update replicate config
    user_client = MilvusClient(uri=CLUSTER_A_ADDR, token=f"{USER}:{PASSWORD}")
    config = build_replicate_config_2(CLUSTER_A_ID, CLUSTER_B_ID)

    try:
        user_client.update_replicate_configuration(**config)
        raise AssertionError("Read-only user should NOT be able to update replicate config")
    except AssertionError:
        raise
    except Exception as e:
        logger.info(f"Got expected permission error: {e}")

    # Verify root CAN update
    root.update_replicate_configuration(**config)
    logger.info("Root user updated replicate config successfully")

    # Cleanup
    root.revoke_role(user_name=USER, role_name=ROLE)
    root.drop_user(USER)
    root.drop_role(ROLE)

    logger.info("PASSED: RBAC test")


if __name__ == "__main__":
    test_rbac()
