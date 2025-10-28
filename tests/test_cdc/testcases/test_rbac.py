from pymilvus import MilvusClient
from common import *
from loguru import logger

def generate_pchannels(cluster_id: str, pchannel_num: int = 16):
    pchannels = []
    for i in range(pchannel_num):
        pchannel_name = f"{cluster_id}-rootcoord-dml_{i}"
        pchannels.append(pchannel_name)
    return pchannels

def main():
    root_client = MilvusClient(
        uri=CLUSTER_A_ADDR,
        token="root:Milvus"
    ) 

    root_client.create_user(
        user_name="user_1",
        password="P@ssw0rd",
    )

    resp = root_client.describe_user("user_1")
    logger.info(f"User created: {resp}")

    root_client.create_role(role_name="role_a")
    logger.info(f"Roles: {root_client.list_roles()}")

    read_only_privileges = [
    {"object_type": "Global", "object_name": "*", "privilege": "DescribeCollection"},
    {"object_type": "Global", "object_name": "*", "privilege": "ShowCollections"},
    {"object_type": "Collection", "object_name": "quick_setup", "privilege": "Search"},
    {"object_type": "Collection", "object_name": "*", "privilege": "Query"}
    ] 

    for item in read_only_privileges:
        root_client.grant_privilege(
            role_name="role_a",
            object_type=item["object_type"],
            privilege=item["privilege"],
            object_name=item["object_name"]
        )

    root_client.grant_role(user_name="user_1", role_name="role_a")

    resp = root_client.describe_user(user_name="user_1")
    logger.info(f"User description: {resp}")

    user_client = MilvusClient(
        uri="http://localhost:19530",
        token="user_1:P@ssw0rd"
    )

    clusterA_addr = CLUSTER_A_ADDR
    clusterB_addr = CLUSTER_B_ADDR
    clusterA_id = "by-dev1"
    clusterB_id = "by-dev2"

    config = {
            "clusters": [
                {
                    "cluster_id": clusterA_id,
                    "connection_param": {
                        "uri": clusterA_addr,
                        "token": TOKEN
                    },
                    "pchannels": generate_pchannels(clusterA_id),
                },
                {
                    "cluster_id": clusterB_id,
                    "connection_param": {
                        "uri": clusterB_addr,
                        "token": TOKEN
                    },
                    "pchannels": generate_pchannels(clusterB_id),
                }
            ],
            "cross_cluster_topology": [
                {
                    "source_cluster_id": clusterA_id,
                    "target_cluster_id": clusterB_id
                }
            ]
        }

    # use readonly user to update replicate configuration
    try:
        user_client.update_replicate_configuration(**config)
        logger.error(f"Should raise error and not reach here")
    except Exception as e:
        logger.error(f"[Error] Failed to update replicate configuration as readonly user: {e}")

    # use root user to update replicate configuration
    root_client.update_replicate_configuration(**config)
    logger.info(f"Replicate source configuration updated successfully")

if __name__ == "__main__":
    main()
