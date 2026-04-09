import os
from loguru import logger
from pymilvus import MilvusClient
from common.constants import CLUSTER_A_ADDR, CLUSTER_B_ADDR, TOKEN


def _create_client(uri, token=TOKEN):
    return MilvusClient(uri=uri, token=token)


def _probe_cluster_b():
    """Try multiple ports to find cluster B when env not set."""
    candidates = [f"tcp://localhost:{p}" for p in range(19531, 19551, 2)] + ["tcp://localhost:19528"]
    last_exc = None
    for cand in candidates:
        try:
            client = MilvusClient(uri=cand, token=TOKEN)
            os.environ["CLUSTER_B_ADDR"] = cand
            return client
        except Exception as e:
            last_exc = e
    raise last_exc or RuntimeError("Failed to connect to cluster B")


# --- Global clients ---
cluster_A_client = _create_client(CLUSTER_A_ADDR)

if os.getenv("CLUSTER_B_ADDR"):
    cluster_B_client = _create_client(CLUSTER_B_ADDR)
else:
    cluster_B_client = _probe_cluster_b()


def reconnect_clients():
    """Reconnect global clients after cluster restart."""
    global cluster_A_client, cluster_B_client
    for c in [cluster_A_client, cluster_B_client]:
        try:
            c.close()
        except Exception:
            pass
    cluster_A_client = _create_client(CLUSTER_A_ADDR)
    cluster_B_client = _create_client(CLUSTER_B_ADDR)
    logger.info("Reconnected both clients")
