import os
from loguru import logger
from pymilvus import MilvusClient
from common.constants import CLUSTER_A_ADDR, CLUSTER_B_ADDR, CLUSTER_C_ADDR, TOKEN


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


def _is_port_open(uri, timeout=1.0):
    """Return True if the TCP port in the uri is reachable within timeout seconds."""
    import socket
    try:
        # Strip scheme prefix
        host_port = uri
        for scheme in ("tcp://", "http://", "https://"):
            if host_port.startswith(scheme):
                host_port = host_port[len(scheme):]
                break
        host, port_str = host_port.rsplit(":", 1)
        port = int(port_str)
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def _try_create_client(uri):
    """Create client; return None if cluster is not reachable (e.g. not started).

    Does a quick TCP probe first to avoid hanging on MilvusClient.__init__
    when the target port is unreachable.
    """
    if not _is_port_open(uri):
        return None
    try:
        return _create_client(uri)
    except Exception:
        return None


# --- Global clients ---
cluster_A_client = _create_client(CLUSTER_A_ADDR)

if os.getenv("CLUSTER_B_ADDR"):
    cluster_B_client = _create_client(CLUSTER_B_ADDR)
else:
    cluster_B_client = _probe_cluster_b()

# Cluster C is optional (not all test setups start it).
cluster_C_client = _try_create_client(CLUSTER_C_ADDR)


def reconnect_clients():
    """Reconnect global clients after cluster restart."""
    global cluster_A_client, cluster_B_client, cluster_C_client
    for c in [cluster_A_client, cluster_B_client, cluster_C_client]:
        if c is None:
            continue
        try:
            c.close()
        except Exception:
            pass
    cluster_A_client = _create_client(CLUSTER_A_ADDR)
    cluster_B_client = _create_client(CLUSTER_B_ADDR)
    cluster_C_client = _try_create_client(CLUSTER_C_ADDR)
    logger.info(f"Reconnected clients (C available: {cluster_C_client is not None})")
