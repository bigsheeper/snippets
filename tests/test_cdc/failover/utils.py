from test_replica_features import cluster_control as cc
from port_layout import CLUSTER_B_PROXY_HEALTH
from testcases.common import (
    cluster_A_client, cluster_B_client, CLUSTER_A_ADDR, CLUSTER_B_ADDR, TOKEN,
)
import os
import sys
import time
import threading
from typing import Callable
from loguru import logger

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE not in sys.path:
    sys.path.insert(0, BASE)


def _normalize_http_uri(addr: str) -> str:
    """Normalize client addr to http(s) style expected by replicate configuration.
    Examples:
    - tcp://localhost:19530 -> http://localhost:19530
    - http://localhost:19530 -> keep
    - https://localhost:19530 -> keep
    - localhost:19530 -> http://localhost:19530
    """
    if addr.startswith("tcp://"):
        return "http://" + addr[len("tcp://"):]
    if addr.startswith("http://") or addr.startswith("https://"):
        return addr
    if "://" not in addr:
        return "http://" + addr
    return addr


A_HTTP_URI = _normalize_http_uri(CLUSTER_A_ADDR)
B_HTTP_URI = _normalize_http_uri(CLUSTER_B_ADDR)

CLUSTER_A_ID = "by-dev1"
CLUSTER_B_ID = "by-dev2"
PCHANNEL_NUM = 16


def _pchannels(cluster_id: str, n: int = PCHANNEL_NUM):
    return [f"{cluster_id}-rootcoord-dml_{i}" for i in range(n)]


STANDALONE_B = {
    "clusters": [
        {
            "cluster_id": CLUSTER_B_ID,
            "connection_param": {"uri": B_HTTP_URI, "token": TOKEN},
            "pchannels": _pchannels(CLUSTER_B_ID),
        }
    ],
    "cross_cluster_topology": [],
}

REPL_A_TO_B = {
    "clusters": [
        {
            "cluster_id": CLUSTER_A_ID,
            "connection_param": {"uri": A_HTTP_URI, "token": TOKEN},
            "pchannels": _pchannels(CLUSTER_A_ID),
        },
        {
            "cluster_id": CLUSTER_B_ID,
            "connection_param": {"uri": B_HTTP_URI, "token": TOKEN},
            "pchannels": _pchannels(CLUSTER_B_ID),
        },
    ],
    "cross_cluster_topology": [
        {"source_cluster_id": CLUSTER_A_ID, "target_cluster_id": CLUSTER_B_ID}
    ],
}


def ensure_secondary_b():
    logger.info("Resetting B to secondary via config + restart")
    cluster_B_client.update_replicate_configuration(**REPL_A_TO_B)
    cluster_A_client.update_replicate_configuration(**REPL_A_TO_B)
    cc.stop_cluster_b()
    time.sleep(2)
    cc.start_cluster_b()
    # 等待共享代理健康检查通过
    try:
        cc._wait_for_health(CLUSTER_B_PROXY_HEALTH, "Cluster B")
    except Exception:
        pass


def init_replication_a_to_b():
    cluster_B_client.update_replicate_configuration(**REPL_A_TO_B)
    cluster_A_client.update_replicate_configuration(**REPL_A_TO_B)
    logger.info("Replication initialized: A -> B")


def force_promote_b():
    cluster_B_client.update_replicate_configuration(
        **STANDALONE_B, force_promote=True)
    logger.info("Force promote called on B")


def start_async(fn: Callable, *args, **kwargs):
    result = {"exc": None}

    def _run():
        try:
            fn(*args, **kwargs)
        except Exception as e:
            result["exc"] = e
    t = threading.Thread(target=_run, daemon=True)
    t.start()

    def join(timeout: float | None = None):
        t.join(timeout)
        if t.is_alive():
            raise TimeoutError("async operation did not finish in time")
        if result["exc"]:
            raise result["exc"]
    return t, join


def await_rows(client, collection: str, count: int, timeout: int = 120):
    from testcases.query import query_on_primary
    start = time.time()
    last = None
    while time.time() - start < timeout:
        try:
            last = query_on_primary(collection, count, client)
            return last
        except Exception:
            time.sleep(2)
    raise TimeoutError(f"Expected {count} rows in {collection}, last={last}")
