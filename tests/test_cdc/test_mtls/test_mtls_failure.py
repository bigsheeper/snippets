"""
CDC mTLS E2E Test -- Failure tests for mTLS enforcement.

Verifies that:
  1. PyMilvus clients with bad/missing TLS credentials are rejected
  2. Cross-cluster CA mismatch is rejected (key validation for per-cluster CAs)
  3. CDC without per-cluster client certs cannot replicate to mTLS targets

All 3 clusters run with tlsMode=2 (mTLS), each with its own CA. CDC reads
per-cluster client certs from tls.clusters.<clusterID>.* in milvus.yaml (via
MILVUSCONF). The failure test restarts CDC with MILVUSCONF pointing to the
original config dir (which has per-cluster TLS commented out), so CDC connects
without client certs.

Usage:
  python test_mtls_failure.py
"""
import glob as globmod
import os
import signal
import subprocess
import tempfile
import time
import traceback

from loguru import logger
from pymilvus import MilvusClient

# NOTE: Do NOT import common.py at module level for Part 1 tests.
# Importing common.py creates global mTLS clients that establish gRPC channels.
# gRPC Python reuses these channels for subsequent MilvusClient instances to the
# same address, causing one-way TLS tests to falsely succeed.
# We import common.py lazily only for Part 2 (CDC replication failure test).

# --- Constants duplicated from common.py to avoid import side effects ---
TOKEN = "root:Milvus"
CERT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "certs")

# Per-cluster certs for cluster A (dev1) — used in Part 1 PyMilvus tests
CA_PEM_DEV1 = os.path.join(CERT_DIR, "ca-dev1.pem")
CLIENT_PEM_DEV1 = os.path.join(CERT_DIR, "pymilvus-dev1.pem")
CLIENT_KEY_DEV1 = os.path.join(CERT_DIR, "pymilvus-dev1.key")

# Per-cluster certs for cluster B (dev2) — used in cross-CA test
CA_PEM_DEV2 = os.path.join(CERT_DIR, "ca-dev2.pem")

CLUSTER_A_ID = "by-dev1"
CLUSTER_B_ID = "by-dev2"
CLUSTER_A_ADDR = "https://localhost:19530"
CLUSTER_B_ADDR = "https://localhost:19531"

COLLECTION_NAME = "test_mtls_failure"
CDC_WAIT_TIMEOUT = 30  # Short timeout — we expect failure, not success


# ================================================================
# Wrong cert generation
# ================================================================

def generate_wrong_certs(tmpdir):
    """Generate a separate CA + client cert (not trusted by any cluster CA)."""
    def run(cmd):
        subprocess.run(cmd, check=True, capture_output=True)

    # Wrong CA (self-signed, independent of all cluster CAs)
    run(["openssl", "genrsa", "-out", f"{tmpdir}/wrong-ca.key", "2048"])
    run(["openssl", "req", "-new", "-x509", "-key", f"{tmpdir}/wrong-ca.key",
         "-out", f"{tmpdir}/wrong-ca.pem", "-days", "1", "-subj", "/CN=WrongCA"])

    # Wrong client cert signed by the wrong CA
    run(["openssl", "genrsa", "-out", f"{tmpdir}/wrong-client.key", "2048"])
    run(["openssl", "req", "-new", "-key", f"{tmpdir}/wrong-client.key",
         "-out", f"{tmpdir}/wrong-client.csr", "-subj", "/CN=wrong-client"])
    run(["openssl", "x509", "-req", "-in", f"{tmpdir}/wrong-client.csr",
         "-CA", f"{tmpdir}/wrong-ca.pem", "-CAkey", f"{tmpdir}/wrong-ca.key",
         "-CAcreateserial", "-out", f"{tmpdir}/wrong-client.pem", "-days", "1"])

    return (
        f"{tmpdir}/wrong-ca.pem",
        f"{tmpdir}/wrong-client.pem",
        f"{tmpdir}/wrong-client.key",
    )


# ================================================================
# CDC process management
# ================================================================

def find_pid_by_metrics_port(metrics_port):
    """Find PID of the milvus CDC process with METRICS_PORT=<port> in /proc environ."""
    target = f"METRICS_PORT={metrics_port}".encode()
    for environ_path in globmod.glob("/proc/*/environ"):
        try:
            pid = int(environ_path.split("/")[2])
            with open(environ_path, "rb") as f:
                environ_data = f.read()
            if target not in environ_data.split(b"\0"):
                continue
            # Verify it's a milvus cdc process
            with open(f"/proc/{pid}/cmdline", "rb") as f:
                cmdline = f.read().decode(errors="replace")
            if "milvus" in cmdline and "cdc" in cmdline:
                return pid
        except (PermissionError, FileNotFoundError, ProcessLookupError, ValueError):
            continue
    return None


def read_proc_environ(pid):
    """Read environment variables from /proc/<pid>/environ."""
    with open(f"/proc/{pid}/environ", "rb") as f:
        data = f.read()
    env = {}
    for var in data.split(b"\0"):
        if b"=" in var:
            key, _, value = var.partition(b"=")
            env[key.decode(errors="replace")] = value.decode(errors="replace")
    return env


def kill_cdc_process(metrics_port):
    """Kill CDC process by metrics port. Returns its original environ dict."""
    pid = find_pid_by_metrics_port(metrics_port)
    if pid is None:
        raise RuntimeError(f"CDC process with METRICS_PORT={metrics_port} not found")

    env = read_proc_environ(pid)
    logger.info(f"Killing CDC process PID={pid} (METRICS_PORT={metrics_port})")
    os.kill(pid, signal.SIGTERM)

    # Wait for graceful exit
    for _ in range(10):
        try:
            os.kill(pid, 0)
            time.sleep(0.5)
        except ProcessLookupError:
            break
    else:
        logger.warning(f"CDC PID={pid} didn't exit gracefully, sending SIGKILL")
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    logger.info(f"CDC process PID={pid} terminated")
    return env


def start_cdc_process(env, log_path=None):
    """Start a CDC process with the given environment."""
    milvus_bin = os.path.join(os.environ["MILVUS_DEV_PATH"], "bin", "milvus")
    if log_path:
        stdout = open(log_path, "w")
    else:
        stdout = subprocess.DEVNULL
    proc = subprocess.Popen(
        [milvus_bin, "run", "cdc"],
        env=env,
        stdout=stdout,
        stderr=subprocess.STDOUT,
    )
    logger.info(f"Started CDC process PID={proc.pid}")
    return proc


# ================================================================
# Part 1: PyMilvus connection failure tests
# ================================================================

def test_plain_connection():
    """Test that plain (non-TLS) connection is rejected by mTLS server."""
    logger.info("Test: plain connection (no TLS) -> should be rejected")
    try:
        client = MilvusClient(uri="http://localhost:19530", token=TOKEN)
        client.list_collections()
        raise AssertionError("Expected connection to fail, but list_collections() succeeded")
    except AssertionError:
        raise
    except Exception as e:
        logger.info(f"PASSED: plain connection rejected: {type(e).__name__}: {e}")


def test_oneway_tls_only():
    """Test that one-way TLS (no client cert) is rejected by mTLS server."""
    logger.info("Test: one-way TLS only (no client cert) -> should be rejected")
    try:
        client = MilvusClient(
            uri="https://localhost:19530", token=TOKEN,
            server_pem_path=CA_PEM_DEV1,
        )
        client.list_collections()
        raise AssertionError("Expected connection to fail, but list_collections() succeeded")
    except AssertionError:
        raise
    except Exception as e:
        logger.info(f"PASSED: one-way TLS rejected: {type(e).__name__}: {e}")


def test_wrong_ca(wrong_ca_pem):
    """Test that wrong CA cert causes connection failure."""
    logger.info("Test: wrong CA cert -> should be rejected")
    try:
        client = MilvusClient(
            uri="https://localhost:19530", token=TOKEN,
            ca_pem_path=wrong_ca_pem,
            client_pem_path=CLIENT_PEM_DEV1,
            client_key_path=CLIENT_KEY_DEV1,
        )
        client.list_collections()
        raise AssertionError("Expected connection to fail, but list_collections() succeeded")
    except AssertionError:
        raise
    except Exception as e:
        logger.info(f"PASSED: wrong CA rejected: {type(e).__name__}: {e}")


def test_wrong_client_cert(wrong_client_pem, wrong_client_key):
    """Test that wrong client cert (not signed by trusted CA) is rejected."""
    logger.info("Test: wrong client cert -> should be rejected")
    try:
        client = MilvusClient(
            uri="https://localhost:19530", token=TOKEN,
            ca_pem_path=CA_PEM_DEV1,
            client_pem_path=wrong_client_pem,
            client_key_path=wrong_client_key,
        )
        client.list_collections()
        raise AssertionError("Expected connection to fail, but list_collections() succeeded")
    except AssertionError:
        raise
    except Exception as e:
        logger.info(f"PASSED: wrong client cert rejected: {type(e).__name__}: {e}")


def test_cross_cluster_ca_rejected():
    """Connecting to cluster B with cluster A's CA should fail.

    This is the KEY validation that per-cluster CAs are enforced.
    Cluster B's server cert is signed by ca-dev2, so using ca-dev1
    to verify it must fail. With the old shared CA, this would pass.
    """
    logger.info("Test: cross-cluster CA (A's CA for B's server) -> should be rejected")
    try:
        client = MilvusClient(
            uri=CLUSTER_B_ADDR, token=TOKEN,
            ca_pem_path=CA_PEM_DEV1,        # A's CA, not B's
            client_pem_path=CLIENT_PEM_DEV1,  # signed by A's CA, B won't trust it
            client_key_path=CLIENT_KEY_DEV1,
        )
        client.list_collections()
        raise AssertionError(
            "Expected connection to fail — cluster B should reject A's CA, "
            "but list_collections() succeeded. Are CAs still shared?"
        )
    except AssertionError:
        raise
    except Exception as e:
        logger.info(f"PASSED: cross-cluster CA rejected: {type(e).__name__}: {e}")


# ================================================================
# Part 2: CDC replication failure test
# ================================================================

def create_tls_client(uri, dev_num):
    """Create a PyMilvus client with mTLS using per-cluster certs."""
    return MilvusClient(
        uri=uri, token=TOKEN,
        ca_pem_path=os.path.join(CERT_DIR, f"ca-dev{dev_num}.pem"),
        client_pem_path=os.path.join(CERT_DIR, f"pymilvus-dev{dev_num}.pem"),
        client_key_path=os.path.join(CERT_DIR, f"pymilvus-dev{dev_num}.key"),
    )


def test_cdc_replication_without_client_certs():
    """Test that CDC without per-cluster client certs cannot replicate to mTLS targets.

    The running CDC has MILVUSCONF pointing to a config dir with per-cluster
    TLS entries (tls.clusters.<clusterID>.*). We restart CDC with MILVUSCONF
    pointing to the original config dir (which has these entries commented out),
    so CDC connects to the target cluster without client certs.

    Important: UpdateReplicateConfiguration on non-primary clusters waits for
    the primary's broadcast via CDC. If CDC is broken, the call hangs forever.
    Therefore we configure replication FIRST (while correct CDC is running),
    then kill and replace CDC. The replicate config is persisted in etcd, so
    the new CDC reloads it on startup and attempts replication (but fails).

    Steps:
      1. Configure replication A -> B, A -> C (while correct CDC is running)
      2. Kill cluster A's CDC process (METRICS_PORT=29091)
      3. Restart CDC with MILVUSCONF pointing to original configs (no per-cluster TLS)
      4. Create collection on A, insert data
      5. Wait bounded time — expect replication to NOT happen
      6. Assert B does not have the collection
      7. Cleanup: kill bad CDC, restart correct CDC, drop collection
    """
    # Lazy import: create mTLS clients only for Part 2
    from common import build_replicate_config
    from collection_helpers import create_collection_schema, create_index_params, generate_data

    cluster_A_client = create_tls_client(CLUSTER_A_ADDR, 1)
    cluster_B_client = create_tls_client(CLUSTER_B_ADDR, 2)
    cluster_C_client = create_tls_client("https://localhost:19532", 3)

    logger.info("Test: CDC without per-cluster client certs -> replication should fail")

    CDC_A_METRICS_PORT = 29091
    bad_cdc_proc = None
    original_env = None

    try:
        # Step 1: Configure replication A -> B, A -> C on ALL clusters.
        # The previous switchover test may have changed the primary to B,
        # making A secondary. Sending to all ensures the current primary
        # (whichever it is) processes the config change.
        logger.info("Step 1: Configure replication A -> B, A -> C (all clusters)")
        config = build_replicate_config(
            source_id=CLUSTER_A_ID,
            target_ids=[CLUSTER_B_ID, "by-dev3"],
        )
        from concurrent.futures import ThreadPoolExecutor
        all_clients = [cluster_A_client, cluster_B_client, cluster_C_client]
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(c.update_replicate_configuration, **config) for c in all_clients]
            for f in futures:
                f.result(timeout=60)
        logger.info("Replicate config applied to all clusters")

        # Step 2: Kill cluster A's CDC process and capture its environment
        logger.info("Step 2: Kill cluster A CDC (METRICS_PORT=29091)")
        original_env = kill_cdc_process(CDC_A_METRICS_PORT)

        # Step 3: Restart CDC with MILVUSCONF pointing to original configs
        # (which has per-cluster TLS commented out → CDC won't use client certs)
        logger.info("Step 3: Restart CDC without per-cluster TLS config")
        bad_env = original_env.copy()
        original_configs = os.path.join(os.environ["MILVUS_DEV_PATH"], "configs")
        bad_env["MILVUSCONF"] = original_configs
        bad_cdc_proc = start_cdc_process(bad_env)
        time.sleep(5)  # Give CDC time to initialize and reload replicate config

        # Step 4: Create collection on A, insert data
        logger.info("Step 4: Create collection on A and insert data")
        schema = create_collection_schema()
        cluster_A_client.create_collection(
            collection_name=COLLECTION_NAME, schema=schema, shards_num=1,
        )
        index_params = create_index_params(cluster_A_client)
        cluster_A_client.create_index(COLLECTION_NAME, index_params=index_params)
        cluster_A_client.load_collection(COLLECTION_NAME)
        data = generate_data(100, start_id=1)
        cluster_A_client.insert(COLLECTION_NAME, data)
        logger.info("Collection created and 100 rows inserted on A")

        # Step 5: Wait bounded time — expect replication to NOT happen
        logger.info(f"Step 5: Waiting {CDC_WAIT_TIMEOUT}s (expecting replication failure)")
        start = time.time()
        while time.time() - start < CDC_WAIT_TIMEOUT:
            if cluster_B_client.has_collection(COLLECTION_NAME):
                raise AssertionError(
                    "Collection appeared on B — replication succeeded without client certs!"
                )
            time.sleep(2)

        # Step 6: Final assertion
        assert not cluster_B_client.has_collection(COLLECTION_NAME), \
            "Collection should NOT exist on B after timeout"
        logger.info("PASSED: CDC without client certs failed to replicate (as expected)")

    finally:
        # Step 7: Cleanup
        logger.info("Cleanup: restoring original CDC and dropping test collection")

        # Kill the bad CDC process
        if bad_cdc_proc is not None:
            bad_cdc_proc.terminate()
            try:
                bad_cdc_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                bad_cdc_proc.kill()
                bad_cdc_proc.wait()
            logger.info("Bad CDC process terminated")

        # Restart the original CDC with correct certs
        if original_env is not None:
            start_cdc_process(original_env)
            time.sleep(5)
            logger.info("Original CDC process restored")

        # Drop test collection on A
        try:
            if cluster_A_client.has_collection(COLLECTION_NAME):
                cluster_A_client.release_collection(COLLECTION_NAME)
                cluster_A_client.drop_collection(COLLECTION_NAME)
                logger.info(f"Dropped {COLLECTION_NAME} on A")
        except Exception as e:
            logger.warning(f"Cleanup drop on A failed: {e}")

        # Drop on B just in case
        try:
            if cluster_B_client.has_collection(COLLECTION_NAME):
                cluster_B_client.release_collection(COLLECTION_NAME)
                cluster_B_client.drop_collection(COLLECTION_NAME)
                logger.info(f"Dropped {COLLECTION_NAME} on B")
        except Exception as e:
            logger.warning(f"Cleanup drop on B failed: {e}")


# ================================================================
# Main
# ================================================================

def main():
    logger.info("=" * 60)
    logger.info("mTLS Failure Tests")
    logger.info("=" * 60)

    # Generate wrong certs for tests 3 and 4
    tmpdir = tempfile.mkdtemp(prefix="mtls-wrong-certs-")
    try:
        wrong_ca_pem, wrong_client_pem, wrong_client_key = generate_wrong_certs(tmpdir)
        logger.info(f"Wrong certs generated in {tmpdir}")

        # Part 1: PyMilvus connection failure tests
        logger.info("-" * 60)
        logger.info("Part 1: PyMilvus connection failure tests")
        logger.info("-" * 60)

        test_plain_connection()
        test_oneway_tls_only()
        test_wrong_ca(wrong_ca_pem)
        test_wrong_client_cert(wrong_client_pem, wrong_client_key)
        test_cross_cluster_ca_rejected()

        # Part 2: CDC replication failure test
        logger.info("-" * 60)
        logger.info("Part 2: CDC replication failure test")
        logger.info("-" * 60)

        test_cdc_replication_without_client_certs()

    finally:
        subprocess.run(["rm", "-rf", tmpdir], check=False)
        logger.info(f"Cleaned up {tmpdir}")

    logger.info("=" * 60)
    logger.info("ALL PASSED: mTLS failure tests")
    logger.info("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.error(traceback.format_exc())
        raise
