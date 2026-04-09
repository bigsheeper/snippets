"""CDC mTLS E2E: Failure tests for mTLS enforcement.

Part 1: PyMilvus clients with bad/missing TLS credentials are rejected
Part 2: CDC without per-cluster client certs cannot replicate to mTLS targets
"""
import glob as globmod
import os
import signal
import subprocess
import tempfile
import time
import traceback
import _path_setup  # noqa: F401
from loguru import logger
from pymilvus import MilvusClient
from common.schema import create_collection_schema, default_index_params, generate_data

# Constants (duplicated from common to avoid gRPC channel reuse side effects)
TOKEN = "root:Milvus"
CERT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "certs")
CA_PEM_DEV1 = os.path.join(CERT_DIR, "ca-dev1.pem")
CLIENT_PEM_DEV1 = os.path.join(CERT_DIR, "pymilvus-dev1.pem")
CLIENT_KEY_DEV1 = os.path.join(CERT_DIR, "pymilvus-dev1.key")
CA_PEM_DEV2 = os.path.join(CERT_DIR, "ca-dev2.pem")

CLUSTER_A_ID = "by-dev1"
CLUSTER_B_ID = "by-dev2"
CLUSTER_A_ADDR = "https://localhost:19530"
CLUSTER_B_ADDR = "https://localhost:19531"
COLLECTION_NAME = "test_mtls_failure"
CDC_WAIT_TIMEOUT = 30


def _expect_connection_fail(label, **client_kwargs):
    """Verify MilvusClient(**kwargs).list_collections() fails."""
    logger.info(f"Test: {label} -> should be rejected")
    try:
        client = MilvusClient(**client_kwargs)
        client.list_collections()
        raise AssertionError(f"Expected {label} to fail, but succeeded")
    except AssertionError:
        raise
    except Exception as e:
        logger.info(f"PASSED: {label} rejected: {type(e).__name__}: {e}")


# ---- CDC process management ----

def find_pid_by_metrics_port(metrics_port):
    target = f"METRICS_PORT={metrics_port}".encode()
    for path in globmod.glob("/proc/*/environ"):
        try:
            pid = int(path.split("/")[2])
            with open(path, "rb") as f:
                data = f.read()
            if target not in data.split(b"\0"):
                continue
            with open(f"/proc/{pid}/cmdline", "rb") as f:
                cmdline = f.read().decode(errors="replace")
            if "milvus" in cmdline and "cdc" in cmdline:
                return pid
        except (PermissionError, FileNotFoundError, ProcessLookupError, ValueError):
            continue
    return None


def read_proc_environ(pid):
    with open(f"/proc/{pid}/environ", "rb") as f:
        data = f.read()
    env = {}
    for var in data.split(b"\0"):
        if b"=" in var:
            key, _, value = var.partition(b"=")
            env[key.decode(errors="replace")] = value.decode(errors="replace")
    return env


def kill_cdc_process(metrics_port):
    pid = find_pid_by_metrics_port(metrics_port)
    if pid is None:
        raise RuntimeError(f"CDC with METRICS_PORT={metrics_port} not found")
    env = read_proc_environ(pid)
    logger.info(f"Killing CDC PID={pid}")
    os.kill(pid, signal.SIGTERM)
    for _ in range(10):
        try:
            os.kill(pid, 0)
            time.sleep(0.5)
        except ProcessLookupError:
            break
    else:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    return env


def start_cdc_process(env, log_path=None):
    milvus_bin = os.path.join(os.environ["MILVUS_DEV_PATH"], "bin", "milvus")
    stdout = open(log_path, "w") if log_path else subprocess.DEVNULL
    proc = subprocess.Popen([milvus_bin, "run", "cdc"], env=env, stdout=stdout, stderr=subprocess.STDOUT)
    logger.info(f"Started CDC PID={proc.pid}")
    return proc


# ---- Part 1: PyMilvus connection failure tests ----

def test_plain_connection():
    _expect_connection_fail("plain (no TLS)", uri="http://localhost:19530", token=TOKEN)


def test_oneway_tls_only():
    _expect_connection_fail("one-way TLS only", uri="https://localhost:19530", token=TOKEN,
                            server_pem_path=CA_PEM_DEV1)


def test_wrong_ca(wrong_ca_pem):
    _expect_connection_fail("wrong CA", uri="https://localhost:19530", token=TOKEN,
                            ca_pem_path=wrong_ca_pem, client_pem_path=CLIENT_PEM_DEV1,
                            client_key_path=CLIENT_KEY_DEV1)


def test_wrong_client_cert(wrong_client_pem, wrong_client_key):
    _expect_connection_fail("wrong client cert", uri="https://localhost:19530", token=TOKEN,
                            ca_pem_path=CA_PEM_DEV1, client_pem_path=wrong_client_pem,
                            client_key_path=wrong_client_key)


def test_cross_cluster_ca_rejected():
    _expect_connection_fail("cross-cluster CA (A's CA for B)", uri=CLUSTER_B_ADDR, token=TOKEN,
                            ca_pem_path=CA_PEM_DEV1, client_pem_path=CLIENT_PEM_DEV1,
                            client_key_path=CLIENT_KEY_DEV1)


# ---- Part 2: CDC replication failure without certs ----

def _create_tls_client(uri, dev_num):
    return MilvusClient(
        uri=uri, token=TOKEN,
        ca_pem_path=os.path.join(CERT_DIR, f"ca-dev{dev_num}.pem"),
        client_pem_path=os.path.join(CERT_DIR, f"pymilvus-dev{dev_num}.pem"),
        client_key_path=os.path.join(CERT_DIR, f"pymilvus-dev{dev_num}.key"),
    )


def test_cdc_replication_without_client_certs():
    from common.config import build_replicate_config_star
    from concurrent.futures import ThreadPoolExecutor

    client_a = _create_tls_client(CLUSTER_A_ADDR, 1)
    client_b = _create_tls_client(CLUSTER_B_ADDR, 2)
    client_c = _create_tls_client("https://localhost:19532", 3)

    CDC_A_METRICS_PORT = 29091
    bad_cdc = None
    orig_env = None

    try:
        # Configure replication while correct CDC is running
        config = build_replicate_config_star(CLUSTER_A_ID, [CLUSTER_B_ID, "by-dev3"])
        all_clients = [client_a, client_b, client_c]
        with ThreadPoolExecutor(max_workers=3) as ex:
            for f in [ex.submit(c.update_replicate_configuration, **config) for c in all_clients]:
                f.result(timeout=60)

        # Kill CDC and restart without per-cluster TLS
        orig_env = kill_cdc_process(CDC_A_METRICS_PORT)
        bad_env = orig_env.copy()
        bad_env["MILVUSCONF"] = os.path.join(os.environ["MILVUS_DEV_PATH"], "configs")
        bad_cdc = start_cdc_process(bad_env)
        time.sleep(5)

        # Create collection + insert on A
        schema = create_collection_schema()
        client_a.create_collection(collection_name=COLLECTION_NAME, schema=schema, shards_num=1)
        idx = default_index_params(client_a)
        client_a.create_index(COLLECTION_NAME, index_params=idx)
        client_a.load_collection(COLLECTION_NAME)
        client_a.insert(COLLECTION_NAME, generate_data(100, 1))

        # Wait — expect replication to NOT happen
        start = time.time()
        while time.time() - start < CDC_WAIT_TIMEOUT:
            if client_b.has_collection(COLLECTION_NAME):
                raise AssertionError("Collection appeared on B without client certs!")
            time.sleep(2)

        assert not client_b.has_collection(COLLECTION_NAME)
        logger.info("PASSED: CDC without certs failed to replicate")

    finally:
        if bad_cdc:
            bad_cdc.terminate()
            try:
                bad_cdc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                bad_cdc.kill()
                bad_cdc.wait()
        if orig_env:
            start_cdc_process(orig_env)
            time.sleep(5)
        for c in [client_a, client_b]:
            try:
                if c.has_collection(COLLECTION_NAME):
                    c.release_collection(COLLECTION_NAME)
                    c.drop_collection(COLLECTION_NAME)
            except Exception:
                pass


# ---- Main ----

def generate_wrong_certs(tmpdir):
    def run(cmd):
        subprocess.run(cmd, check=True, capture_output=True)
    run(["openssl", "genrsa", "-out", f"{tmpdir}/wrong-ca.key", "2048"])
    run(["openssl", "req", "-new", "-x509", "-key", f"{tmpdir}/wrong-ca.key",
         "-out", f"{tmpdir}/wrong-ca.pem", "-days", "1", "-subj", "/CN=WrongCA"])
    run(["openssl", "genrsa", "-out", f"{tmpdir}/wrong-client.key", "2048"])
    run(["openssl", "req", "-new", "-key", f"{tmpdir}/wrong-client.key",
         "-out", f"{tmpdir}/wrong-client.csr", "-subj", "/CN=wrong-client"])
    run(["openssl", "x509", "-req", "-in", f"{tmpdir}/wrong-client.csr",
         "-CA", f"{tmpdir}/wrong-ca.pem", "-CAkey", f"{tmpdir}/wrong-ca.key",
         "-CAcreateserial", "-out", f"{tmpdir}/wrong-client.pem", "-days", "1"])
    return f"{tmpdir}/wrong-ca.pem", f"{tmpdir}/wrong-client.pem", f"{tmpdir}/wrong-client.key"


def main():
    logger.info("=" * 60)
    logger.info("mTLS Failure Tests")
    logger.info("=" * 60)

    tmpdir = tempfile.mkdtemp(prefix="mtls-wrong-certs-")
    try:
        wrong_ca, wrong_pem, wrong_key = generate_wrong_certs(tmpdir)

        logger.info("--- Part 1: PyMilvus connection failures ---")
        test_plain_connection()
        test_oneway_tls_only()
        test_wrong_ca(wrong_ca)
        test_wrong_client_cert(wrong_pem, wrong_key)
        test_cross_cluster_ca_rejected()

        logger.info("--- Part 2: CDC replication failure ---")
        test_cdc_replication_without_client_certs()
    finally:
        subprocess.run(["rm", "-rf", tmpdir], check=False)

    logger.info("ALL PASSED: mTLS failure tests")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.error(traceback.format_exc())
        raise
