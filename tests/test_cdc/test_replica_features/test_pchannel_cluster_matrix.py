"""State matrix test: pchannel increase × cluster/topology changes.

All cases verify the relaxed validator constraint tracked by issue #48993 —
pchannel growth can now happen simultaneously with cluster set / topology
changes (previously rejected by validatePChannelIncreasingConstraints).

  M1: P + C+         — standalone A (N pchannels) + restart to N+1 + add B
  M2: P + C-         — A→B (N pchannels) + restart to N+1 + remove B (back to A)
  M3: P + C+ + switch — M1 followed by switchover A↔B
  M4: C- → scale → P + C+ — #48993 production scenario:
                             A→B → remove B → scale pchannels → re-add B

Test strategy (mirrors test_pchannel_increase.py):
  - Every case establishes an initial replicate config (Round 0 baseline).
  - Restart is used to expand pchannel count (dml_channel_num env).
  - Collection shard_num is always INIT_PCHANNEL_NUM (new pchannels don't need
    collection traffic; only the AlterReplicateConfig broadcast exercises them).
  - Topology dismantling is symmetric: both A and B get their own
    single-cluster standalone config in parallel via `_dismantle_to_standalone`
    — NO force_promote anywhere. A's call goes through the normal broadcast
    path; B's call falls into `waitUntilPrimaryChangeOrConfigurationSame`
    which unblocks once CDC has delivered the (server-rewritten) B-only
    AlterReplicateConfig. See `_dismantle_to_standalone` docstring for the
    full race-resolution picture.

Usage:
  python test_pchannel_cluster_matrix.py
  python test_pchannel_cluster_matrix.py --case M4
"""
import argparse
import signal
import traceback
from concurrent.futures import ThreadPoolExecutor
import _path_setup  # noqa: F401
from loguru import logger
from common import (
    CLUSTER_A_ID, CLUSTER_B_ID, CLUSTER_C_ID,
    setup_collection, insert_and_verify,
    reconnect_clients, restart_both_clusters, restart_all_three_clusters,
)
from common.config import build_standalone_config, build_replicate_config_star
from common import clients as _clients_mod
from test_replica_features.common import get_primary_and_standby
from common.config import build_replicate_config_2, update_replicate_config_on_clients

INIT_PCHANNEL_NUM = 16
CLUSTER_B_REPLICA = 2
CASE_TIMEOUT_SEC = 180  # abort a case if it runs longer than this


class CaseTimeout(Exception):
    pass


class _Deadline:
    """Per-case SIGALRM watchdog — raises CaseTimeout if the case hangs."""

    def __init__(self, seconds):
        self.seconds = seconds
        self.prev_handler = None

    def __enter__(self):
        def _handler(signum, frame):
            raise CaseTimeout(f"case exceeded {self.seconds}s deadline")
        self.prev_handler = signal.signal(signal.SIGALRM, _handler)
        signal.alarm(self.seconds)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        signal.alarm(0)
        if self.prev_handler is not None:
            signal.signal(signal.SIGALRM, self.prev_handler)
        return False


GRPC_TIMEOUT_SEC = 60  # gRPC deadline for update_replicate_configuration calls


def update_replicate_config(source_id, target_id, pchannel_num):
    """Update replicate config on both A and B clusters with gRPC timeout."""
    config = build_replicate_config_2(source_id, target_id, pchannel_num=pchannel_num)
    # Pass timeout into pymilvus -> gRPC deadline so hung broadcast surfaces
    # as DeadlineExceeded rather than an indefinite block.
    config_with_timeout = {**config, "timeout": GRPC_TIMEOUT_SEC}
    update_replicate_config_on_clients(
        [_clients_mod.cluster_A_client, _clients_mod.cluster_B_client],
        config_with_timeout,
    )
    logger.info(f"Replicate config: {source_id} -> {target_id}, pchannels={pchannel_num}")


def _set_a_standalone(pchannel_num):
    """Seed primary A with a standalone single-cluster config (baseline only).

    Use for M1/M3 Step 1 where currentConfig starts nil — writes {clusters:[A]}
    so the next A→B update is seen by the validator as a cluster-set change
    rather than fresh initialization. NO force_promote, NO client-side B call.

    Do NOT call this to dismantle an existing A→B topology; use
    `_dismantle_to_standalone` for that instead so B's demotion is
    explicitly synchronized.
    """
    config = build_standalone_config(CLUSTER_A_ID, pchannel_num=pchannel_num)
    _clients_mod.cluster_A_client.update_replicate_configuration(
        clusters=config["clusters"],
        cross_cluster_topology=config["cross_cluster_topology"],
        timeout=GRPC_TIMEOUT_SEC,
    )
    logger.info(f"Set A to standalone, pchannels={pchannel_num}")


def _dismantle_to_standalone(pchannel_num):
    """Dismantle A→B by submitting each cluster its own standalone config.

    Symmetric, fully synchronized, NO force_promote anywhere:

    - A (primary) ← {clusters:[A], topology:[]}
        Normal path on A: validator accepts (PR #48993 relaxation allows
        pchannel growth × cluster-set shrink), A broadcasts AlterReplicateConfig
        to its own pchannels. A's CDC then propagates to B.

    - B (secondary) ← {clusters:[B], topology:[]}
        `StartBroadcastWithResourceKeys` fails with ErrNotPrimary (B is still
        secondary when the call arrives), so the server falls into
        `waitUntilPrimaryChangeOrConfigurationSame` (assignment.go:94-139) and
        blocks on `WatchChannelAssignments` until B's local replicate config
        proto-equals the submitted {[B]}.

    Race resolution (why parallel calls work):
        A's broadcast hits B's WAL via CDC. On B's proxy,
        `replicate_service.overwriteAlterReplicateConfigMessage`
        (replicate_service.go:226-256) detects ErrCurrentClusterNotFound for
        the incoming A-only cfg and rewrites the header to {clusters:[B]}.
        B's `replicate_interceptor.SwitchReplicateMode` drops B's
        secondaryState. B's balancer persists {[B]} as its new replicate
        config — which is exactly what the B-side call is waiting for.

    Running both calls in parallel (ThreadPoolExecutor) is required:
    if B is called alone first it blocks forever (no A broadcast to
    trigger CDC); if A is called alone the test has no explicit
    synchronization confirming B has actually demoted.
    """
    a_cfg = build_standalone_config(CLUSTER_A_ID, pchannel_num=pchannel_num)
    b_cfg = build_standalone_config(CLUSTER_B_ID, pchannel_num=pchannel_num)
    with ThreadPoolExecutor(max_workers=2) as ex:
        fa = ex.submit(
            _clients_mod.cluster_A_client.update_replicate_configuration,
            clusters=a_cfg["clusters"],
            cross_cluster_topology=a_cfg["cross_cluster_topology"],
            timeout=GRPC_TIMEOUT_SEC,
        )
        fb = ex.submit(
            _clients_mod.cluster_B_client.update_replicate_configuration,
            clusters=b_cfg["clusters"],
            cross_cluster_topology=b_cfg["cross_cluster_topology"],
            timeout=GRPC_TIMEOUT_SEC,
        )
        fa.result()
        fb.result()
    logger.info(f"Dismantled A→B: both standalone, pchannels={pchannel_num}")


def _verify_replication(name, source_id, shard_num=INIT_PCHANNEL_NUM):
    """Create collection on primary, insert data, verify on standby."""
    primary, standby = get_primary_and_standby(source_id)
    setup_collection(name, primary, standby, shard_num=shard_num)
    insert_and_verify(name, primary, standby)
    logger.info(f"Replication verified: {name} ({source_id} -> standby)")


def _wait_cluster_fully_ready(client, label, timeout=120):
    """End-to-end readiness probe — create + drop a throwaway collection.

    proxy /healthz returning OK does NOT mean:
    - all streamingnodes have registered to etcd
    - balancer has assigned every pchannel to a streamingnode
    - DDL broadcast (which requires producers on every pchannel) works
    This probe requires all of the above; it fails otherwise.
    """
    import time as _time
    from common.schema import create_collection_schema
    name = f"__readiness_probe_{label}_{int(_time.time()*1000)}"
    deadline = _time.time() + timeout
    last_err = None
    while _time.time() < deadline:
        try:
            schema = create_collection_schema()
            client.create_collection(collection_name=name, schema=schema, timeout=15)
            client.drop_collection(name, timeout=15)
            logger.info(f"{label} readiness probe passed (create+drop OK)")
            return
        except Exception as e:
            last_err = e
            # try again; pchannels may not be fully assigned yet
            _time.sleep(2)
    raise RuntimeError(f"{label} not ready within {timeout}s: {last_err}")


_PULSAR_CONTAINER = "milvus_control-pulsar-1"


def _http_probe(url, timeout=3, expect_body_contains=None, expect_http_200=True):
    """Tiny curl-based HTTP probe. Returns True iff probe succeeds within timeout.

    We shell out to curl instead of urllib so a hung server times out cleanly
    rather than blocking the Python thread on DNS / connect retries.
    """
    import subprocess
    try:
        res = subprocess.run(
            ["curl", "-sS", "-o", "-", "-w", "\n__HTTP__%{http_code}",
             "-m", str(timeout), url],
            capture_output=True, text=True, timeout=timeout + 3,
        )
    except subprocess.TimeoutExpired:
        return False
    if res.returncode != 0:
        return False
    body, _, tail = res.stdout.rpartition("\n__HTTP__")
    code = tail.strip()
    if expect_http_200 and code != "200":
        return False
    if expect_body_contains is not None and expect_body_contains not in body:
        return False
    return True


def _ensure_pulsar_healthy(max_wait=180):
    """Ensure Pulsar admin HTTP actually responds before we start milvus.

    We have observed Pulsar entering a frozen state (TCP listening on 18080
    but HTTP handler deadlocked) after container churn. That state blocks
    every milvus streamingnode producer, which blocks channel assignment,
    which blocks every CreateCollection — including our readiness probe.

    Probe: `GET /admin/v2/brokers/healthy` with a 5s deadline.
    Recovery: if the probe fails, `docker restart` the pulsar container,
    then poll the probe until it succeeds or max_wait expires.
    """
    import subprocess
    import time
    if _http_probe("http://localhost:18080/admin/v2/brokers/healthy", timeout=5):
        return
    logger.warning(
        "Pulsar admin HTTP unresponsive (frozen JVM?); restarting container"
    )
    subprocess.run(
        ["docker", "restart", _PULSAR_CONTAINER],
        check=True, capture_output=True, text=True, timeout=60,
    )
    deadline = time.time() + max_wait
    while time.time() < deadline:
        if _http_probe("http://localhost:18080/admin/v2/brokers/healthy", timeout=5):
            logger.info("Pulsar admin HTTP ready after restart")
            return
        time.sleep(3)
    raise RuntimeError(f"Pulsar did not become healthy within {max_wait}s after restart")


def _ensure_etcd_healthy(max_wait=60):
    """Probe etcd /health until it returns {"health":"true"}."""
    import time
    deadline = time.time() + max_wait
    while time.time() < deadline:
        if _http_probe(
            "http://localhost:2379/health",
            timeout=3,
            expect_body_contains='"health":"true"',
        ):
            return
        time.sleep(2)
    raise RuntimeError(f"etcd did not become healthy within {max_wait}s")


def _ensure_minio_healthy(max_wait=60):
    """Probe minio /minio/health/live for HTTP 200."""
    import time
    deadline = time.time() + max_wait
    while time.time() < deadline:
        if _http_probe("http://localhost:9000/minio/health/live", timeout=3):
            return
        time.sleep(2)
    raise RuntimeError(f"minio did not become healthy within {max_wait}s")


def _reset_to_initial():
    """Reset both clusters to fresh INIT_PCHANNEL_NUM state.

    Kills all milvus processes, wipes cluster etcd prefixes, ensures the
    three-party services (pulsar / minio / etcd) are actually serving —
    NOT just TCP-listening — then starts clean. Required between cases
    because pchannel count cannot shrink via restart alone (rootcoord keeps
    the persisted channel assignments).
    """
    import subprocess
    import os
    logger.info(f"=== Reset: wipe etcd + fresh start with {INIT_PCHANNEL_NUM} pchannels ===")
    # Stop all milvus processes
    subprocess.run("pkill -9 -f 'milvus run' 2>/dev/null; sleep 2; rm -f /tmp/milvus/*.pid 2>/dev/null; true",
                   shell=True, check=False)
    # Third-party readiness gates — each probes the service layer, not the
    # container TCP socket. Pulsar probe auto-restarts on frozen JVM.
    _ensure_etcd_healthy()
    _ensure_minio_healthy()
    _ensure_pulsar_healthy()
    # Wipe etcd prefixes for all clusters. etcdctl failure is non-fatal (fresh
    # env may have no keys yet), but we surface stderr so network / container
    # naming issues don't silently break the reset.
    for prefix in ("by-dev1/", "by-dev2/", "by-dev3/"):
        res = subprocess.run(
            f"docker exec milvus_control-etcd-1 etcdctl del --prefix {prefix}",
            shell=True, check=False, capture_output=True, text=True,
        )
        if res.returncode != 0:
            logger.warning(
                f"etcdctl wipe prefix={prefix} rc={res.returncode} "
                f"stderr={res.stderr.strip()!r} stdout={res.stdout.strip()!r}"
            )
    # Start fresh via start_clusters.sh (supports MILVUS_DEV_PATH override).
    # On failure, surface captured stdout/stderr — otherwise CalledProcessError
    # hides them and the next health probe fails with no clue.
    start_script = os.path.join(os.path.dirname(__file__), "start_clusters.sh")
    try:
        subprocess.run(
            f"bash {start_script}",
            shell=True, check=True, capture_output=True, text=True,
        )
    except subprocess.CalledProcessError as e:
        logger.error(
            f"start_clusters.sh failed rc={e.returncode}\n"
            f"--- stdout ---\n{e.stdout}\n--- stderr ---\n{e.stderr}"
        )
        raise
    # Wait for health (A, B, C all three clusters)
    import time
    import urllib.request
    for _ in range(120):
        try:
            a = urllib.request.urlopen("http://localhost:19101/healthz", timeout=2).read().decode().strip()
            b = urllib.request.urlopen("http://localhost:19201/healthz", timeout=2).read().decode().strip()
            c = urllib.request.urlopen("http://localhost:19301/healthz", timeout=2).read().decode().strip()
            if a == "OK" and b == "OK" and c == "OK":
                break
        except Exception:
            pass
        time.sleep(2)
    else:
        raise RuntimeError("Clusters did not all become healthy after reset")
    reconnect_clients()
    # proxy /healthz OK is not enough — streamingnode registration + balancer
    # channel assignment happen later. Probe each cluster with a create+drop
    # DDL to confirm broadcast-capable pchannels are fully assigned.
    _wait_cluster_fully_ready(_clients_mod.cluster_A_client, "A")
    _wait_cluster_fully_ready(_clients_mod.cluster_B_client, "B")
    if _clients_mod.cluster_C_client is not None:
        _wait_cluster_fully_ready(_clients_mod.cluster_C_client, "C")
    logger.info("=== Reset complete ===")


# ---------------------------------------------------------------------------
# M1: P + C+ — A standalone (N) → restart to N+1 → add B (standalone → A→B)
# ---------------------------------------------------------------------------
def test_m1_pchannel_increase_with_add_cluster():
    """
    Precondition: fresh clusters, no replicate config.
    Steps:
      1. Establish A standalone config with N pchannels (baseline, makes
         currentConfig non-nil so validator will see isPChannelIncreasing).
      2. Restart with N+1 pchannels.
      3. Update to A→B with N+1 pchannels.
         This triggers validator with isPChannelIncreasing=true AND new cluster
         B added — the combination whose constraint was relaxed for
         issue #48993.
      4. Verify replication works.
    """
    logger.info("=" * 60)
    logger.info("M1: P + C+ — pchannel increase with cluster addition")
    logger.info("=" * 60)

    pchannel_num = INIT_PCHANNEL_NUM

    try:
        # Step 1: Baseline — A standalone with 16 pchannels
        logger.info(f"[M1] Step 1: A standalone with {pchannel_num} pchannels")
        _set_a_standalone(pchannel_num)

        # Step 2: Restart to N+1 pchannels
        pchannel_num += 1
        logger.info(f"[M1] Step 2: Restart with pchannels={pchannel_num}")
        restart_both_clusters(dml_channel_num=pchannel_num, cluster_b_replica_number=CLUSTER_B_REPLICA)
        reconnect_clients()

        # Step 3: P + C+ — increase pchannels AND add B as secondary.
        # This triggers the validator with isPChannelIncreasing=true AND a new
        # cluster B added — the combination whose constraint was relaxed for
        # issue #48993.
        logger.info(f"[M1] Step 3: Add B (P+C+, pchannels {INIT_PCHANNEL_NUM}→{pchannel_num})")
        update_replicate_config(CLUSTER_A_ID, CLUSTER_B_ID, pchannel_num)

        # Step 4: Verify replication
        name = "m1_after_addb"
        _verify_replication(name, CLUSTER_A_ID)

        logger.info("[M1] PASSED")

    except Exception as e:
        logger.error(f"[M1] FAILED: {e}")
        logger.error(traceback.format_exc())
        raise


# ---------------------------------------------------------------------------
# M2: P + C- — A→B (N) → restart to N+1 → remove B (→ A standalone N+1)
# ---------------------------------------------------------------------------
def test_m2_pchannel_increase_with_remove_cluster():
    """
    Precondition: fresh clusters.
    Steps:
      1. Establish A→B with N pchannels, verify baseline replication.
      2. Restart with N+1 pchannels.
      3. Remove B: both A and B get standalone configs (P+C-).
         The validator must accept the P+C- update (issue #48993 core —
         previously rejected because pchannels were growing while the
         cluster set shrunk).
    """
    logger.info("=" * 60)
    logger.info("M2: P + C- — pchannel increase with cluster removal")
    logger.info("=" * 60)

    pchannel_num = INIT_PCHANNEL_NUM

    try:
        # Step 1: Baseline — A→B with 16 pchannels
        logger.info(f"[M2] Step 1: A→B with {pchannel_num} pchannels")
        update_replicate_config(CLUSTER_A_ID, CLUSTER_B_ID, pchannel_num)
        _verify_replication("m2_before_remove", CLUSTER_A_ID)

        # Step 2: Restart to N+1
        pchannel_num += 1
        logger.info(f"[M2] Step 2: Restart with pchannels={pchannel_num}")
        restart_both_clusters(dml_channel_num=pchannel_num, cluster_b_replica_number=CLUSTER_B_REPLICA)
        reconnect_clients()

        # Step 3: P + C- — dismantle A→B symmetrically.
        # This is the exact combination the relaxed validator (issue #48993)
        # must accept: isPChannelIncreasing=true AND cluster set shrinks.
        # A gets {[A]}, B gets {[B]} in parallel — see _dismantle_to_standalone.
        logger.info(f"[M2] Step 3: Remove B (P+C-, pchannels {INIT_PCHANNEL_NUM}→{pchannel_num})")
        _dismantle_to_standalone(pchannel_num)

        logger.info("[M2] PASSED")

    except Exception as e:
        logger.error(f"[M2] FAILED: {e}")
        logger.error(traceback.format_exc())
        raise


# ---------------------------------------------------------------------------
# M3: P + C+ + switchover — M1 plus immediate switch after
# ---------------------------------------------------------------------------
def test_m3_pchannel_increase_add_cluster_then_switchover():
    """
    Precondition: fresh clusters, no replicate config.
    Steps:
      1-3. Same as M1 (A standalone → restart → A→B with N+1 pchannels).
      4.   Immediate switchover to B→A.
      5.   Verify replication in reversed direction.
    """
    logger.info("=" * 60)
    logger.info("M3: P + C+ + switchover")
    logger.info("=" * 60)

    pchannel_num = INIT_PCHANNEL_NUM

    try:
        # Step 1: Baseline — A standalone
        logger.info(f"[M3] Step 1: A standalone with {pchannel_num} pchannels")
        _set_a_standalone(pchannel_num)

        # Step 2: Restart to N+1
        pchannel_num += 1
        logger.info(f"[M3] Step 2: Restart with pchannels={pchannel_num}")
        restart_both_clusters(dml_channel_num=pchannel_num, cluster_b_replica_number=CLUSTER_B_REPLICA)
        reconnect_clients()

        # Step 3: P + C+ — A→B
        logger.info(f"[M3] Step 3: Add B (P+C+), pchannels={pchannel_num}")
        update_replicate_config(CLUSTER_A_ID, CLUSTER_B_ID, pchannel_num)

        name_a_primary = "m3_a_primary"
        _verify_replication(name_a_primary, CLUSTER_A_ID)

        # Step 4: Switchover B→A
        logger.info("[M3] Step 4: Switchover B→A")
        update_replicate_config(CLUSTER_B_ID, CLUSTER_A_ID, pchannel_num)

        name_b_primary = "m3_b_primary"
        _verify_replication(name_b_primary, CLUSTER_B_ID)

        logger.info("[M3] PASSED")

    except Exception as e:
        logger.error(f"[M3] FAILED: {e}")
        logger.error(traceback.format_exc())
        raise


# ---------------------------------------------------------------------------
# M4: Production scenario #48993 — remove B, scale pchannels, re-add B
# ---------------------------------------------------------------------------
def test_m4_remove_scale_add_fresh_cluster():
    """
    Reproduces the production scenario from issue #48993.

    Uses a fresh cluster C as the new secondary (NOT re-adding the polluted B)
    — this matches the real production flow where the old secondary is gone
    and a brand-new cluster is added after scaling.

    Precondition: fresh clusters A, B, C.
    Steps:
      1. Establish A→B with N pchannels (baseline replication).
      2. Update A to standalone (seeds the stale single-cluster config).
      3. Restart A+B with N+2 pchannels WITHOUT updating replicate config
         (mirrors production scaling that bypasses update_replicate_config).
      4. Add fresh C as new secondary: A→C with N+2 pchannels
         (P+C+ with stale pchannel count in A's etcd).
      5. Verify A→C replication end-to-end.
    """
    logger.info("=" * 60)
    logger.info("M4: #48993 — stale config + scale pchannels + add fresh C")
    logger.info("=" * 60)

    if _clients_mod.cluster_C_client is None:
        raise RuntimeError("M4 requires cluster C to be running (check start_clusters.sh)")

    pchannel_num = INIT_PCHANNEL_NUM

    try:
        # Step 1: Baseline — A→B
        logger.info(f"[M4] Step 1: A→B with {pchannel_num} pchannels")
        update_replicate_config(CLUSTER_A_ID, CLUSTER_B_ID, pchannel_num)
        _verify_replication("m4_initial", CLUSTER_A_ID)

        # Step 2: Dismantle A→B symmetrically. Seeds the "A post-removal with
        # stale single-cluster config" state that Step 3 will then scale past.
        logger.info(f"[M4] Step 2: Dismantle A→B (both standalone, pchannels={pchannel_num})")
        _dismantle_to_standalone(pchannel_num)

        # Step 3: Scale A+B+C pchannels via restart (bypass update_replicate_config)
        pchannel_num += 2
        logger.info(f"[M4] Step 3: Scale A+B+C to {pchannel_num} pchannels (etcd config on A stays at {INIT_PCHANNEL_NUM})")
        restart_all_three_clusters(dml_channel_num=pchannel_num, cluster_b_replica_number=CLUSTER_B_REPLICA)
        reconnect_clients()

        # Step 4: Add fresh C as new secondary — P+C+ with stale pchannel count
        logger.info(f"[M4] Step 4: Add fresh C as new secondary (P+C+ with stale {INIT_PCHANNEL_NUM}→{pchannel_num} pchannel jump)")
        cfg = build_replicate_config_star(CLUSTER_A_ID, [CLUSTER_C_ID], pchannel_num=pchannel_num)
        cfg_with_timeout = {**cfg, "timeout": GRPC_TIMEOUT_SEC}
        update_replicate_config_on_clients(
            [_clients_mod.cluster_A_client, _clients_mod.cluster_C_client],
            cfg_with_timeout,
        )
        logger.info(f"Replicate config applied: {CLUSTER_A_ID} -> {CLUSTER_C_ID}, pchannels={pchannel_num}")

        # Step 5: Verify A→C replication end-to-end
        primary = _clients_mod.cluster_A_client
        standby = _clients_mod.cluster_C_client
        name = "m4_a_to_c"
        setup_collection(name, primary, standby, shard_num=INIT_PCHANNEL_NUM)
        insert_and_verify(name, primary, standby)
        logger.info(f"Replication A→C verified: {name}")

        logger.info("[M4] PASSED")

    except Exception as e:
        logger.error(f"[M4] FAILED: {e}")
        logger.error(traceback.format_exc())
        raise


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------
ALL_CASES = {
    "M1": test_m1_pchannel_increase_with_add_cluster,
    "M2": test_m2_pchannel_increase_with_remove_cluster,
    "M3": test_m3_pchannel_increase_add_cluster_then_switchover,
    "M4": test_m4_remove_scale_add_fresh_cluster,
}


def main():
    parser = argparse.ArgumentParser(description="PChannel + Cluster state matrix tests")
    parser.add_argument("--case", choices=list(ALL_CASES.keys()), default=None,
                        help="Run a specific test case (default: all)")
    parser.add_argument("--timeout", type=int, default=CASE_TIMEOUT_SEC,
                        help=f"per-case timeout in seconds (default: {CASE_TIMEOUT_SEC})")
    args = parser.parse_args()

    cases = {args.case: ALL_CASES[args.case]} if args.case else ALL_CASES
    passed, failed = [], []

    for name, func in cases.items():
        # Always reset before each case — guarantees fresh state (pchannel=INIT,
        # no replicate config) regardless of prior test state or session history.
        try:
            _reset_to_initial()
        except Exception as e:
            logger.error(f"Reset before {name} failed: {e}")
            failed.append(name)
            continue
        try:
            with _Deadline(args.timeout):
                func()
            passed.append(name)
        except CaseTimeout as e:
            logger.error(f"[{name}] TIMEOUT: {e}")
            failed.append(name)
            continue
        except Exception:
            failed.append(name)

    logger.info("=" * 60)
    logger.info(f"Matrix results: {len(passed)} passed, {len(failed)} failed")
    if passed:
        logger.info(f"  PASSED: {', '.join(passed)}")
    if failed:
        logger.error(f"  FAILED: {', '.join(failed)}")
    logger.info("=" * 60)

    if failed:
        raise RuntimeError(f"Failed cases: {', '.join(failed)}")
    logger.info("PASSED: all state matrix tests")


if __name__ == "__main__":
    main()
