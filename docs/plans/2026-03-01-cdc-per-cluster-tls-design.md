# CDC Per-Cluster TLS Config Design

**Date:** 2026-03-01
**Author:** bigsheeper
**Status:** Draft
**Issue:** #47843

## Background

CDC replicates data between Milvus clusters. When clusters use mTLS (tlsMode=2),
CDC outbound connections need client certificates to authenticate with target clusters.

User feedback (Amay): clusters have **dedicated certs per cluster** — different CAs
and client certs for each target. A single global TLS config is insufficient.

Additionally, CDC topology supports runtime switchover (e.g., A→B,C switches to
B→A,C) via the replicate API without restarting any processes. The TLS config must
support this — all possible target certs must be pre-loaded.

## Goal

- Support per-cluster outbound TLS configuration for CDC connections
- Enable switchover without process restart
- Keep milvus.yaml as the single source of truth for TLS config

## Non-Goal

- Per-cluster TLS fields in the proto `ConnectionParam` (cert paths stay in config)
- Global/default client cert fallback (each cluster must be explicitly configured)

## Design

### Config Structure

Add `tls.clusters` map in `milvus.yaml`. Each entry is keyed by `cluster_id`
(matching the ID used in the replicate configuration API):

```yaml
tls:
  # Existing server-side TLS (unchanged)
  caPemPath: configs/cert/ca.pem
  serverPemPath: configs/cert/server.pem
  serverKeyPath: configs/cert/server.key

  # Per-cluster outbound TLS for CDC
  clusters:
    cluster-a:
      caPemPath: /certs/cluster-a/ca.pem
      clientPemPath: /certs/cluster-a/client.pem
      clientKeyPath: /certs/cluster-a/client.key
    cluster-b:
      caPemPath: /certs/cluster-b/ca.pem
      clientPemPath: /certs/cluster-b/client.pem
      clientKeyPath: /certs/cluster-b/client.key
    cluster-c:
      caPemPath: /certs/cluster-c/ca.pem
      clientPemPath: /certs/cluster-c/client.pem
      clientKeyPath: /certs/cluster-c/client.key
```

**Key point:** All nodes in every cluster have the SAME `tls.clusters` section
(containing certs for ALL clusters). Only the top-level server TLS
(`caPemPath`, `serverPemPath`, `serverKeyPath`) differs per cluster.

### Switchover Without Restart

1. Topology A→B,C is running. A's CDC reads `tls.clusters.cluster-b` and
   `tls.clusters.cluster-c` from paramtable to build outbound TLS.
2. User calls `update_replicate_configuration` to switch topology to B→A,C.
3. B's CDC starts replicating. It looks up `tls.clusters.cluster-a` and
   `tls.clusters.cluster-c` — already in paramtable, no restart needed.

### Code Changes

#### 1. `pkg/util/paramtable/grpc_param.go`

Add a method to dynamically look up per-cluster TLS config:

```go
// GetClusterTLSConfig returns TLS cert paths for a specific cluster ID.
// Returns empty strings if the cluster has no TLS config.
func (p *grpcConfig) GetClusterTLSConfig(clusterID string) (caPemPath, clientPemPath, clientKeyPath string) {
    prefix := "tls.clusters." + clusterID + "."
    caPemPath = p.base.mgr.GetConfig(prefix + "caPemPath")
    clientPemPath = p.base.mgr.GetConfig(prefix + "clientPemPath")
    clientKeyPath = p.base.mgr.GetConfig(prefix + "clientKeyPath")
    return
}
```

No static `ParamItem` fields needed — the cluster IDs are dynamic.

#### 2. `internal/cdc/cluster/milvus_client.go`

Update `NewMilvusClient` to read per-cluster TLS config:

```go
func NewMilvusClient(ctx context.Context, cluster *commonpb.MilvusCluster) (MilvusClient, error) {
    connParam := cluster.GetConnectionParam()
    config := &milvusclient.ClientConfig{
        Address: connParam.GetUri(),
        APIKey:  connParam.GetToken(),
    }

    // Build TLS config from per-cluster paramtable config.
    tlsConfig, err := buildCDCTLSConfig(cluster.GetClusterId())
    if err != nil {
        return nil, errors.Wrap(err, "failed to build CDC TLS config")
    }
    if tlsConfig != nil {
        config.TLSConfig = tlsConfig
    }

    cli, err := milvusclient.New(ctx, config)
    if err != nil {
        return nil, errors.Wrap(err, "failed to create milvus client")
    }
    return cli, nil
}

func buildCDCTLSConfig(clusterID string) (*tls.Config, error) {
    caPem, clientPem, clientKey := paramtable.Get().ProxyGrpcServerCfg.GetClusterTLSConfig(clusterID)
    if clientPem == "" || clientKey == "" {
        return nil, nil  // No TLS for this cluster
    }
    log.Info("CDC outbound TLS enabled",
        zap.String("targetCluster", clusterID),
        zap.String("caPemPath", caPem),
        zap.String("clientPemPath", clientPem),
        zap.String("clientKeyPath", clientKey))
    return milvusclient.BuildTLSConfig(caPem, clientPem, clientKey)
}
```

#### 3. `client/milvusclient/tls.go` (from PR #47933, unchanged)

`BuildTLSConfig(caPemPath, clientPemPath, clientKeyPath)` helper stays the same.

#### 4. `client/milvusclient/client_config.go` + `client.go` (from PR #47933, unchanged)

`TLSConfig *tls.Config` field on `ClientConfig` and the `dialOptions()` priority
(`TLSConfig` > `EnableTLSAuth` > insecure) stay the same.

#### 5. `configs/milvus.yaml`

Add commented example for `tls.clusters`:

```yaml
tls:
  serverPemPath: configs/cert/server.pem
  serverKeyPath: configs/cert/server.key
  caPemPath: configs/cert/ca.pem
  # Per-cluster outbound TLS for CDC replication.
  # Key is the cluster_id used in update_replicate_configuration.
  # clusters:
  #   target-cluster-1:
  #     caPemPath: /path/to/target-1/ca.pem
  #     clientPemPath: /path/to/target-1/client.pem
  #     clientKeyPath: /path/to/target-1/client.key
```

### Files Changed (vs PR #47933)

| File | Change |
|------|--------|
| `pkg/util/paramtable/grpc_param.go` | Add `GetClusterTLSConfig()` method (replaces static `ClientPemPath`/`ClientKeyPath` ParamItems) |
| `pkg/util/paramtable/grpc_param_test.go` | Test dynamic cluster TLS lookup |
| `internal/cdc/cluster/milvus_client.go` | `buildCDCTLSConfig` takes `clusterID`, reads per-cluster config |
| `internal/cdc/cluster/milvus_client_test.go` | Test per-cluster TLS lookup + fallback |
| `client/milvusclient/tls.go` | Same as PR #47933 |
| `client/milvusclient/tls_test.go` | Same as PR #47933 |
| `client/milvusclient/client_config.go` | Same as PR #47933 (`TLSConfig` field) |
| `client/milvusclient/client.go` | Same as PR #47933 (`dialOptions` priority) |
| `configs/milvus.yaml` | Add commented `tls.clusters` example (no `clientPemPath`/`clientKeyPath` at top level) |

### No Proto Changes

`ConnectionParam` stays as `uri` + `token`. PyMilvus TLS fields
(`ca_pem_path`, etc. in `prepare.py`) are unused — they set non-existent proto
fields which are silently dropped. This is fine; we can clean them up later.

## Test Strategy

### Unit Tests

1. `grpc_param_test.go`: Set `tls.clusters.test-cluster.{caPemPath,clientPemPath,clientKeyPath}`,
   verify `GetClusterTLSConfig("test-cluster")` returns correct values.
   Verify unknown cluster returns empty strings.

2. `milvus_client_test.go`: Test `buildCDCTLSConfig` with:
   - Cluster with full TLS config → returns valid `*tls.Config`
   - Cluster with no config → returns `nil`
   - Cluster with partial config (only clientPemPath) → returns `nil`
   - Cluster with invalid cert paths → returns error

3. `tls_test.go`: Same as PR #47933 (BuildTLSConfig helper tests).

### E2E Tests

Use the existing mTLS test infrastructure (`snippets/tests/test_cdc/test_mtls/`):
- Configure `tls.clusters` in start_clusters.sh via env vars
- Run existing replication + switchover tests
- Run mTLS failure tests (test_mtls_failure.py)

## Milestones

1. Update PR #47933 with per-cluster design (replace global with per-cluster)
2. Unit tests pass
3. E2E validation with 3-cluster mTLS setup
