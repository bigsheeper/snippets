# CDC mTLS E2E Tests

3 集群 mTLS 环境下的 CDC 跨集群复制测试。

## 架构

3 个 Milvus 集群（A/B/C），均启用 `tlsMode=2`（mutual TLS），每个集群独立 CA：

| Cluster | ID | Proxy | CA |
|---------|----|-------|----|
| A | by-dev1 | https://localhost:19530 | ca-dev1.pem |
| B | by-dev2 | https://localhost:19531 | ca-dev2.pem |
| C | by-dev3 | https://localhost:19532 | ca-dev3.pem |

CDC 通过 `tls.clusters.<clusterID>.*` 读取每集群的客户端证书，replicate config 只传 `uri` + `token`。

## 测试用例

| 脚本 | 场景 |
|------|------|
| `test_mtls_replication.py` | A→B, A→C 复制 + 证书校验 |
| `test_mtls_switchover.py` | B 升主（B→A, B→C） |
| `test_mtls_failure.py` | TLS 负面测试（错误证书、无证书、跨集群CA） |

## 运行

```bash
# 1. 启动 3 个 mTLS 集群
./start_clusters.sh

# 2. 运行测试
conda activate milvus2
python test_mtls_replication.py
python test_mtls_switchover.py
python test_mtls_failure.py

# 3. 停止
./stop_clusters.sh
```

## 依赖

- 共享库：`common/`（schema、wait helpers、config 构建）
- 本目录 `common.py`：mTLS 客户端创建、证书路径、3集群 config 构建
- 证书：`certs/` 目录（`gen_certs.sh` 生成）
