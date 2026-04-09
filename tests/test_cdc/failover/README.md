# CDC Failover 测试说明

本目录包含 Milvus CDC 故障切换（failover）相关的端到端测试用例：
- Force promote 过程中重启 B（standby）/ A（primary）
- 不完整的 broadcast DDL（create/drop index、release/drop collection）
- Force promote 期间的 FastLock 资源争用

## 前置条件
- Python 环境：`conda activate milvus2`（Python 3.10+）
- 已编译 Milvus 可执行文件：设置 `MILVUS_DEV_PATH` 指向 Milvus 源码根目录（包含 `bin/milvus`）
- 可写数据目录：设置 `MILVUS_VOLUME_DIRECTORY` 用于数据/日志存放（同时用于第三方服务的 `MILVUS_INF_VOLUME_DIRECTORY`）
- Docker + docker compose（用于启动 etcd/minio/pulsar，复用 `~/workspace/snippets/milvus_control/docker-compose-pulsar.yml`）
- 端口与地址（代码中已内置）：
  - Proxy gRPC：A `tcp://localhost:19530`，B external `tcp://localhost:19531`，B internal `tcp://localhost:19528`
  - Proxy healthz：A `http://127.0.0.1:19101/healthz`，B `http://127.0.0.1:19201/healthz`
  - 认证：`TOKEN = "root:Milvus"`

## 目录结构
- `test_restart_b_during_force_promote.py` — 提升过程中重启 B
- `test_restart_a_during_force_promote.py` — 提升过程中重启 A  
- `test_incomplete_broadcast_ddl.py` — 固定 DDL 场景：create→index→load→release→promote→create new→load→insert→verify
- `test_fastlock_contention.py` — 提升期间的 FastLock 争用
- `utils.py` — 复制拓扑配置、健康等待、异步工具等

## 快速开始
1. 启动本地 A/B 集群（非 TLS，与 cluster+streaming 端口布局一致）。在本目录执行：
   ```bash
   export MILVUS_DEV_PATH=~/workspace/milvus
   export MILVUS_VOLUME_DIRECTORY=~/milvus-vol
   # 会同时启动 etcd/minio/pulsar（复用 ~/workspace/snippets/milvus_control/docker-compose-pulsar.yml）
   bash start_clusters.sh
   # 等待约 20~30 秒后健康检查：
   curl -s http://127.0.0.1:19101/healthz; echo
   curl -s http://127.0.0.1:19201/healthz; echo
   ```
2. 激活 Python 环境
   ```bash
   conda activate milvus2
   ```
3. 运行用例（直接 python 执行，建议在本目录运行）：
   ```bash
   # 重启 B 期间强制提升
   FAILOVER_TIMEOUT=180 python test_restart_b_during_force_promote.py
   # 重启 A 期间强制提升
   FAILOVER_TIMEOUT=180 python test_restart_a_during_force_promote.py
   # 固定 DDL 场景（create→index→load→release→promote→create new→load→insert→verify）
   FAILOVER_TIMEOUT=180 python test_incomplete_broadcast_ddl.py
   # FastLock 争用
   FAILOVER_TIMEOUT=180 python test_fastlock_contention.py
   ```
   - `FAILOVER_TIMEOUT`：单个异步等待/提升操作的超时（秒）
4. 结束后停止集群（在本目录执行）
   ```bash
   # 会同时停止 etcd/minio/pulsar
   bash stop_clusters.sh
   ```

## 只跑单个用例（直接 python）
- 跑固定 DDL 用例：
  ```bash
  FAILOVER_TIMEOUT=120 \
  python test_incomplete_broadcast_ddl.py
  ```
- 只跑“提升过程中重启 B”：
  ```bash
  FAILOVER_TIMEOUT=180 python test_restart_b_during_force_promote.py
  ```
- 只跑“提升过程中重启 A”：
  ```bash
  FAILOVER_TIMEOUT=180 python test_restart_a_during_force_promote.py
  ```
- 只跑 FastLock 争用：
  ```bash
  FAILOVER_TIMEOUT=180 python test_fastlock_contention.py
  ```

## 运行时行为与健康检查
- 测试会通过 `failover/utils.py` 调用复制拓扑 API，设置 A→B 复制，或对 B 执行 `force_promote`。
- 集群启停与健康检查由 `start_clusters.sh` / `stop_clusters.sh` 与 `test_replica_features/cluster_control.py` 协同完成。
- 健康探针：
  - `http://127.0.0.1:19101/healthz`（A），`http://127.0.0.1:19201/healthz`（B）
  - 返回 `OK` 视为就绪。

### 端口占用排查
- A/B 外部与内部 gRPC 端口均为固定配置：A external `19530`，B external `19531`，B internal `19528`。
- 若启动失败，排查占用进程：
  ```bash
  ss -tlnp | grep -E ':19528|:19530|:19531|:1910[0-9]|:1920[0-9]|:19150|:19250'
  ```

## 常见问题排查
- 长时间无输出：
  - 检查 metrics 端口监听与健康：
    ```bash
    ss -tln | grep -E ':1910[0-9]|:1920[0-9]|:19150|:19250|:19528|:19530|:19531'
    curl -s http://127.0.0.1:19101/healthz; echo
    curl -s http://127.0.0.1:19201/healthz; echo
    ```
  - 若不健康，使用 `milvus_control` 先停止再启动：
    ```bash
    ~/workspace/snippets/milvus_control/milvus_control -m -c -s -f --primary stop_milvus
    ~/workspace/snippets/milvus_control/milvus_control -m -c -s -f --standby stop_milvus
    ~/workspace/snippets/milvus_control/milvus_control -m -c -s -u --primary start_milvus
    ~/workspace/snippets/milvus_control/milvus_control -m -c -s -u --standby start_milvus
    ```
- 连接报 `UNIMPLEMENTED`：确认使用的是 gRPC 地址（以 `tcp://` 开头，A 为 `19530`，B external 为 `19531`，测试内部端口为 `19528`）。
- 用例超时：降低 `FAILOVER_ROWS`（如 100 或更小），提高 `FAILOVER_TIMEOUT`（如 240），或先只跑单个用例定位。

## 建议
- 初次验证建议先跑 `test_incomplete_broadcast_ddl.py`，这个固定 DDL 场景更适合快速确认 failover 基本链路。
- 在修改复制拓扑或重启节点后，等待健康探针返回 `OK` 再继续下一步，以降低不稳定性。

---
如需扩展更多场景或集成 CI，请在本目录补充相应用例并在此 README 增加运行说明。
