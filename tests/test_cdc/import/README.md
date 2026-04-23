# Import 2PC × Replication E2E 测试

跨集群复制下的 Import 2PC（`auto_commit=false`）端到端测试。

基于：
- PR [#48472](https://github.com/milvus-io/milvus/pull/48472)：`commit_timestamp` on SegmentInfo
- PR [#48524](https://github.com/milvus-io/milvus/pull/48524)：CommitImport/AbortImport + WAL broadcast
- Issue [#48525](https://github.com/milvus-io/milvus/issues/48525)：Import in replication clusters via 2PC

## 测试用例

| 脚本 | 场景 | 耗时 |
|------|------|------|
| `test_import_replication_basic.py` | 2PC happy path：Uncommitted 时 B=0，commit 后 A=B=N | < 1m |
| `test_import_replication_delete.py` | 前/后 commit 的 delete 语义在 A/B 两侧一致 | < 1m |
| `test_import_replication_insert_delete.py` | 3-phase interleaved（import 偶数 + SDK insert 奇数 + 每 phase B 侧校验），fresh ↔ accumulate 轮替 | 可配置 |
| `test_import_replication_stability.py` | import + delete 长时间轮替压测（无 SDK 插入交织） | 可配置 |

## 前置条件

- 源码为带 Import 2PC + 复制集群支持的 Milvus worktree 编译出的二进制
- `MILVUS_DEV_PATH` / `MILVUS_VOLUME_DIRECTORY` 已设置
- `conda activate milvus2`（需要 `pymilvus` / `pyarrow` / `boto3` / `numpy` / `requests` / `loguru`）
- A + B 两个集群已启动（cluster+streaming 模式）

## 快速开始

```bash
# 1. 启动 A + B 集群（与 testcases/、failover/ 共用脚本，已 symlink）
bash start_clusters.sh

# 2. 运行测试
conda activate milvus2

python test_import_replication_basic.py
python test_import_replication_delete.py

# 压测默认 30 分钟；烟雾测试缩短
STABILITY_DURATION_MINUTES=2 python test_import_replication_insert_delete.py
STABILITY_DURATION_MINUTES=2 python test_import_replication_stability.py

# 3. 停止
bash stop_clusters.sh
```

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `CLUSTER_A_ADDR` | `tcp://localhost:19530` | A gRPC |
| `CLUSTER_B_ADDR` | `tcp://localhost:19531` | B gRPC |
| `CLUSTER_A_REST_URI` | `http://localhost:18530` | A REST（import 2PC 只对 A 发起；与 `start_clusters.sh` 的 `PROXY_HTTP_PORT=18530` 对应） |
| `CLUSTER_B_REST_URI` | `http://localhost:18531` | B REST（目前未用） |
| `MILVUS_TOKEN` | `root:Milvus` | |
| `CDC_TIMEOUT` | `180` | 等待 B 侧收敛的全局超时（秒） |
| `STABILITY_DURATION_MINUTES` | `30` | 压测总时长（`insert_delete` / `stability`） |
| `MINIO_ENDPOINT` / `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` / `MINIO_BUCKET` / `MILVUS_ROOT_PATH` | minio 默认值 | parquet 上传配置 |

## 验证的核心不变量

1. Uncommitted import 数据绝不出现在 B 上。
2. commit 后 A/B 一致（相同 PK 集合）。
3. commit 之前的 delete（`delete_ts < commit_ts`）在 A/B 两侧都是 no-op（MVCC）。
4. commit 之后的 delete（`delete_ts > commit_ts`）在 A/B 两侧都生效。
5. SDK 写入和 2PC import 在同一 vchannel 交织时复制不出现偏差。
6. 30min 持续压测后 A/B 两侧仍保持一致。

## 本目录 vs 上游

- `test_import_commit_ts/`：单集群 Import 2PC 覆盖（auto_commit true/false、MVCC、delete、TTL 等）
- `test_cdc/import/`（本目录）：仅覆盖 `auto_commit=false` 在 A→B 复制下的行为；复用 `test_cdc/common/` 的集群客户端 / wait 辅助

## 结构说明

- `_path_setup.py`：把上级 `test_cdc/` 加到 `sys.path`，使 `from common import ...` 命中 `test_cdc/common/` 包。
- `common.py`：本目录独有的 REST 2PC + parquet + `insert_rows` 辅助。
- 每个测试脚本都用 `importlib.util` 以 `import_common` 的别名侧载本地 `common.py`，避免与 `test_cdc/common/` 包命名冲突。
- `start_clusters.sh` / `stop_clusters.sh`：指向 `../failover/` 的同名脚本（和 `testcases/` 走同一份权威脚本）。
