# CDC Replica Features 测试

switchover + 副本异构 + pchannel 动态扩容 + cluster/topology 变更矩阵的回归/压测套件。

## 架构

三个 Milvus 集群（cluster+streaming 模式，每个 11 进程：mixcoord + proxy + 3×sn + 3×qn + data/index/cdc）：

| Cluster | ID | External gRPC | Metrics | 特殊配置 |
|---------|-------|-------|---------|----------|
| A | by-dev1 | 19530 | 19101 | 默认 |
| B | by-dev2 | 19531 | 19201 | `QUERYCOORD_CLUSTERLEVELLOADREPLICANUMBER=2` |
| C | by-dev3 | 19532 | 19301 | M4 矩阵用例专用 |

三方依赖（etcd / minio / pulsar）通过 `~/workspace/snippets/milvus_control/docker-compose-pulsar.yml` 统一起。

## 前置环境变量

```bash
export MILVUS_DEV_PATH=~/workspace/milvus          # 含 bin/milvus 的源码目录
export MILVUS_VOLUME_DIRECTORY=~/milvus-vol        # 数据/日志根目录
```

## 启停

```bash
bash start_clusters.sh   # 启三方 + A/B/C（默认 dmlChannelNum=16）
bash stop_clusters.sh    # 停所有 milvus 进程 + 三方
bash start_cluster_c.sh  # 仅启 C（M4 的独立扩容阶段用）
```

## 测试用例

| 脚本 | 场景 | 依赖集群 |
|------|------|---------|
| `test_stress.py` | 每 10 轮 switchover + 每轮变化 replica 数 + 数据一致性校验 | A, B |
| `test_pchannel_increase.py` | Phase 1 vchannel 分配边界；Phase 2 pchannel 递增 + 随机 switchover（10 轮） | A, B |
| `test_pchannel_cluster_matrix.py` | M1–M4 状态矩阵：pchannel 扩容 × cluster set / topology 变化（覆盖 issue #48993） | A, B, C |

## 运行

```bash
conda activate milvus2
cd test_replica_features

# 压测（上限 1000 轮或 1 小时，先到者停）
python test_stress.py --cycles 1000 --duration 3600

# 快速冒烟
python test_stress.py --cycles 1000 --duration 30

# pchannel 递增（可复现）
python test_pchannel_increase.py --seed 42

# 状态矩阵（全量 / 单 case）
python test_pchannel_cluster_matrix.py
python test_pchannel_cluster_matrix.py --case M4
```

## 依赖布局

- 共享库 `../common/`：客户端、schema、wait helpers、config、集群控制
- 本目录 `common.py`：replica 特性专用工具（`get_replica_count()`、`update_replicate_config()`）
- 集群控制：`../common/cluster_control.py`（底层通过 `milvus_control` 脚本管理进程）
