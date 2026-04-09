# CDC Replica Features 压测

switchover + 副本异构 + pchannel 动态扩容的压力测试。

## 架构

两个 Milvus 集群（cluster+streaming 模式）：

| Cluster | ID | Proxy | 特殊配置 |
|---------|----|-------|----------|
| A | by-dev1 | 19530 | 默认 |
| B | by-dev2 | 19531 | `QUERYCOORD_CLUSTERLEVELLOADREPLICANUMBER=2` |

## 测试用例

| 脚本 | 场景 |
|------|------|
| `test_stress.py` | 每10轮 switchover + 每轮变化 replica 数 + 数据一致性校验 |
| `test_pchannel_increase.py` | Phase 1: vchannel 分配边界测试; Phase 2: 10轮 pchannel 递增 + 随机 switchover |

## 运行

```bash
conda activate milvus2
cd test_replica_features

# 压测（1000轮或1小时）
python test_stress.py --cycles 1000 --duration 3600

# 快速验证（30秒）
python test_stress.py --cycles 1000 --duration 30

# pchannel 扩容测试
python test_pchannel_increase.py
```

## 依赖

- 共享库：`common/`（客户端、schema、wait helpers、config、集群控制）
- 本目录 `common.py`：replica 特性专用工具（`get_replica_count()`、`update_replicate_config()`）
- 集群控制：`common/cluster_control.py`（通过 `milvus_control` 管理进程）
