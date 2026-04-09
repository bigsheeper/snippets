# CDC Replication E2E Tests

Milvus CDC（Change Data Capture）跨集群复制的端到端测试套件。

## 目录结构

```
test_cdc/
├── common/                      # 共享库（所有测试目录引用）
│   ├── constants.py             # 常量：端口、集群ID、超时、schema字段
│   ├── clients.py               # MilvusClient 初始化、重连
│   ├── schema.py                # schema / index / 数据生成
│   ├── wait.py                  # 通用 wait_until() + 所有等待函数 + 高级 helpers
│   ├── config.py                # replicate config 构建（2集群/star/standalone）
│   ├── cluster_control.py       # 集群生命周期（stop/start/restart/health check）
│   ├── async_helpers.py         # 线程工具
│   └── port_layout.py           # 健康检查端点
│
├── testcases/                   # 基础复制功能测试
│   ├── test_quick_start.py      # 冒烟测试：初始化复制 + 验证 collection 同步
│   ├── test_collection.py       # collection 生命周期：create→index→load→release→drop
│   ├── test_insert.py           # insert/delete 数据一致性
│   ├── test_partition.py        # partition 生命周期
│   ├── test_continuously_insert.py  # 持续写入压测（insert/upsert/delete）
│   ├── test_force_promote.py    # force promote 故障切换（5个场景）
│   ├── test_switchover.py       # 主备切换 + 数据完整性校验
│   ├── test_update_config.py    # replicate config 反复切换测试
│   ├── test_standalone_promote.py   # 单集群独立提升测试
│   └── test_rbac.py             # RBAC 权限校验
│
├── failover/                    # 故障切换高级场景
│   ├── test_restart_b_during_force_promote.py   # promote 过程中重启 B
│   ├── test_restart_a_during_force_promote.py   # promote 过程中重启 A
│   ├── test_incomplete_broadcast_ddl.py         # 不完整 DDL broadcast
│   └── test_fastlock_contention.py              # FastLock 资源争用
│
├── test_mtls/                   # mTLS 跨集群复制测试（3集群）
│   ├── test_mtls_replication.py   # A→B, A→C mTLS 复制
│   ├── test_mtls_switchover.py    # B 升主 mTLS 切换
│   └── test_mtls_failure.py       # TLS 证书错误/缺失的负面测试
│
├── test_replica_features/       # 副本特性压测
│   ├── test_stress.py             # switchover + replica 异构压测
│   └── test_pchannel_increase.py  # pchannel 动态扩容
│
├── test_deployment/             # K8s 部署配置
└── update_replicate_config/     # Go 工具（手动配置更新）
```

## 前置条件

- 编译好的 Milvus 可执行文件：`MILVUS_DEV_PATH` 指向源码根目录
- 数据目录：`MILVUS_VOLUME_DIRECTORY`
- Docker + docker compose（etcd/minio/pulsar）
- Python 环境：`conda activate milvus2`（需要 `pymilvus`、`loguru`）

## 快速开始

```bash
# 1. 设置环境
export MILVUS_DEV_PATH=~/workspace/milvus
export MILVUS_VOLUME_DIRECTORY=/data/tmp/milvus-volumes
conda activate milvus2

# 2. 启动集群（A + B，具体方式视测试目录而定，参考各子目录的 start_clusters.sh）

# 3. 运行测试（每个脚本都是独立可执行的）
cd testcases
python test_quick_start.py          # 冒烟测试
python test_collection.py           # collection 生命周期
python test_insert.py               # 数据一致性
python test_force_promote.py        # force promote（全部5个场景）
python test_force_promote.py success  # 只跑单个场景
python test_switchover.py --cycles 10 --duration 300
python test_continuously_insert.py --mode full --duration 60
```

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `CLUSTER_A_ADDR` | `tcp://localhost:19530` | 集群 A 地址 |
| `CLUSTER_B_ADDR` | `tcp://localhost:19531` | 集群 B 地址 |
| `MILVUS_TOKEN` | `root:Milvus` | 认证 token |
| `CDC_TIMEOUT` | `180` | 等待复制同步的全局超时（秒） |
| `FAILOVER_ROWS` | `300` | 每轮插入行数 |
| `FAILOVER_TIMEOUT` | `180` | failover 异步操作超时（秒） |

## 端口布局

| 组件 | Cluster A | Cluster B |
|------|-----------|-----------|
| Proxy gRPC | 19530 | 19531 |
| mixcoord metrics | 19100 | 19200 |
| proxy metrics | 19101 | 19201 |
| datanode | 19102 | 19202 |
| indexnode | 19103 | 19203 |
| streaming nodes | 19104-19106 | 19204-19206 |
| query nodes | 19107-19109 | 19207-19209 |
| CDC | 19150 | 19250 |

## 编写新测试

所有测试脚本遵循统一结构：

```python
import _path_setup  # noqa: F401  — 设置 sys.path
from common import (
    cluster_A_client, cluster_B_client,
    setup_collection, insert_and_verify, cleanup_collection,
    ...
)

def test_xxx():
    # 1. setup
    # 2. 操作 + 断言
    # 3. cleanup
    logger.info("PASSED: xxx")

if __name__ == "__main__":
    test_xxx()
```

关键原则：
- 每个脚本 `python xxx.py` 直接可执行，成功退出码 0，失败抛异常退出码 1
- 使用 `from common import *` 引用共享代码，不要在测试目录里重复定义
- 需要 argparse 的长时间测试保留参数（`--cycles`、`--duration`）
- 所有等待使用 `common.wait` 中的函数，不要手写 polling 循环
