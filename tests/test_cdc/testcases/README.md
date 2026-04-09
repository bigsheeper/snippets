# CDC 基础复制功能测试

Milvus CDC 跨集群复制的基础功能测试，覆盖 collection/partition 生命周期、数据一致性、switchover、force promote、RBAC 等。

## 测试用例

| 脚本 | 场景 | 自动初始化复制 |
|------|------|---------------|
| `test_quick_start.py` | 冒烟测试：初始化复制 + collection 同步验证 | ✅ |
| `test_collection.py` | collection 生命周期：create→index→load→release→drop（10个） | ✅ |
| `test_insert.py` | 多轮 insert + delete 数据一致性校验 | ✅ |
| `test_partition.py` | partition 生命周期：create→insert→load→query→release→drop | ✅ |
| `test_continuously_insert.py` | 持续写入压测：insert/upsert/delete 混合操作 | ✅ (full 模式) |
| `test_force_promote.py` | force promote 故障切换（5个子场景） | ✅ |
| `test_switchover.py` | A↔B 主备切换 + 每轮数据完整性校验 | ✅ |
| `test_update_config.py` | replicate config 反复切换（init/switch 交替） | ✅ |
| `test_standalone_promote.py` | 单集群独立提升（清空 topology） | ❌ 需已有配置 |
| `test_rbac.py` | 只读用户无法调用 update_replicate_configuration | ✅ |

## 快速开始

```bash
# 1. 设置环境
export MILVUS_DEV_PATH=~/workspace/milvus
export MILVUS_VOLUME_DIRECTORY=/data/tmp/milvus-volumes
conda activate milvus2

# 2. 启动集群（A + B + 基础设施）
bash start_clusters.sh

# 3. 等待健康检查通过（约 20-30 秒）
curl -s http://127.0.0.1:19101/healthz; echo   # Cluster A
curl -s http://127.0.0.1:19201/healthz; echo   # Cluster B

# 4. 运行测试
python test_quick_start.py                          # 冒烟
python test_collection.py                           # collection 生命周期
python test_insert.py                               # 数据一致性
python test_partition.py                            # partition 生命周期
python test_force_promote.py                        # 全部 5 个 force promote 场景
python test_force_promote.py success                # 只跑单个场景
python test_switchover.py --cycles 10 --duration 300
python test_continuously_insert.py --mode full --duration 60
python test_update_config.py --cycles 5
python test_standalone_promote.py A
python test_rbac.py

# 5. 停止集群
bash stop_clusters.sh
```

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `CLUSTER_A_ADDR` | `tcp://localhost:19530` | 集群 A 地址 |
| `CLUSTER_B_ADDR` | `tcp://localhost:19531` | 集群 B 地址 |
| `MILVUS_TOKEN` | `root:Milvus` | 认证 token |
| `CDC_TIMEOUT` | `180` | 等待复制同步的超时（秒） |
| `FAILOVER_ROWS` | `300` | 每轮插入行数（减小可加速测试） |

## 依赖

- 共享库：`../common/`（通过 `_path_setup.py` 自动设置路径）
- 集群脚本：`start_clusters.sh` / `stop_clusters.sh`（符号链接到 `../failover/`）
