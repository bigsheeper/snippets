# CDC Failover 测试

Force promote 故障切换的高级场景测试。

## 测试用例

| 脚本 | 场景 |
|------|------|
| `test_restart_b_during_force_promote.py` | promote 过程中重启 B（standby） |
| `test_restart_a_during_force_promote.py` | promote 过程中重启 A（primary） |
| `test_incomplete_broadcast_ddl.py` | create→index→load→release→promote→create new→verify |
| `test_fastlock_contention.py` | promote 期间并发 update_replicate_configuration |

## 运行

```bash
conda activate milvus2
cd failover

# 单个用例
FAILOVER_TIMEOUT=180 python test_restart_b_during_force_promote.py
FAILOVER_TIMEOUT=180 python test_restart_a_during_force_promote.py
python test_incomplete_broadcast_ddl.py
python test_fastlock_contention.py
```

## 依赖

- 共享库：`common/`（常量、客户端、config 构建、集群控制）
- 本目录 `utils.py`：failover 专用工具（`ensure_secondary_b()`、`force_promote_b()`）
- 集群控制：`common/cluster_control.py`（stop/start/restart/health check）

## 前置条件

- 本地 A/B 集群运行中（cluster+streaming 模式）
- `MILVUS_DEV_PATH` 和 `MILVUS_VOLUME_DIRECTORY` 已设置
- 端口：A `19530`，B `19531`，健康检查 `19101`/`19201`

## 排查

- 超时：降低 `FAILOVER_ROWS`（如 100），提高 `FAILOVER_TIMEOUT`（如 240）
- 端口占用：`ss -tlnp | grep -E ':19530|:19531|:1910[0-9]|:1920[0-9]'`
- 集群不健康：`curl -s http://127.0.0.1:19101/healthz && curl -s http://127.0.0.1:19201/healthz`
