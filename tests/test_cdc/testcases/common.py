import os
from pymilvus import MilvusClient

# Common constants
TIMEOUT = 180
# Allow overriding via environment variables to match dynamic ports chosen at startup.
DEFAULT_A = "tcp://localhost:19530"
DEFAULT_B = "tcp://localhost:19531"
DEFAULT_TOKEN = "root:Milvus"

CLUSTER_A_ADDR = os.getenv("CLUSTER_A_ADDR", DEFAULT_A)
CLUSTER_B_ADDR = os.getenv("CLUSTER_B_ADDR", DEFAULT_B)
TOKEN = os.getenv("MILVUS_TOKEN", DEFAULT_TOKEN)

# If env not provided, export sensible defaults for downstream tools/tests
if not os.getenv("CLUSTER_A_ADDR"):
    os.environ["CLUSTER_A_ADDR"] = CLUSTER_A_ADDR
if not os.getenv("CLUSTER_B_ADDR"):
    os.environ["CLUSTER_B_ADDR"] = CLUSTER_B_ADDR
if not os.getenv("MILVUS_TOKEN"):
    os.environ["MILVUS_TOKEN"] = TOKEN

# Initialize clients with probing when env not provided
cluster_A_client = MilvusClient(uri=CLUSTER_A_ADDR, token=TOKEN)

if os.getenv("CLUSTER_B_ADDR"):
    cluster_B_client = MilvusClient(uri=CLUSTER_B_ADDR, token=TOKEN)
else:
    last_exc = None
    # Prefer external gRPC first (startup脚本可能把B外部端口从19531移动到奇数序列)
    # 尝试外部 gRPC 端口序列（19531/19533/...）优先，其次内部 19528
    candidates = [f"tcp://localhost:{p}" for p in range(19531, 19551, 2)] + ["tcp://localhost:19528"]
    for cand in candidates:
        try:
            cluster_B_client = MilvusClient(uri=cand, token=TOKEN)
            os.environ["CLUSTER_B_ADDR"] = cand
            break
        except Exception as e:
            last_exc = e
            continue
    else:
        raise last_exc if last_exc else RuntimeError("failed to connect to cluster B on default ports (19531/19533/…/19549, 19528)")
