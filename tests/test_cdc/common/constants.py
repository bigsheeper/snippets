import os

# --- Timeouts (seconds) ---
TIMEOUT = int(os.getenv("CDC_TIMEOUT", "180"))
HEALTH_CHECK_TIMEOUT = 120
HEALTH_CHECK_INTERVAL = 2

# --- Auth ---
TOKEN = os.getenv("MILVUS_TOKEN", "root:Milvus")

# --- Cluster IDs ---
CLUSTER_A_ID = "by-dev1"
CLUSTER_B_ID = "by-dev2"
CLUSTER_C_ID = "by-dev3"

# --- Cluster Addresses ---
DEFAULT_CLUSTER_A_ADDR = "tcp://localhost:19530"
DEFAULT_CLUSTER_B_ADDR = "tcp://localhost:19531"
DEFAULT_CLUSTER_C_ADDR = "tcp://localhost:19532"
CLUSTER_A_ADDR = os.getenv("CLUSTER_A_ADDR", DEFAULT_CLUSTER_A_ADDR)
CLUSTER_B_ADDR = os.getenv("CLUSTER_B_ADDR", DEFAULT_CLUSTER_B_ADDR)
CLUSTER_C_ADDR = os.getenv("CLUSTER_C_ADDR", DEFAULT_CLUSTER_C_ADDR)

# --- Schema ---
DEFAULT_DIM = 4
VECTOR_FIELD_NAME = "vector"
PK_FIELD_NAME = "id"

# --- Collection ---
DEFAULT_COLLECTION_NAME = "test_collection"
COLLECTION_NAME_PREFIX = "collection_"
NUM_COLLECTIONS = 10

# --- Partition ---
PARTITION_NAME_PREFIX = "partition_"

# --- Insert ---
INSERT_COUNT = int(os.getenv("FAILOVER_ROWS", "300"))
INSERT_ROUNDS = 100

# --- PChannel ---
PCHANNEL_NUM = 16

# --- Cluster Control ---
MILVUS_CONTROL = os.path.expanduser("~/workspace/snippets/milvus_control/milvus_control")
