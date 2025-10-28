from pymilvus import MilvusClient

# Common constants
TIMEOUT = 180
CLUSTER_A_ADDR = "http://localhost:19530"
CLUSTER_B_ADDR = "http://localhost:19531"
TOKEN = "root:Milvus"

# Initialize clients
cluster_A_client = MilvusClient(
    uri=CLUSTER_A_ADDR,
    token=TOKEN
)
cluster_B_client = MilvusClient(
    uri=CLUSTER_B_ADDR,
    token=TOKEN
)
