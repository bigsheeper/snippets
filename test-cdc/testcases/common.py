from pymilvus import MilvusClient

# Common constants
TIMEOUT = 180
PRIMARY_ADDR = "http://localhost:19530"
SECONDARY_ADDR = "http://localhost:19531"
TOKEN = "root:Milvus"

# Initialize clients
primary_client = MilvusClient(
    uri=PRIMARY_ADDR,
    token=TOKEN
)
secondary_client = MilvusClient(
    uri=SECONDARY_ADDR,
    token=TOKEN
)
