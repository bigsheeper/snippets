from pymilvus import MilvusClient

cluster_a_addr = "http://127.0.0.1:19530"
cluster_b_addr = "http://127.0.0.1:19531"
cluster_a_token = "root:Milvus"
cluster_b_token = "root:Milvus"
cluster_a_id = "cluster-a"
cluster_b_id = "cluster-b"
pchannel_num = 16

cluster_a_pchannels = []
cluster_b_pchannels = []
for i in range(pchannel_num):
    cluster_a_pchannels.append(f"{cluster_a_id}-rootcoord-dml_{i}")
    cluster_b_pchannels.append(f"{cluster_b_id}-rootcoord-dml_{i}")

config = {
    "clusters": [
        {
            "cluster_id": cluster_a_id,
            "connection_param": {
                "uri": cluster_a_addr,
                "token": cluster_a_token
            },
            "pchannels": cluster_a_pchannels
        },
        {
            "cluster_id": cluster_b_id,
            "connection_param": {
                "uri": cluster_b_addr,
                "token": cluster_b_token
            },
            "pchannels": cluster_b_pchannels
        }
    ],
    "cross_cluster_topology": [
        {
            "source_cluster_id": cluster_a_id,
            "target_cluster_id": cluster_b_id
        }
    ]
}

client_a = MilvusClient(uri=cluster_a_addr, token=cluster_a_token)
client_a.update_replicate_configuration(**config)
client_a.close()

client_b = MilvusClient(uri=cluster_b_addr, token=cluster_b_token)
client_b.update_replicate_configuration(**config)
client_b.close()
