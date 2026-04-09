from port_layout import (
    CLUSTER_A_CDC_METRICS,
    CLUSTER_A_DATANODE_HEALTH,
    CLUSTER_A_INDEXNODE_HEALTH,
    CLUSTER_A_METRICS_BASE,
    CLUSTER_A_MIXCOORD_HEALTH,
    CLUSTER_A_PROXY_HEALTH,
    CLUSTER_A_QUERYNODE_HEALTH,
    CLUSTER_A_STREAMINGNODE_HEALTH,
    CLUSTER_B_CDC_METRICS,
    CLUSTER_B_DATANODE_HEALTH,
    CLUSTER_B_INDEXNODE_HEALTH,
    CLUSTER_B_METRICS_BASE,
    CLUSTER_B_MIXCOORD_HEALTH,
    CLUSTER_B_PROXY_HEALTH,
    CLUSTER_B_QUERYNODE_HEALTH,
    CLUSTER_B_STREAMINGNODE_HEALTH,
)


def test_full_shared_cluster_port_layout_constants():
    assert CLUSTER_A_METRICS_BASE == 19100
    assert CLUSTER_B_METRICS_BASE == 19200

    assert CLUSTER_A_MIXCOORD_HEALTH == "http://127.0.0.1:19100"
    assert CLUSTER_A_PROXY_HEALTH == "http://127.0.0.1:19101"
    assert CLUSTER_A_DATANODE_HEALTH == "http://127.0.0.1:19102"
    assert CLUSTER_A_INDEXNODE_HEALTH == "http://127.0.0.1:19103"
    assert CLUSTER_A_STREAMINGNODE_HEALTH == (
        "http://127.0.0.1:19104",
        "http://127.0.0.1:19105",
        "http://127.0.0.1:19106",
    )
    assert CLUSTER_A_QUERYNODE_HEALTH == (
        "http://127.0.0.1:19107",
        "http://127.0.0.1:19108",
        "http://127.0.0.1:19109",
    )
    assert CLUSTER_A_CDC_METRICS == "http://127.0.0.1:19150"

    assert CLUSTER_B_MIXCOORD_HEALTH == "http://127.0.0.1:19200"
    assert CLUSTER_B_PROXY_HEALTH == "http://127.0.0.1:19201"
    assert CLUSTER_B_DATANODE_HEALTH == "http://127.0.0.1:19202"
    assert CLUSTER_B_INDEXNODE_HEALTH == "http://127.0.0.1:19203"
    assert CLUSTER_B_STREAMINGNODE_HEALTH == (
        "http://127.0.0.1:19204",
        "http://127.0.0.1:19205",
        "http://127.0.0.1:19206",
    )
    assert CLUSTER_B_QUERYNODE_HEALTH == (
        "http://127.0.0.1:19207",
        "http://127.0.0.1:19208",
        "http://127.0.0.1:19209",
    )
    assert CLUSTER_B_CDC_METRICS == "http://127.0.0.1:19250"
