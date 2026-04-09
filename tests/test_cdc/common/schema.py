import random
from pymilvus import MilvusClient, DataType
from common.constants import DEFAULT_DIM, VECTOR_FIELD_NAME, PK_FIELD_NAME


def create_collection_schema():
    schema = MilvusClient.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field(field_name=PK_FIELD_NAME, datatype=DataType.INT64, is_primary=True)
    schema.add_field(field_name=VECTOR_FIELD_NAME, datatype=DataType.FLOAT_VECTOR, dim=DEFAULT_DIM)
    return schema


def default_index_params(client):
    index_params = client.prepare_index_params()
    index_params.add_index(
        field_name=VECTOR_FIELD_NAME,
        index_type="AUTOINDEX",
        metric_type="COSINE",
    )
    return index_params


def generate_data(n, start_id, dim=DEFAULT_DIM):
    return [
        {PK_FIELD_NAME: start_id + i, VECTOR_FIELD_NAME: [random.uniform(-1, 1) for _ in range(dim)]}
        for i in range(n)
    ]
