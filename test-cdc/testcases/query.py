import time
from loguru import logger
from common import *
from collection import *

def query_on_primary(collection_name, expected_count):
    query_expr = f"{PK_FIELD_NAME} >= 0"
    res = primary_client.query(
        collection_name=collection_name,
        consistency_level="Strong",
        filter=query_expr,
        output_fields=[PK_FIELD_NAME]
    )
    if len(res) != expected_count:
        error_msg = f"Query result count not match on primary: expected={expected_count}, actual={len(res)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    return res


def query_on_primary_without_expected_count(collection_name):
    query_expr = f"{PK_FIELD_NAME} >= 0"
    res = primary_client.query(
        collection_name=collection_name,
        consistency_level="Strong",
        filter=query_expr,
        output_fields=[PK_FIELD_NAME]
    )
    return res


def query_on_secondary(collection_name):
    query_expr = f"{PK_FIELD_NAME} >= 0"
    res = secondary_client.query(
        collection_name=collection_name,
        consistency_level="Strong",
        filter=query_expr,
        output_fields=[PK_FIELD_NAME]
    )
    return res


def wait_for_secondary_query(collection_name, res_on_primary):
    start_time = time.time()
    while True:
        res_on_secondary = query_on_secondary(collection_name)
        if len(res_on_secondary) != len(res_on_primary):
            logger.warning(
                f"Length not match: primary={len(res_on_primary)}, secondary={len(res_on_secondary)}")
        else:
            ids_primary = sorted([item[PK_FIELD_NAME] for item in res_on_primary])
            ids_secondary = sorted([item[PK_FIELD_NAME] for item in res_on_secondary])
            if ids_primary == ids_secondary:
                break
        time.sleep(1)
        if time.time() - start_time > TIMEOUT:
            error_msg = f"Timeout waiting for query result to be consistent on secondary"
            logger.error(error_msg)
            raise TimeoutError(error_msg)
    logger.info(f"Success: Query result consistent between primary and secondary!")
