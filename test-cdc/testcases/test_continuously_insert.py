import argparse
from tracemalloc import start
from loguru import logger
from common import *
from collection import *
from index import *
from insert import *
from query import *

def get_max_id_from_primary(collection_name, client):
    query_expr = f"{PK_FIELD_NAME} >= 0"
    res = client.query(
        collection_name=collection_name,
        consistency_level="Strong",
        filter=query_expr,
        output_fields=[PK_FIELD_NAME]
    )
    
    if not res:
        error_msg = f"Collection {collection_name} is empty or does not exist."
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    max_id = max(item[PK_FIELD_NAME] for item in res)
    logger.info(f"Found max ID in collection {collection_name}: {max_id}")
    return max_id


def setup_collection_and_index(collection_name, primary_client, standby_client):
    create_collection_on_primary(collection_name, primary_client)
    wait_for_standby_create_collection(collection_name, standby_client)

    create_index_on_primary(collection_name, primary_client)
    wait_for_standby_create_index(collection_name, standby_client)

    load_collection_on_primary(collection_name, primary_client)
    wait_for_standby_load_collection(collection_name, standby_client)


def cleanup_collection(collection_name, primary_client, standby_client):
    release_collection_on_primary(collection_name, primary_client)
    wait_for_standby_release_collection(collection_name, standby_client)

    drop_collection_on_primary(collection_name, primary_client)
    wait_for_standby_drop_collection(collection_name, standby_client)


def insert_and_query_loop(collection_name, test_duration=600, start_id=None, primary_client=None, standby_client=None):
    start_time = time.time()
    if start_id is None:
        start_id = 0
    total_count = start_id
    loop_count = 0

    while time.time() - start_time < test_duration:
        # Insert data
        insert_into_primary(start_id, collection_name, primary_client)
        start_id += INSERT_COUNT
        total_count += INSERT_COUNT
        loop_count += 1
        
        # Upsert every 20 loops
        if loop_count % 20 == 0:
            # Upsert the current batch (update if exists, insert if not)
            upsert_start_id = start_id - INSERT_COUNT
            upsert_into_primary(upsert_start_id, collection_name, primary_client)
            logger.info(f"Upsert: upserted IDs from {upsert_start_id} to {upsert_start_id + INSERT_COUNT - 1}")
        
        # Random delete every 10 loops
        if loop_count % 10 == 0:
            # Delete first half of the current batch
            delete_start_id = start_id - INSERT_COUNT
            delete_end_id = delete_start_id + INSERT_COUNT // 2
            delete_expr = f"{PK_FIELD_NAME} >= {delete_start_id} and {PK_FIELD_NAME} < {delete_end_id}"
            delete_from_primary(collection_name, delete_expr, primary_client)
            total_count -= INSERT_COUNT // 2
            logger.info(f"Random delete: deleted IDs from {delete_start_id} to {delete_end_id-1}")

        logger.info(f"Entities number on primary: {total_count}")
        res_on_primary = query_on_primary_without_expected_count(collection_name, primary_client)
        wait_for_standby_query(collection_name, res_on_primary, standby_client)

    logger.info(f"Total count: {total_count}")
    logger.info(f"Loop count: {loop_count}")
    logger.info(f"Test duration: {time.time() - start_time}")
    
    return total_count, loop_count


def test_mode_1_full_cycle(test_duration=600):
    """Mode 1: Full cycle - setup + insert&query + cleanup"""

    primary_client = cluster_A_client
    standby_client = cluster_B_client

    collection_name = DEFAULT_COLLECTION_NAME
    
    logger.info("Mode: Full cycle - setup + insert&query + cleanup")
    
    # Setup
    setup_collection_and_index(collection_name, primary_client, standby_client)
    
    # Insert and query
    insert_and_query_loop(collection_name, test_duration, None, primary_client, standby_client)
    
    # Cleanup
    cleanup_collection(collection_name, primary_client, standby_client)


def test_mode_2_insert_query_only(test_duration=600):
    """Mode 2: Insert and query only (assumes collection already exists)"""

    primary_client = cluster_A_client
    standby_client = cluster_B_client
 
    collection_name = DEFAULT_COLLECTION_NAME
    
    logger.info("Mode: Insert and query only")
    
    # Get the maximum ID from existing collection to avoid primary key conflicts
    max_id = get_max_id_from_primary(collection_name, primary_client)
    start_id = max_id + 1
    logger.info(f"Starting insert from ID: {start_id}")
    
    # Only insert and query
    insert_and_query_loop(collection_name, test_duration, start_id, primary_client, standby_client)


def test_mode_3_cleanup_only():
    """Mode 3: Cleanup only (drop and release)"""

    primary_client = cluster_A_client
    standby_client = cluster_B_client

    collection_name = DEFAULT_COLLECTION_NAME
    
    logger.info("Mode: Cleanup only - drop and release")
    
    # Only cleanup
    cleanup_collection(collection_name, primary_client, standby_client)


def test_mode_4_insert_query_only_reversed(test_duration=600):
    """Mode 4: Insert and query only with reversed roles (B->A)"""

    primary_client = cluster_B_client  # B as primary
    standby_client = cluster_A_client  # A as standby

    collection_name = DEFAULT_COLLECTION_NAME
    
    logger.info("Mode: Insert and query only with reversed roles (B->A)")
    
    # Get the maximum ID from existing collection to avoid primary key conflicts
    max_id = get_max_id_from_primary(collection_name, primary_client)
    start_id = max_id + 1
    logger.info(f"Starting insert from ID: {start_id}")
    
    # Only insert and query with reversed roles
    insert_and_query_loop(collection_name, test_duration, start_id, primary_client, standby_client)


def test_continuously_insert(mode="full", test_duration=600):
    """Main test function with mode selection"""
    if mode == "full":
        test_mode_1_full_cycle(test_duration)
    elif mode == "insert":
        test_mode_2_insert_query_only(test_duration)
    elif mode == "cleanup":
        test_mode_3_cleanup_only()
    elif mode == "insert_reversed":
        test_mode_4_insert_query_only_reversed(test_duration)
    else:
        raise ValueError(f"Invalid mode: {mode}. Must be 'full', 'insert', 'cleanup', or 'insert_reversed'.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test continuously insert with different modes')
    parser.add_argument('--mode', type=str, choices=['full', 'insert', 'cleanup', 'insert_reversed'], default='full',
                        help='Test mode: full=complete cycle, insert=insert&query only (A->B), cleanup=cleanup only, insert_reversed=insert&query only (B->A)')
    parser.add_argument('--duration', type=int, default=600,
                        help='Test duration in seconds (default: 600)')
    
    args = parser.parse_args()
    
    logger.info(f"Starting test in '{args.mode}' mode with duration {args.duration} seconds")
    test_continuously_insert(args.mode, args.duration)