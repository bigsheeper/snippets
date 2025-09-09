import argparse
from loguru import logger
from common import *
from collection import *
from index import *
from insert import *
from query import *

def setup_collection_and_index(collection_name):
    """Setup collection, index and load collection"""
    create_collection_on_primary(collection_name)
    wait_for_secondary_create_collection(collection_name)

    create_index_on_primary(collection_name)
    wait_for_secondary_create_index(collection_name)

    load_collection_on_primary(collection_name)
    wait_for_secondary_load_collection(collection_name)

def cleanup_collection(collection_name):
    """Release and drop collection"""
    release_collection_on_primary(collection_name)
    wait_for_secondary_release_collection(collection_name)

    drop_collection_on_primary(collection_name)
    wait_for_secondary_drop_collection(collection_name)

def insert_and_query_loop(collection_name, test_duration=600):
    """Perform insert and query operations in a loop"""
    total_count = 0
    start_time = time.time()
    start_id = 0
    loop_count = 0

    while time.time() - start_time < test_duration:
        # Insert data
        insert_into_primary(start_id, collection_name)
        start_id += INSERT_COUNT
        total_count += INSERT_COUNT
        loop_count += 1
        
        # Random delete every 10 loops
        if loop_count % 10 == 0:
            # Delete first half of the current batch
            delete_start_id = start_id - INSERT_COUNT
            delete_end_id = delete_start_id + INSERT_COUNT // 2
            delete_expr = f"{PK_FIELD_NAME} >= {delete_start_id} and {PK_FIELD_NAME} < {delete_end_id}"
            delete_from_primary(collection_name, delete_expr)
            total_count -= INSERT_COUNT // 2
            logger.info(f"Random delete: deleted IDs from {delete_start_id} to {delete_end_id-1}")

        logger.info(f"Entities number on primary: {total_count}")
        res_on_primary = query_on_primary_without_expected_count(collection_name)
        wait_for_secondary_query(collection_name, res_on_primary)

    logger.info(f"Total count: {total_count}")
    logger.info(f"Loop count: {loop_count}")
    logger.info(f"Test duration: {time.time() - start_time}")
    
    return total_count, loop_count

def test_mode_1_full_cycle(test_duration=600):
    """Mode 1: Full cycle - setup + insert&query + cleanup"""
    collection_name = DEFAULT_COLLECTION_NAME
    
    logger.info("Mode: Full cycle - setup + insert&query + cleanup")
    
    # Setup
    setup_collection_and_index(collection_name)
    
    # Insert and query
    insert_and_query_loop(collection_name, test_duration)
    
    # Cleanup
    cleanup_collection(collection_name)

def test_mode_2_insert_query_only(test_duration=600):
    """Mode 2: Insert and query only (assumes collection already exists)"""
    collection_name = DEFAULT_COLLECTION_NAME
    
    logger.info("Mode: Insert and query only")
    
    # Only insert and query
    insert_and_query_loop(collection_name, test_duration)

def test_mode_3_cleanup_only():
    """Mode 3: Cleanup only (drop and release)"""
    collection_name = DEFAULT_COLLECTION_NAME
    
    logger.info("Mode: Cleanup only - drop and release")
    
    # Only cleanup
    cleanup_collection(collection_name)

def test_continuously_insert(mode="full", test_duration=600):
    """Main test function with mode selection"""
    if mode == "full":
        test_mode_1_full_cycle(test_duration)
    elif mode == "insert":
        test_mode_2_insert_query_only(test_duration)
    elif mode == "cleanup":
        test_mode_3_cleanup_only()
    else:
        raise ValueError(f"Invalid mode: {mode}. Must be 'full', 'insert', or 'cleanup'.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test continuously insert with different modes')
    parser.add_argument('--mode', type=str, choices=['full', 'insert', 'cleanup'], default='full',
                        help='Test mode: full=complete cycle, insert=insert&query only, cleanup=cleanup only')
    parser.add_argument('--duration', type=int, default=600,
                        help='Test duration in seconds (default: 600)')
    
    args = parser.parse_args()
    
    logger.info(f"Starting test in '{args.mode}' mode with duration {args.duration} seconds")
    test_continuously_insert(args.mode, args.duration)