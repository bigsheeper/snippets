#!/bin/bash

# # Script to cycle datanode: start a datanode every 5 minutes, stop it after 5 minutes, and loop

# # obtain the script installed path.
# SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# # Check required environment variables
# if [[ -z "${MILVUS_DEV_PATH}" ]]; then
#     echo "MILVUS_DEV_PATH is not set or is empty"
#     exit 1
# fi
# echo "MILVUS_DEV_PATH: ${MILVUS_DEV_PATH}"

# if [[ -z "${MILVUS_VOLUME_DIRECTORY}" ]]; then
#     echo "MILVUS_VOLUME_DIRECTORY is not set or is empty"
#     exit 1
# fi
# echo "MILVUS_VOLUME_DIRECTORY: ${MILVUS_VOLUME_DIRECTORY}"

# # Function to find the last milvus volume directory
# find_last_milvus_volume() {
#     if [[ -z "$MILVUS_VOLUME_DIRECTORY" || ! -d "$MILVUS_VOLUME_DIRECTORY" ]]; then
#         echo "MILVUS_VOLUME_DIRECTORY is not set or not a directory" >&2
#         return 1
#     fi

#     LAST_VOLUME=$(find "$MILVUS_VOLUME_DIRECTORY" -mindepth 1 -maxdepth 1 -type d ! -name '.' -printf '%T@ %p\n' | sort -k1,1nr | head -n 1 | cut -d' ' -f2-)
#     if [ -n "$LAST_VOLUME" ]; then
#         echo "${LAST_VOLUME}"
#         return 0
#     else
#         echo "No volume found" >&2
#         return 1
#     fi
# }

# # Setup volume directory (reuse last one)
# VOLUME_DIR=$(find_last_milvus_volume)
# if [[ $? -ne 0 || -z "${VOLUME_DIR}" ]]; then
#     echo "Failed to find last volume directory"
#     exit 1
# fi
# echo "Using volume directory: ${VOLUME_DIR}"

# LOG_PATH="${VOLUME_DIR}/milvus-logs/"
# mkdir -p "${LOG_PATH}" || exit 1

# # Setup environment
# pushd "$MILVUS_DEV_PATH" || exit 1
# source "./scripts/setenv.sh"

# export LOG_LEVEL=debug
# export MALLOC_CONF="prof:true,lg_prof_sample:1,prof_accum:true:background_thread:true"

# export MQ_TYPE="pulsar"
# # You can add support for Kafka/RMQ if needed by checking environment variables

# # Counter for datanode instances
DATANODE_COUNTER=1
BASE_METRICS_PORT=19100  # Start from 19100 to avoid conflicts

# Function to start a datanode
start_datanode() {
    local counter=$1
    local metrics_port=$((BASE_METRICS_PORT + counter))
    local log_prefix="datanode_cycle_${counter}"
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting datanode #${counter} with METRICS_PORT=${metrics_port}"
    
    /home/sheep/workspace/milvus/bin/milvus run datanode \
        1>"/tmp/${log_prefix}.stdout.log" \
        2>"/tmp/${log_prefix}.stderr.log" &
    
    local pid=$!
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Datanode #${counter} started with PID: ${pid}"
    echo ${pid}
}

# Function to stop a datanode by PID
stop_datanode() {
    local pid=$1
    local counter=$2
    
    if [[ -z "${pid}" ]]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] No PID provided for datanode #${counter}"
        return
    fi
    
    # Check if process is still running
    if ! kill -0 ${pid} 2>/dev/null; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Datanode #${counter} (PID: ${pid}) is not running"
        return
    fi
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Force killing datanode #${counter} (PID: ${pid})"
    kill -9 ${pid} 2>/dev/null
}

# Main loop
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting datanode cycling script (5 minutes per cycle)"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Press Ctrl+C to stop"

while true; do
    # Start datanode
    CURRENT_PID=$(start_datanode ${DATANODE_COUNTER})
    
    # Wait 5 minutes (300 seconds)
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Waiting 5 minutes before stopping datanode #${DATANODE_COUNTER}..."
    sleep 300
    
    # Stop datanode
    stop_datanode "${CURRENT_PID}" "${DATANODE_COUNTER}"
    
    # Increment counter for next iteration
    DATANODE_COUNTER=$((DATANODE_COUNTER + 1))
    CURRENT_PID=""
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Cycle completed, starting next cycle..."
done

popd || exit 1

