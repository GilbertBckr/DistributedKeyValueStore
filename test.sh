#!/bin/bash

# Target a single node to prevent cross-port local contention
TARGET_PORT=3001

echo "Starting isolated load test on Port $TARGET_PORT for 15 seconds..."

# Spin up 5 concurrent workers sending different data
for i in {1..5}; do
  
  # Generate a random offset for the value
  OFFSET=$(( $RANDOM % 100 + 1 ))
  VAL_INDEX=$(( i + OFFSET ))
  
  echo "Launching worker $i -> Key: key_$i | Value: value_$VAL_INDEX"
  
  # Fire 'hey' at the single target port
  hey \
    -m POST \
    -z 15s \
    -q 10 \
    -c 2 \
    -H "Accept: application/json" \
    -H "Content-Type: application/json" \
    -d "{\"key\": \"key_$i\", \"value\": \"value_$VAL_INDEX\"}" \
    -t 3 \
    "http://localhost:$TARGET_PORT/crud" > "results_worker${i}.txt" 2>&1 &
    
done

echo "All 5 workers are firing at port $TARGET_PORT! Waiting 15 seconds..."

# Hold the script here until the 15 seconds are up and all 'hey' processes exit
wait

echo "Test complete! Check the 5 results_worker.txt files to see how the node survived."
