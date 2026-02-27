#!/bin/bash

PORTS=(3001 3002 3003)

echo "Starting parallel load tests with randomized values..."

for PORT in "${PORTS[@]}"; do
  for i in {1..5}; do
    
    # 1. Generate a random offset between 1 and 100
    OFFSET=$(( RANDOM % 100 + 1 ))
    
    # 2. Add the offset to 'i' to get a mismatched value index
    VAL_INDEX=$(( i + OFFSET ))
    
    echo "Launching test: Port $PORT | Key key_$i | Value value_$VAL_INDEX"
    
    # 3. Inject both variables into the JSON payload
    hey \
      -m POST \
      -n 100 \
      -H "Accept: application/json" \
      -H "Content-Type: application/json" \
      -d "{\"key\": \"key_$i\", \"value\": \"value_$VAL_INDEX\"}" \
      http://localhost:$PORT/crud > "results_port${PORT}_key${i}.txt" 2>&1 &
      
  done
done

echo "All 15 batches fired simultaneously! Waiting for them to finish..."

wait

echo "All tests complete! Check the generated .txt files for your results."
