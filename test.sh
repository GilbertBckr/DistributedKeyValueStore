#!/bin/bash

echo "Starting Concurrent Random Load Test (Same-Node Collisions Enabled)..."
echo "Press Ctrl+C to stop."
echo "-----------------------------------"

while true; do
  # 1. Pick two random keys from a larger pool (e.g., 1 to 100)
  KEY_1="key_$(( (RANDOM % 10) + 1 ))"
  KEY_2="key_$(( (RANDOM % 10) + 1 ))"

  VAL_1="val_A_${RANDOM}"
  VAL_2="val_B_${RANDOM}"

  # 2. Pick two random nodes independently (they can now be the same)
  PORTS=(3001 3002 3003)
  
  IDX1=$(( RANDOM % 3 ))
  IDX2=$(( RANDOM % 3 ))

  NODE_1=${PORTS[$IDX1]}
  NODE_2=${PORTS[$IDX2]}

  echo "   -> Node :${NODE_1} receiving [${KEY_1}=${VAL_1}]"
  echo "   -> Node :${NODE_2} receiving [${KEY_2}=${VAL_2}]"

  # 3. Fire both requests concurrently
  curl -s -X POST "http://localhost:${NODE_1}/crud/" \
    -H "Content-Type: application/json" \
    -d "{\"key\":\"${KEY_1}\",\"value\":\"${VAL_1}\"}" \
    --max-time 2 > /dev/null &
  PID_1=$!

  curl -s -X POST "http://localhost:${NODE_2}/crud/" \
    -H "Content-Type: application/json" \
    -d "{\"key\":\"${KEY_2}\",\"value\":\"${VAL_2}\"}" \
    --max-time 2 > /dev/null &
  PID_2=$!

  # 4. Wait for both to complete
  wait $PID_1
  STATUS_1=$?
  wait $PID_2
  STATUS_2=$?

  # 5. Evaluate the cluster's health
  if [ $STATUS_1 -eq 0 ] && [ $STATUS_2 -eq 0 ]; then
    echo "✅ Both requests accepted"
  elif [ $STATUS_1 -ne 0 ] && [ $STATUS_2 -ne 0 ]; then
    echo "❌ Both failed!"
  else
    echo "⚠️  Partial success. One request failed."
  fi

  echo "-----------------------------------"
  sleep 0.1 
done
