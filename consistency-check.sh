#!/bin/bash

# ---------------------------------------------------------------------------
# Consistency Check for a Specific Key
# ---------------------------------------------------------------------------
# Usage: ./consistency-check.sh <key>
#
# Queries every node in the cluster for the given key IN PARALLEL and
# validates TRUE consistency:
#
#   CONSISTENT means one of:
#     - All responding nodes return the SAME value       (HTTP 200)
#     - All responding nodes report the key as locked    (HTTP 409)
#     - Some nodes are locked (409) and the rest agree   (HTTP 200)
#
#   INCONSISTENT means:
#     - Two or more nodes returned DIFFERENT values
#
#   Other outcomes (cluster down, single node, unexpected errors) are
#   reported but do not count as a consistency violation.
#
# All requests are fired simultaneously as background processes so that
# the snapshot of the cluster is as close to a single point-in-time as
# possible, avoiding false inconsistencies caused by sequential delays.
# ---------------------------------------------------------------------------

if [ -z "$1" ]; then
  echo "Usage: $0 <key>"
  echo "Example: $0 key_1"
  exit 1
fi

KEY="$1"
PORTS=(3001 3002 3003)

echo "Consistency Check for key: '${KEY}'"
echo "==================================="

# Create a temporary directory for per-node response files
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# ── Fire all requests in parallel ─────────────────────────────────────────
PIDS=()
for PORT in "${PORTS[@]}"; do
  (
    curl -s --max-time 2 -w "\n%{http_code}" \
      "http://localhost:${PORT}/crud/${KEY}" \
      > "${TMPDIR}/${PORT}" 2>/dev/null
    echo $? > "${TMPDIR}/${PORT}.exit"
  ) &
  PIDS+=($!)
done

# Wait for every request to finish
for PID in "${PIDS[@]}"; do
  wait "$PID"
done

# ── Collect results ───────────────────────────────────────────────────────
OK_VALUES=()       # values from nodes that returned 200
LOCKED_NODES=()    # ports of nodes that returned 409 (key locked)
DOWN_NODES=()      # ports of nodes that are unreachable / timed out
ERROR_NODES=()     # ports of nodes that returned other HTTP errors

for PORT in "${PORTS[@]}"; do
  CURL_EXIT=$(cat "${TMPDIR}/${PORT}.exit")

  if [ "$CURL_EXIT" -ne 0 ]; then
    echo "  Node :${PORT} -> [DOWN/TIMEOUT]"
    DOWN_NODES+=("$PORT")
    continue
  fi

  RESPONSE=$(cat "${TMPDIR}/${PORT}")
  HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
  RESP_BODY=$(echo "$RESPONSE" | sed '$d')

  if [ "$HTTP_CODE" -eq 200 ] 2>/dev/null; then
    VALUE=$(echo "$RESP_BODY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('value',''))" 2>/dev/null)
    if [ $? -ne 0 ]; then
      VALUE="$RESP_BODY"  # fallback: use raw body
    fi
    echo "  Node :${PORT} -> value='${VALUE}' (HTTP 200)"
    OK_VALUES+=("$VALUE")

  elif [ "$HTTP_CODE" -eq 409 ] 2>/dev/null; then
    echo "  Node :${PORT} -> [LOCKED] (HTTP 409)"
    LOCKED_NODES+=("$PORT")

  else
    echo "  Node :${PORT} -> [ERROR] (HTTP ${HTTP_CODE}) ${RESP_BODY}"
    ERROR_NODES+=("$PORT")
  fi
done

echo "-----------------------------------"

TOTAL=${#PORTS[@]}
NUM_OK=${#OK_VALUES[@]}
NUM_LOCKED=${#LOCKED_NODES[@]}
NUM_DOWN=${#DOWN_NODES[@]}
NUM_ERROR=${#ERROR_NODES[@]}

# ── Evaluate consistency ──────────────────────────────────────────────────

# 1. Every node is unreachable
if [ $NUM_DOWN -eq $TOTAL ]; then
  echo "Result: CLUSTER UNAVAILABLE (all nodes down)"
  exit 2
fi

# 2. All responding nodes report the key as locked
if [ $NUM_OK -eq 0 ] && [ $NUM_LOCKED -gt 0 ] && [ $NUM_ERROR -eq 0 ]; then
  echo "Result: CONSISTENT - all responding nodes report key is locked (2PC in progress)"
  exit 0
fi

# 3. No node returned a value (mix of errors / down)
if [ $NUM_OK -eq 0 ]; then
  echo "Result: UNABLE TO VERIFY (no node returned a value; ${NUM_ERROR} error(s), ${NUM_DOWN} down)"
  exit 2
fi

# 4. Only one node returned a value – cannot cross-check
if [ $NUM_OK -eq 1 ] && [ $NUM_LOCKED -eq 0 ]; then
  echo "Result: NO QUORUM (only 1 node returned a value, consistency unverified)"
  exit 2
fi

# 5. Compare all returned values
CONSISTENT=true
FIRST_VAL="${OK_VALUES[0]}"
for VAL in "${OK_VALUES[@]}"; do
  if [ "$VAL" != "$FIRST_VAL" ]; then
    CONSISTENT=false
    break
  fi
done

if [ "$CONSISTENT" = true ]; then
  DETAIL=""
  [ $NUM_LOCKED -gt 0 ] && DETAIL="${DETAIL}, ${NUM_LOCKED} locked"
  [ $NUM_DOWN   -gt 0 ] && DETAIL="${DETAIL}, ${NUM_DOWN} down"
  [ $NUM_ERROR  -gt 0 ] && DETAIL="${DETAIL}, ${NUM_ERROR} error(s)"
  echo "Result: CONSISTENT (${NUM_OK} node(s) agree on value='${FIRST_VAL}'${DETAIL})"
  exit 0
else
  echo "Result: INCONSISTENT - nodes returned different values!"
  exit 1
fi
