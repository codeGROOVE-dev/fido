#!/bin/bash
# Fine-grained binary search for optimal smallQueueRatio
set -e

S3FIFO="$(dirname "$0")/../s3fifo.go"
SIZE="${1:-16}"
RESULTS_FILE="/tmp/small_ratio_${SIZE}k.txt"

# Clear previous results
> "$RESULTS_FILE"

run_benchmark() {
    local ratio=$1

    # Skip if already tested
    if grep -q "^$ratio " "$RESULTS_FILE" 2>/dev/null; then
        return 0
    fi

    # Update smallQueueRatio in s3fifo.go
    sed -i '' "s/smallQueueRatio = [0-9]*/smallQueueRatio = $ratio/" "$S3FIFO"

    # Run benchmark - capture output
    output=$(SIZES=$SIZE SUITES=hitrate make benchmark 2>&1) || true

    # Extract the average from Category Summary line for multicache
    avg=$(echo "$output" | grep -E '^\s*\| multicache' | tail -1 | awk -F'|' '{print $3}' | grep -o '[0-9.]*')

    if [ -z "$avg" ]; then
        echo "ratio=$ratio: FAILED"
        return 1
    fi

    echo "ratio=$ratio ($(echo "scale=1; $ratio/10" | bc)%): hitrate=$avg%"
    echo "$ratio $avg" >> "$RESULTS_FILE"
}

echo "=== Fine-tuning smallQueueRatio for ${SIZE}K cache ==="
echo ""

# Binary search between bounds
low=100
high=160
iterations=0
max_iterations=20

while [ $((high - low)) -gt 2 ] && [ $iterations -lt $max_iterations ]; do
    mid=$(( (low + high) / 2 ))
    left=$(( (low + mid) / 2 ))
    right=$(( (mid + high) / 2 ))

    echo "--- Iteration $((iterations+1)): testing $left, $mid, $right (range: $low-$high) ---"

    run_benchmark $left
    run_benchmark $mid
    run_benchmark $right

    # Get results
    left_val=$(grep "^$left " "$RESULTS_FILE" | awk '{print $2}')
    mid_val=$(grep "^$mid " "$RESULTS_FILE" | awk '{print $2}')
    right_val=$(grep "^$right " "$RESULTS_FILE" | awk '{print $2}')

    # Find best and narrow range
    best_val=$left_val
    best_pos="left"

    if [ "$(echo "$mid_val > $best_val" | bc -l)" -eq 1 ]; then
        best_val=$mid_val
        best_pos="mid"
    fi
    if [ "$(echo "$right_val > $best_val" | bc -l)" -eq 1 ]; then
        best_val=$right_val
        best_pos="right"
    fi

    case $best_pos in
        left)  high=$mid ;;
        mid)   low=$left; high=$right ;;
        right) low=$mid ;;
    esac

    iterations=$((iterations + 1))
done

# Final sweep of remaining range
echo ""
echo "--- Final sweep: $low to $high ---"
for ratio in $(seq $low $high); do
    run_benchmark $ratio
done

echo ""
echo "=== Results for ${SIZE}K (sorted by hitrate) ==="
sort -k2 -rn "$RESULTS_FILE" | head -10

# Find the best
best_line=$(sort -k2 -rn "$RESULTS_FILE" | head -1)
best_ratio=$(echo "$best_line" | awk '{print $1}')
best_hitrate=$(echo "$best_line" | awk '{print $2}')

echo ""
echo "=== OPTIMAL for ${SIZE}K ==="
echo "smallQueueRatio = $best_ratio ($(echo "scale=1; $best_ratio/10" | bc)%) with hitrate=$best_hitrate%"

# Restore original value
sed -i '' "s/smallQueueRatio = [0-9]*/smallQueueRatio = 137/" "$S3FIFO"
echo ""
echo "Restored smallQueueRatio to 137"
