#!/bin/bash
# Binary search for optimal maxFreq across cache sizes
set -e

S3FIFO="$(dirname "$0")/../s3fifo.go"
SIZE="${1:-16}"
RESULTS_FILE="/tmp/max_freq_${SIZE}k.txt"

# Clear previous results
> "$RESULTS_FILE"

run_benchmark() {
    local freq=$1

    # Skip if already tested
    if grep -q "^$freq " "$RESULTS_FILE" 2>/dev/null; then
        return 0
    fi

    # Update maxFreq in s3fifo.go
    sed -i '' "s/maxFreq = [0-9]*/maxFreq = $freq/" "$S3FIFO"

    # Run benchmark - capture output
    output=$(SIZES=$SIZE SUITES=hitrate make benchmark 2>&1) || true

    # Extract the average from Category Summary line for multicache
    avg=$(echo "$output" | grep -E '^\s*\| multicache' | tail -1 | awk -F'|' '{print $3}' | grep -o '[0-9.]*')

    if [ -z "$avg" ]; then
        echo "freq=$freq: FAILED"
        return 1
    fi

    echo "maxFreq=$freq: hitrate=$avg%"
    echo "$freq $avg" >> "$RESULTS_FILE"
}

echo "=== Tuning maxFreq for ${SIZE}K cache ==="
echo ""

# Test range from 2 (minimum safe) to 10
# Note: maxFreq=1 causes infinite loop, so minimum is 2
echo "=== Testing maxFreq 2-10 ==="
for freq in 2 3 4 5 6 7 8 9 10; do
    run_benchmark $freq
done

echo ""
echo "=== Results (sorted by hitrate) ==="
sort -k2 -rn "$RESULTS_FILE"

# Find the best
best_line=$(sort -k2 -rn "$RESULTS_FILE" | head -1)
best_freq=$(echo "$best_line" | awk '{print $1}')
best_hitrate=$(echo "$best_line" | awk '{print $2}')

echo ""
echo "=== OPTIMAL for ${SIZE}K ==="
echo "maxFreq = $best_freq with hitrate=$best_hitrate%"

# Restore original value
sed -i '' "s/maxFreq = [0-9]*/maxFreq = 5/" "$S3FIFO"
echo ""
echo "Restored maxFreq to 5"
