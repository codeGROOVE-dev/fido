#!/bin/bash
# Binary search for optimal maxPeakFreq across cache sizes
set -e

S3FIFO="$(dirname "$0")/../s3fifo.go"
SIZE="${1:-16}"
RESULTS_FILE="/tmp/peak_freq_${SIZE}k.txt"

# Clear previous results
> "$RESULTS_FILE"

run_benchmark() {
    local freq=$1

    # Skip if already tested
    if grep -q "^$freq " "$RESULTS_FILE" 2>/dev/null; then
        return 0
    fi

    # Update maxPeakFreq in s3fifo.go
    sed -i '' "s/maxPeakFreq = [0-9]*/maxPeakFreq = $freq/" "$S3FIFO"

    # Run benchmark - capture output
    output=$(SIZES=$SIZE SUITES=hitrate make benchmark 2>&1) || true

    # Extract the average from Category Summary line for multicache
    avg=$(echo "$output" | grep -E '^\s*\| multicache' | tail -1 | awk -F'|' '{print $3}' | grep -o '[0-9.]*')

    if [ -z "$avg" ]; then
        echo "peakFreq=$freq: FAILED"
        return 1
    fi

    echo "maxPeakFreq=$freq: hitrate=$avg%"
    echo "$freq $avg" >> "$RESULTS_FILE"
}

echo "=== Tuning maxPeakFreq for ${SIZE}K cache ==="
echo ""

# Test range from 5 to 40 (current is 21)
echo "=== Coarse scan 5-40 ==="
for freq in 5 10 15 21 25 30 35 40; do
    run_benchmark $freq
done

echo ""
echo "=== Results So Far ==="
sort -k2 -rn "$RESULTS_FILE" | head -10

# Find best from initial scan
best_line=$(sort -k2 -rn "$RESULTS_FILE" | head -1)
best_freq=$(echo "$best_line" | awk '{print $1}')

echo ""
echo "Best so far: $best_freq"

# Fine-tune around best
echo ""
echo "=== Fine-tuning around $best_freq ==="
low=$((best_freq - 8))
high=$((best_freq + 8))
[ $low -lt 3 ] && low=3

for freq in $(seq $low 2 $high); do
    run_benchmark $freq
done

echo ""
echo "=== Final Results (sorted by hitrate) ==="
sort -k2 -rn "$RESULTS_FILE" | head -15

# Find the best
best_line=$(sort -k2 -rn "$RESULTS_FILE" | head -1)
best_freq=$(echo "$best_line" | awk '{print $1}')
best_hitrate=$(echo "$best_line" | awk '{print $2}')

echo ""
echo "=== OPTIMAL for ${SIZE}K ==="
echo "maxPeakFreq = $best_freq with hitrate=$best_hitrate%"

# Restore original value
sed -i '' "s/maxPeakFreq = [0-9]*/maxPeakFreq = 21/" "$S3FIFO"
echo ""
echo "Restored maxPeakFreq to 21"
