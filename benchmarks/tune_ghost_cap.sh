#!/bin/bash
# Binary search for optimal ghostCapPerMille across cache sizes
set -e

S3FIFO="$(dirname "$0")/../s3fifo.go"
SIZE="${1:-16}"
RESULTS_FILE="/tmp/ghost_cap_${SIZE}k.txt"

# Clear previous results
> "$RESULTS_FILE"

run_benchmark() {
    local cap=$1

    # Skip if already tested
    if grep -q "^$cap " "$RESULTS_FILE" 2>/dev/null; then
        return 0
    fi

    # Update ghostCapPerMille in s3fifo.go
    sed -i '' "s/ghostCapPerMille = [0-9]*/ghostCapPerMille = $cap/" "$S3FIFO"

    # Run benchmark - capture output
    output=$(SIZES=$SIZE SUITES=hitrate make benchmark 2>&1) || true

    # Extract the average from Category Summary line for multicache
    avg=$(echo "$output" | grep -E '^\s*\| multicache' | tail -1 | awk -F'|' '{print $3}' | grep -o '[0-9.]*')

    if [ -z "$avg" ]; then
        echo "cap=$cap: FAILED"
        return 1
    fi

    echo "cap=$cap ($(echo "scale=2; $cap/10" | bc)%): hitrate=$avg%"
    echo "$cap $avg" >> "$RESULTS_FILE"
}

echo "=== Tuning ghostCapPerMille for ${SIZE}K cache ==="
echo ""

# Initial coarse scan (500 = 0.5x to 2000 = 2.0x)
echo "=== Coarse Scan ==="
for cap in 500 750 1000 1220 1500 1750 2000; do
    run_benchmark $cap
done

echo ""
echo "=== Results So Far ==="
sort -k2 -rn "$RESULTS_FILE" | head -10

# Find best from initial scan
best_line=$(sort -k2 -rn "$RESULTS_FILE" | head -1)
best_cap=$(echo "$best_line" | awk '{print $1}')

echo ""
echo "Best so far: cap=$best_cap"

# Fine-tune around best value
echo ""
echo "=== Fine-tuning around $best_cap ==="
low=$((best_cap - 300))
high=$((best_cap + 300))
[ $low -lt 100 ] && low=100

for cap in $(seq $low 50 $high); do
    run_benchmark $cap
done

echo ""
echo "=== Final Results (sorted by hitrate) ==="
sort -k2 -rn "$RESULTS_FILE" | head -15

# Find the actual best
best_line=$(sort -k2 -rn "$RESULTS_FILE" | head -1)
best_cap=$(echo "$best_line" | awk '{print $1}')
best_hitrate=$(echo "$best_line" | awk '{print $2}')

echo ""
echo "=== OPTIMAL for ${SIZE}K ==="
echo "ghostCapPerMille = $best_cap ($(echo "scale=2; $best_cap/10" | bc)%) with hitrate=$best_hitrate%"

# Restore original value
sed -i '' "s/ghostCapPerMille = [0-9]*/ghostCapPerMille = 1220/" "$S3FIFO"
echo ""
echo "Restored ghostCapPerMille to 1220"
