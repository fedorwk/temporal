#!/usr/bin/env bash
# Runs the §22.3 None-vs-BTree comparison and writes raw results to
# bench/compare.txt. Uses benchstat when installed
# (go install golang.org/x/perf/cmd/benchstat@latest); otherwise falls back to
# scripts/benchcmp.py (medians and a Mann-Whitney U test, no dependencies).
#
#   ./scripts/bench.sh            # comparison only
#   ./scripts/bench.sh matrix     # also the §22.1 matrix subset and write latency
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p bench

COUNT=${COUNT:-10}
BENCHTIME=${BENCHTIME:-200ms}

go test -run '^$' -bench '^BenchmarkCompare_' -benchmem -count "$COUNT" -benchtime "$BENCHTIME" \
  -timeout 60m . | tee bench/compare.txt

if command -v benchstat >/dev/null 2>&1; then
  benchstat -col /mode bench/compare.txt | tee bench/compare.benchstat.txt
else
  python3 scripts/benchcmp.py bench/compare.txt | tee bench/compare.summary.md
fi

if [[ "${1:-}" == "matrix" ]]; then
  go test -run '^$' -bench '^Benchmark(Matrix_|WriteLatency)' -benchmem -count 1 -benchtime "$BENCHTIME" \
    -timeout 60m . | tee bench/matrix.txt
  go test -run 'TestMemoryPerRow|TestSustainedIngestionHeapBounded' -v -count 1 . | tee bench/memory.txt
fi
