# Benchmarks

Spec §22. Raw output lives in `bench/`: `compare.txt` (the §22.3 comparison, `-count=10`), `compare.summary.md` (medians and Mann-Whitney U test from `scripts/benchcmp.py`), `matrix.txt` (the §22.1 subset and write latency, `-count=1`), `memory.txt`.

Reproduce: `./scripts/bench.sh matrix` (set `TEMPORAL_BENCH_FULL=1` for the full matrix, `COUNT`/`BENCHTIME` to change repetitions).

**Environment:** Apple M4 (10 cores), 24 GB RAM, macOS (Darwin 24.6), Go 1.27.1, `-benchtime=200ms`, run on an otherwise idle machine on 2026-10-11. Value type for most benchmarks: a 32-byte trade record; capacity 65,536 (ring full, so every append evicts); event times with 1% disorder and 1% ties unless stated.

## 1. Time-index comparison (§22.3, mandatory)

Medians of 10 runs. `-benchmem` truncates allocs/op to an integer, so "<1" marks a fractional value. "p" is a two-sided Mann-Whitney U test on ns/op (p < 0.05 = significant difference).

| Operation | None ns/op | BTree ns/op | None records/s | BTree records/s | BTree allocs/op | p |
|---|---:|---:|---:|---:|---:|---:|
| Append | 25.6 | 95.9 | 39.1 M | 10.4 M | <1 (1 B/op amortized) | 0.000 |
| AppendBatch(8) | 271 | 1,454 | 29.5 M | 5.5 M | <1 (471 B/op) | 0.000 |
| AppendBatch(64) | 1,551 | 8,541 | 41.3 M | 7.5 M | 4 | 0.000 |
| AppendBatch(256) | 6,035 | 28,028 | 42.4 M | 9.1 M | 17 | 0.000 |
| AppendBatch(1024) | 21,175 | 103,298 | 48.4 M | 9.9 M | 68 | 0.000 |
| ReadFrom (256 entries) | 1,112 | 1,116 | 230 M | 229 M | 0 | **0.062** |
| Subscription.Next (256 entries) | 1,194 | 1,193 | 214 M | 215 M | 0 | **0.425** |
| AsOf | 6.4 (Disabled) | 152 | — | 6.6 M | 0 | — |
| Range (256 entries) | 15.9 (Disabled) | 2,222 | — | 115 M | 0 | — |

- **Q-017 holds:** ReadFrom and Subscription.Next show no statistically significant difference between modes (p = 0.06 and 0.43).
- **Cost of event-time indexing on ingest:** about 70 ns per record (Append 25.6 → 95.9 ns). Batching does not amortize it: the B-tree costs ~100 ns per record at every batch size, while the None path gets cheaper per record with larger batches.
- **B-tree write-path allocations:** ~0.066 allocations per record (68 per 1,024-record batch; about 33-38 B/record) from node churn: nodes are freed at the old end of the tree and allocated at the new end even at constant size (DECISIONS S7-05). The None path allocates nothing.
- For None, AsOf and Range measure the cost of returning `StatusTimeIndexDisabled`.

## 2. Ingestion matrix (§22.1 subset)

**Event-time disorder (BTree Append):** in-order 91.9 ns; 1% late 95.6 ns; 10% late 109 ns; adversarial reverse bursts 96.5 ns. 10% ties add 1-5 ns. Disorder costs at most ~20%.

**Value size and capacity (Append):**

| Mode | Capacity | 16 B | 64 B | 256 B |
|---|---:|---:|---:|---:|
| None | 1,024 | 25.7 | 26.7 | 29.7 |
| None | 65,536 | 25.7 | 27.2 | 29.9 |
| None | 100,000 (non-power-of-two) | 25.8 | 26.9 | 30.3 |
| BTree | 1,024 | 77.8 | 80.1 | 83.9 |
| BTree | 65,536 | 98.1 | 96.5 | 101.6 |
| BTree | 100,000 | 107.4 | 110.2 | 115.2 |

The non-power-of-two capacity (modulo instead of mask) costs nothing measurable. B-tree cost grows with depth (capacity).

**Key indexes (Append, ns/op):**

| Indexes | None | BTree |
|---|---:|---:|
| none | 26.2 | 96.4 |
| unique (ConflictDrop) | 97.7 | 173.7 |
| unique + multi, low cardinality (2 keys) | 132.2 | 208.1 |
| unique + multi, high cardinality (5,000 keys) | 143.8 | 223.5 |

A unique index costs ~70 ns per record (Go map insert + delete). No index configuration allocates in steady state (≤ 6 B/op amortized).

**Contended Append (10 goroutines):** None 104 ns/op, BTree 151 ns/op aggregate.

## 3. Queries (BTree series, 65,536 records)

| Query | ns/op | allocs |
|---|---:|---:|
| Get | 9.9 | 0 |
| Latest | 9.9 | 0 |
| UniqueIndex.Get | 35.3 | 0 |
| AsOf / Before / After | 132-134 | 0 |
| ReadFrom 256 | 1,057 (4.1 ns/record) | 0 |
| Range 256 | 2,263 (8.8 ns/record) | 0 |
| PrevN / NextN 256 | 2,798 / 2,354 | 0 |
| MultiIndex.Get 256 | 1,096 | 0 |
| ReadFrom 4,096 | 18,470 | 0 |
| Range 4,096 | 34,263 | 0 |
| Entries over 16,384 records (paged) | 179,913 (11 ns/record) | 4 |

With `nil` dst every batch read allocates exactly once (the result slice), as specified. Point event-time reads cost ~130 ns with random access, versus ~10 ns for Seq-addressed Get.

## 4. Writer latency under concurrent readers (§22.2)

Append latency percentiles while N reader goroutines copy batches **in a tight loop with no pause** (an adversarial load):

| Mode | Readers | Read size | p50 | p99 |
|---|---:|---:|---:|---:|
| None | 1 | 64 | 125 ns | 0.46 ms |
| None | 1 | 4,096 | 42 ns | 1.02 ms |
| None | 2 | 64 | 458 ns | 0.31 ms |
| None | 8 | 64 | 4.7 µs | 0.92 ms |
| None | 32 | 64 | 2.8 µs | 1.47 ms |
| None | 2-32 | 4,096 | ~1.05 ms | ~1.08 ms |
| BTree | 1 | 64 | 166 ns | 0.35 ms |
| BTree | 8 | 64 | 167 ns | 1.40 ms |
| BTree | 32 | 64 | 167 ns | 1.88 ms |
| BTree | 2-32 | 4,096 | ~1.08 ms | ~1.12 ms |

**Interpretation.** A single read of 4,096 entries holds the mutex for ~18-34 µs, yet the writer waits ~1 ms. That ~1 ms plateau is Go's `sync.Mutex` starvation threshold: in normal mode a goroutine that just unlocked can immediately re-lock, so readers spinning back-to-back keep winning, until the writer has waited 1 ms and the mutex switches to FIFO hand-off. With 64-entry reads, the median stays low but the tail still reaches 0.3-1.9 ms.

**What this means for a live trading system:**
- Readers that poll in a tight loop can stall ingestion by up to ~1 ms. Consumers should block on `Subscription.Next` or `Wake()` instead of polling, and strategy code should query once per event or decision, not continuously.
- A smaller `MaxReadBatch` (for example 256 instead of the default 4,096) shortens each hold but does not remove the unfairness under spinning readers.
- Spec §22.4 / §11.6 allow an experimental `sync.RWMutex` variant if benchmarks justify it. RWMutex gives a waiting writer priority over new readers, which targets exactly this tail. It requires per-call read scratch instead of the shared `keyScratch`. Not implemented in v1 (the spec's baseline is `sync.Mutex`); this table is the evidence for deciding.

## 5. Memory (§22.2)

Heap bytes per retained row, 32-byte value, capacity 262,144 (`TestMemoryPerRow`):

| Configuration | bytes/row | of which index |
|---|---:|---:|
| TimeIndexNone | 64.0 | — (32 value + 32 metadata) |
| TimeIndexBTree | 99.4 | ~35 (B-tree keys + nodes) |
| None + UniqueIndex[uint64] | 108.2 | ~44 (map entry + per-slot key + bit) |
| None + MultiIndex[int64], 5,000 keys | 92.8 | ~29 (postings + per-slot key + bit) |

**Sustained ingestion (T-009):** with BTree + unique index + a subscriber, HeapInuse is 12,672 KiB after warm-up and peaks at 12,720 KiB over 2 M further appends: bounded.

## 6. Allocation acceptance (§22.2)

`TestAllocAcceptance` asserts exactly zero allocations (exact counting, not `testing.AllocsPerRun`'s truncated integer) for Get, Latest, AsOf, Before, After, FirstAt, LastAt, UniqueIndex.Get, every batch read including Continue with sufficient `cap(dst)`, TimeIndexNone Append and AppendBatch with subscribers attached (notification), and subscriber drain. The B-tree write path's ~0.066 allocs/record is the permitted "amortized B-tree" exception.
