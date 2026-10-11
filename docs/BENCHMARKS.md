# Benchmarks

Spec §22. Raw output lives in `bench/`: `compare.txt` (the §22.3 comparison, `-count=10`), `compare.summary.md` (medians and Mann-Whitney U test from `scripts/benchcmp.py`), `matrix.txt` (the §22.1 subset and write latency, `-count=1`), `memory.txt`.

The raw files are from the 2026-10-11 re-run that added TimeIndexOrdered. Tables and figures not marked as re-measured keep the first run's numbers; the re-run matches them within a few percent, except the write-latency p50s in §4, which vary widely between runs.

Reproduce: `./scripts/bench.sh matrix` (set `TEMPORAL_BENCH_FULL=1` for the full matrix, `COUNT`/`BENCHTIME` to change repetitions).

**Environment:** Apple M4 (10 cores), 24 GB RAM, macOS (Darwin 24.6), Go 1.27.1, `-benchtime=200ms`, run on an otherwise idle machine on 2026-10-11. Value type for most benchmarks: a 32-byte trade record; capacity 65,536 (ring full, so every append evicts); event times with 1% disorder and 1% ties unless stated.

## 1. Time-index comparison (§22.3, mandatory)

Medians of 10 runs, re-measured on 2026-10-11 with all three modes in one run (the None and BTree numbers match the first run within noise). `-benchmem` truncates allocs/op to an integer, so "<1" marks a fractional value. "p" is a two-sided Mann-Whitney U test on ns/op against None (p < 0.05 = significant difference). TimeIndexOrdered runs on the same streams without the 1% late records, which it would reject; each pass over the stream is shifted forward so time never goes back, and every append benchmark fails if any record is rejected.

| Operation | None ns/op | BTree ns/op | Ordered ns/op | BTree allocs/op | Ordered allocs/op | p BTree | p Ordered |
|---|---:|---:|---:|---:|---:|---:|---:|
| Append | 25.7 | 95.4 | 27.0 | <1 (1 B/op amortized) | 0 | 0.000 | 0.000 |
| AppendBatch(8) | 152 | 853 | 166 | <1 (396 B/op) | 0 | 0.000 | 0.000 |
| AppendBatch(64) | 1,181 | 6,391 | 1,275 | 4 | 0 | 0.000 | 0.000 |
| AppendBatch(256) | 4,680 | 25,221 | 4,907 | 17 | 0 | 0.000 | 0.000 |
| AppendBatch(1024) | 18,696 | 98,400 | 19,695 | 68 | 0 | 0.000 | 0.000 |
| ReadFrom (256 entries) | 1,114 | 1,116 | 1,108 | 0 | 0 | **0.322** | 0.000 (-0.6%) |
| Subscription.Next (256 entries) | 1,168 | 1,167 | 1,159 | 0 | 0 | **0.300** | 0.000 (-0.8%) |
| AsOf | 6.4 (Disabled) | 154 | 115 | 0 | 0 | — | — |
| Range (256 entries) | 16.5 (Disabled) | 2,228 | 1,495 | 0 | 0 | — | — |

- **Q-017 holds:** ReadFrom and Subscription.Next do not differ between None and BTree (p = 0.32 and 0.30). Ordered measures 0.6-0.8% *faster* with p < 0.05; it runs the identical code path (Seq reads never touch the time index), so this is drift between consecutive runs, not an effect of the mode.
- **Cost of event-time indexing on ingest:** the B-tree costs about 70 ns per record (Append 25.7 → 95.4 ns), and batching does not amortize it (~96-100 ns per record at every batch size). TimeIndexOrdered costs about 1.3 ns per record (+5%) on Append and +5-10% in batches, because it stores nothing and only moves two counters per transaction.
- **Event-time reads:** TimeIndexOrdered answers AsOf in 115 ns (BTree 154 ns) and a 256-entry Range in 1.5 µs (BTree 2.2 µs): a binary search over the ring's metadata needs no pointer chasing, and the scan reads consecutive slots.
- The AppendBatch rows of the first run were re-measured after fixing the benchmark: the first version paused the timer every iteration, and with `ReportAllocs` each pause reads memory statistics, inflating small batches (8 records: 271 ns reported, 153 ns real).
- **B-tree write-path allocations:** ~0.066 allocations per record (68 per 1,024-record batch; about 33-38 B/record) from node churn: nodes are freed at the old end of the tree and allocated at the new end even at constant size (DECISIONS S7-05). The None and Ordered paths allocate nothing.
- For None, AsOf and Range measure the cost of returning `StatusTimeIndexDisabled`.

## 2. Ingestion matrix (§22.1 subset)

**Event-time disorder (BTree Append):** in-order 91.9 ns; 1% late 95.6 ns; 10% late 109 ns; adversarial reverse bursts 96.5 ns. 10% ties add 1-5 ns. Disorder costs at most ~20%.

**Value size and capacity (Append):**

| Mode | Capacity | 16 B | 64 B | 256 B |
|---|---:|---:|---:|---:|
| None | 1,024 | 25.6 | 26.0 | 29.4 |
| None | 65,536 | 25.5 | 26.2 | 29.7 |
| None | 100,000 (non-power-of-two) | 25.6 | 26.2 | 29.7 |
| BTree | 1,024 | 78.8 | 79.2 | 84.0 |
| BTree | 65,536 | 95.5 | 96.2 | 104.2 |
| BTree | 100,000 | 98.0 | 99.1 | 103.4 |
| Ordered | 1,024 | 26.6 | 28.1 | 31.1 |
| Ordered | 65,536 | 26.7 | 32.3 | 31.4 |
| Ordered | 100,000 | 26.9 | 28.1 | 31.4 |

(Single runs from the matrix, re-measured on 2026-10-11; the 65,536 / 64 B Ordered cell is a one-run outlier.)

The non-power-of-two capacity (modulo instead of mask) costs nothing measurable. B-tree cost grows with depth (capacity); Ordered does not depend on capacity.

**Key indexes (Append, ns/op):**

| Indexes | None | BTree | Ordered |
|---|---:|---:|---:|
| none | 26.4 | 95.6 | 27.1 |
| unique (ConflictDrop) | 100.8 | 177.1 | 102.9 |
| unique + multi, low cardinality (2 keys) | 127.1 | 204.0 | 130.0 |
| unique + multi, high cardinality (5,000 keys) | 142.7 | 222.6 | 144.1 |

A unique index costs ~70 ns per record (Go map insert + delete). No index configuration allocates in steady state (≤ 6 B/op amortized).

**Contended Append (10 goroutines):** None 102 ns/op, BTree 150 ns/op aggregate, Ordered 95 ns/op (Ordered uses one constant EventTime because goroutines racing on a shared counter would arrive out of order; the others use the counter, an extra contended atomic).

## 3. Queries (BTree series, 65,536 records)

Single runs, re-measured on 2026-10-11.

| Query | ns/op | allocs |
|---|---:|---:|
| Get | 9.9 | 0 |
| Latest | 9.8 | 0 |
| UniqueIndex.Get | 35.1 | 0 |
| AsOf / Before / After | 132-133 | 0 |
| ReadFrom 256 | 1,134 (4.4 ns/record) | 0 |
| Range 256 | 2,209 (8.6 ns/record) | 0 |
| PrevN / NextN 256 | 2,299 / 2,209 | 0 |
| MultiIndex.Get 256 | 1,184 | 0 |
| ReadFrom 4,096 | 18,714 | 0 |
| Range 4,096 | 33,250 | 0 |
| Entries over 16,384 records (paged) | 181,464 (11 ns/record) | 4 |

With `nil` dst every batch read allocates exactly once (the result slice), as specified. Point event-time reads cost ~130 ns with random access, versus ~10 ns for Seq-addressed Get.

This matrix runs on a BTree series only. For TimeIndexOrdered, §1 has AsOf (115 ns vs 154 ns) and Range 256 (1.5 µs vs 2.2 µs); PrevN, NextN and AtAll use the same search and scan primitives.

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
| Ordered | 1 | 64 | 84 ns | 0.43 ms |
| Ordered | 8 | 64 | 7.8 µs | 1.17 ms |
| Ordered | 32 | 64 | 84 ns | 1.42 ms |
| Ordered | 2-32 | 4,096 | 84 ns - 1.06 ms | ~1.08 ms |

The Ordered rows come from the 2026-10-11 re-run (single runs; p50 under spinning readers is noisy in every mode). Ordered behaves like the other modes: the tail is set by the mutex, not the index.

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
| TimeIndexOrdered | 64.3 | 0 (searches the ring itself) |
| None + UniqueIndex[uint64] | 108.2 | ~44 (map entry + per-slot key + bit) |
| None + MultiIndex[int64], 5,000 keys | 92.8 | ~29 (postings + per-slot key + bit) |

**Sustained ingestion (T-009):** with BTree + unique index + a subscriber, HeapInuse is 12,672 KiB after warm-up and peaks at 12,720 KiB over 2 M further appends: bounded.

## 6. Allocation acceptance (§22.2)

`TestAllocAcceptance` asserts exactly zero allocations (exact counting, not `testing.AllocsPerRun`'s truncated integer) for Get, Latest, AsOf, Before, After, FirstAt, LastAt, UniqueIndex.Get, every batch read including Continue with sufficient `cap(dst)`, TimeIndexNone Append and AppendBatch with subscribers attached (notification), and subscriber drain. The B-tree write path's ~0.066 allocs/record is the permitted "amortized B-tree" exception. TimeIndexOrdered Append and AppendBatch allocate nothing (`TestAllocs_AppendByMode`, `TestAllocs_AppendBatch`), nor do its lookups, scans and Apply (`TestOrderedTimeIndex_Allocs`).
