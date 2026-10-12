# temporal

An in-memory, bounded, time-aware series store for Go, built for live market
data: trades, quotes and order-book updates arriving at high frequency.

```go
import "github.com/fedorwk/temporal"
```

Requires Go 1.23 or newer. The only dependency is
[tidwall/btree](https://github.com/tidwall/btree), used for the optional
event-time index.

## What it is

A `Series[T]` keeps the newest `Capacity` records of one stream in a ring
buffer. Appending to a full series evicts the oldest record. On top of that
single store you get:

| Capability | What it gives you |
|---|---|
| **Arrival-order log** | Every record gets a `Seq`; replay with `ReadFrom` or follow live with a `Subscription`. |
| **Event-time queries** (optional) | `AsOf`, `Range`, `PrevN`, `NextN`, `Entries`, ... ordered by `(EventTime, Seq)`. Off by default; nearly free for in-order feeds. |
| **Key indexes** (optional) | Unique (`trade_id`) and multi-valued (`symbol`) lookups, with idempotent re-ingestion. |
| **Late-data handling** | Out-of-order records are accepted and flagged, or rejected, per policy. |
| **Watermarks** | A producer declares "complete through T"; reads can be gated so consumers never see non-final answers. |
| **Subscriptions** | Cursor-based readers with coalesced wake-ups; slow readers get a typed lag error instead of blocking producers. |
| **Explicit statuses** | Every empty answer says why: not found, expired, reset, not ready, index disabled, invalid. |

It is a library, not a service: no goroutines of its own, no persistence, no
network. Derived series (bars, indicators) are loops you own; the library
provides the one primitive that is hard to build from outside
(`Subscription.NextState`).

### Design in one paragraph

One `sync.Mutex` per series guards everything. Reads copy their results out
under the lock and return caller-owned memory, so nothing you hold can be
overwritten by a later append. Subscribers are woken only after the lock is
released, and wake-ups are coalesced, so a stuck consumer can never block the
producer. A failed `Append` changes no state, and panics from your callbacks
(`Clone`, key extractors) pass through to you and leave the series intact.

## Quick start

```go
type Trade struct {
	ID    uint64
	Price int64
	Qty   int64
}

trades, err := temporal.New(temporal.Config[Trade]{
	Name:      "btcusdt.trades",
	Capacity:  1 << 20,                 // retain the newest ~1M trades
	TimeIndex: temporal.TimeIndexBTree, // enable event-time queries
})
if err != nil {
	log.Fatal(err)
}

now := temporal.FromTime(time.Now())

res, err := trades.Append(
	Trade{ID: 1, Price: 104_250, Qty: 3},
	temporal.InputMeta{
		EventTime:   now,        // venue timestamp (required)
		ReceiveTime: now + 1500, // local observation time
		SourceSeq:   1,          // feed sequence number, 0 if none
		SourceEpoch: 1,          // feed session, bump on reconnect
	},
)
// res.Ref identifies the record: Ref{Gen: 1, Seq: 1}

e, st := trades.AsOf(now + 1_000_000)         // latest trade at or before t
if st == temporal.StatusOK {
	fmt.Println(e.Value.Price, e.Meta.Ref)
}
```

Timestamps are `int64` Unix nanoseconds, UTC. `temporal.FromTime` and
`Timestamp.Time` convert from and to `time.Time`. `MinTimestamp` and
`MaxTimestamp` are reserved.

## Core concepts

### Identity: Seq, Gen, Ref

- **Seq** is a strictly increasing ingest number, starting at 1. It defines
  arrival order; storage, replay and subscriptions all follow it.
- **Gen** is the generation. It starts at 1 and increments on every `Clear`,
  which also restarts Seq.
- **Ref** `{Gen, Seq}` is a record's public identity. `Get(ref)` tells you
  whether the record is there (`OK`), was evicted (`Expired`), belongs to an
  earlier generation (`Reset`) or never existed (`NotFound`).

### Time

Each record carries `EventTime` (venue time, used for ordering queries),
`ReceiveTime` (local observation), `SourceSeq`/`SourceEpoch` (the feed's own
sequence and session) and `Flags`.

A record is **late** if its `EventTime` is below the highest event time seen
in this generation minus `Config.LateTolerance`. Late records are always
flagged `FlagLate`; `LatePolicy` decides whether they are stored
(`LateAccept`, the default) or rejected (`LateReject` -> `ErrLate`).

### Event-time ordering is opt-in

| `TimeIndex` | Append cost | Event-time queries |
|---|---:|---|
| `TimeIndexNone` (default) | ~26 ns | return `StatusTimeIndexDisabled`; never fall back to a scan |
| `TimeIndexOrdered` | ~27 ns | enabled; `EventTime` must never decrease |
| `TimeIndexBTree` | ~95 ns | enabled; records may arrive in any order |

Seq reads, key lookups and subscriptions work identically in every mode. Use
`None` for relays and raw capture. Where strategy code asks "what was the
price at time T", use `Ordered` for a feed that arrives in event-time order
(one exchange stream, your own snapshots or bars) and `BTree` when records can
arrive out of order (merged feeds, late corrections).

`TimeIndexOrdered` keeps no extra structure: the ring is already sorted by
event time, so queries binary-search it directly and return exactly what
`BTree` would. It requires `LatePolicy: LateReject` with `LateTolerance: 0`
(`New` fails otherwise), so a record older than the newest accepted one fails
with `ErrLate`; equal event times are fine.

Among records with equal `EventTime`, ingest order decides: `AsOf` returns the
latest ingested, `FirstAt` the earliest.

### Statuses

Point reads return `(Entry[T], Status)`; batch reads return a `ReadInfo`.

| Status | Meaning |
|---|---|
| `StatusOK` | Data returned. |
| `StatusNotFound` | Nothing retained matches, and nothing evicted could have. |
| `StatusExpired` | Nothing retained matches, but an evicted record did or may have. |
| `StatusReset` | The `Ref` or cursor is from before a `Clear`. |
| `StatusNotReady` | Beyond the watermark (with `GateReadsByWatermark`). |
| `StatusTimeIndexDisabled` | Event-time query on a `TimeIndexNone` series. |
| `StatusInvalid` | Zero or foreign cursor, unbound index handle, bad argument. |

`Status.Err()` maps a status to a sentinel error for `errors.Is`.

## API overview

Full reference: `go doc -all github.com/fedorwk/temporal`, or the package
documentation on pkg.go.dev.

### Construction

| | |
|---|---|
| `New[T](Config[T]) (*Series[T], error)` | Create a series; `ErrInvalidConfig` on bad config. |
| `Config.Capacity` | Required, >= 1. |
| `Config.TimeIndex` | `TimeIndexNone` (default), `TimeIndexOrdered` or `TimeIndexBTree`. Immutable. |
| `Config.Indexes` | Key indexes, see below. |
| `Config.Clone`, `CloneMode` | Deep-copy hook for values with slices, maps or pointers. |
| `Config.LatePolicy`, `LateTolerance` | Late-record handling. |
| `Config.SourceSeqPolicy` | `Ignore`, `Flag` or `Reject` non-increasing `SourceSeq` within an epoch. |
| `Config.GateReadsByWatermark` | Only expose final answers; see Watermarks. |
| `Config.MaxReadBatch` | Upper bound per batch read (default 4096); bounds lock hold time. |

### Writes

| | |
|---|---|
| `Append(v, InputMeta) (AppendResult, error)` | Store one record. |
| `AppendBatch(dst, []Input[T]) ([]AppendResult, error)` | Atomic: all or none; one subscriber wake-up. Behaves exactly like the same `Append`s in order. |
| `AppendAll(dst, []Input[T])` | Any number of items, in `AppendBatch` chunks of `min(Capacity, MaxReadBatch)`. Atomic per chunk, not overall. |
| `SetWatermark(ts) bool` | Monotonic completeness marker. |
| `WaitWatermark(ctx, ts)` | Block until the series is complete through `ts` (or closed, or ctx done). |
| `Clear() error` | Drop everything, start a new generation. |
| `Close() error` | Stop writes; reads keep working. |
| `CloseWithError(cause) error`, `Err()` | Close and record why; writers and drained subscribers get a `*ClosedError` matching `ErrClosed` and `cause`. |

`AppendResult` carries the `Outcome` (`Appended` / `Dropped`), the assigned
`Ref`, applied flags, the `Evicted` ref and, for key conflicts, the
`Existing` holder.

### Reads by arrival order

| | |
|---|---|
| `Latest()`, `Get(ref)` | Newest record, or one by `Ref`. |
| `ReadFrom(dst, from, limit)` | Replay in Seq order. Continue with `Ref{Gen, LastRef.Seq+1}`. |
| `OldestRef()`, `NewestRef()`, `Len()`, `Cap()`, `Gen()` | Bounds and size. |

### Reads by event time (`TimeIndexOrdered` or `TimeIndexBTree`)

| | |
|---|---|
| `AsOf(ts)`, `Before(ts)`, `After(ts)`, `AtOrAfter(ts)` | Nearest record around `ts`. |
| `FirstAt(ts)`, `LastAt(ts)`, `AtAll(dst, ts, limit)` | Records with exactly `EventTime == ts`. |
| `PrevN(dst, ts, n)`, `NextN(dst, ts, n)` | The `n` records before / after `ts`. |
| `Range(dst, from, to, limit)`, `RangeValues(...)` | `from <= EventTime < to`; truncated reads return a `TimeCursor`. |
| `Continue(dst, cursor, limit)`, `ContinueValues(...)` | Resume a truncated range strictly after the last returned record. |
| `Entries(from, to)` | `iter.Seq` over a range, paged; the loop body may call any method. |
| `LatestByEventTime()` | Record with the greatest event time. |

Batch reads append to a caller-provided `dst` (like `append`) and do not
allocate when `cap(dst)` suffices.

### Key indexes

| | |
|---|---|
| `NewUniqueIndex(name, extractor, ConflictPolicy)` | One record per key. `Get`, `Contains`, `Len`. |
| `NewMultiIndex(name, extractor)` | Many records per key, in Seq order. `Get(dst, key, limit, order)`, `Latest`, `Count`, `Keys`. |

Unique conflict policies: `ConflictReject` (fail the append),
`ConflictDrop` (skip it; idempotent re-ingestion), `ConflictReplace` (move the
key to the new record), `ConflictKeepFirst` (store the record without indexing
it).

### Subscriptions

| | |
|---|---|
| `Subscribe(SubscribeOptions) (*Subscription[T], error)` | `Start`: `StartNext()` (default), `StartOldest()`, `StartAt(ref)`. `OnLag`: `LagFail` or `LagSkip`. `WatermarkUpdates`: also wake and return on watermark advances. |
| `Next(ctx, dst, limit)` | Block until records are available. |
| `NextState(ctx, dst, limit)` | `Next` plus a `SourceState` read under the same lock. |
| `All(ctx, limit)` | `iter.Seq2[Entry[T], error]` over `Next`; ends silently after a clean close, yields the error otherwise. |
| `TryNext(dst, limit)`, `TryNextState(dst, limit)`, `Wake()` | Non-blocking variants and a coalesced wake channel for `select` loops over several subscriptions. |
| `Seek(StartFrom)`, `Cursor()`, `Skipped()`, `Close()` | Reposition, inspect, release. |

A subscription must be used by one goroutine at a time.

### Observability

`Stats()` returns counters (appended, evicted, dropped, rejected, late, gaps,
clears), bounds, min/max retained event time and per-index `IndexStats`.
`CheckInvariants()` verifies internal consistency (useful in tests).

### Errors

All sentinels are prefixed `temporal:` and work with `errors.Is`:
`ErrNotFound`, `ErrExpired`, `ErrReset`, `ErrNotReady`,
`ErrTimeIndexDisabled`, `ErrLagged`, `ErrClosed`, `ErrInvalidConfig`,
`ErrInvalidInput`, `ErrLate`, `ErrSourceSeqNotIncreasing`,
`ErrUniqueConflict`, `ErrBatchTooLarge`, `ErrSeqExhausted`,
`ErrGenExhausted`, `ErrIndexBound`.

Typed errors: `*LagError` (`Missed`, `Oldest`, `Cursor`),
`*ConflictError` (`Index`, `Key`, `Existing`, `BatchPos`) and `*ClosedError`
(`Cause`, also matched by `errors.Is(err, cause)`); use `errors.As`.

## Usage examples

### Idempotent ingestion after a reconnect

```go
byID := temporal.NewUniqueIndex("trade_id",
	func(e *temporal.Entry[Trade]) (uint64, bool) { return e.Value.ID, true },
	temporal.ConflictDrop,
)

trades, _ := temporal.New(temporal.Config[Trade]{
	Capacity:  1 << 20,
	TimeIndex: temporal.TimeIndexBTree,
	Indexes:   []temporal.Index[Trade]{byID},
})

// Replaying a feed segment after a reconnect is safe: duplicates are dropped.
res, _ := trades.Append(Trade{ID: 7}, temporal.InputMeta{EventTime: ts})
res, _ = trades.Append(Trade{ID: 7}, temporal.InputMeta{EventTime: ts})
fmt.Println(res.Outcome == temporal.OutcomeDropped, res.Existing) // true {1 1}

t, st := byID.Get(7) // O(1) lookup by trade id
```

An index handle binds to exactly one series (`ErrIndexBound` otherwise).
Extractors run before anything is mutated; keep them pure and never call
series methods from them.

### Batch ingestion

```go
items := []temporal.Input[Trade]{
	{Value: Trade{ID: 1}, Meta: temporal.InputMeta{EventTime: t0}},
	{Value: Trade{ID: 2}, Meta: temporal.InputMeta{EventTime: t0 + 10}},
}
results, err := trades.AppendBatch(resultsBuf[:0], items)
```

The batch is atomic: if any item fails (for example `ConflictReject`), nothing
is applied, and a `*ConflictError` names the failing position in `BatchPos`.
Reusing `resultsBuf` keeps the path allocation-free for `TimeIndexNone` and
`TimeIndexOrdered`.
A batch larger than `Capacity` fails with `ErrBatchTooLarge`.

### Event-time queries

```go
// Last price at or before a decision time.
e, st := trades.AsOf(decisionTime)

// The 100 trades up to now, oldest first.
buf := make([]temporal.Entry[Trade], 0, 128)
buf, info := trades.PrevN(buf[:0], now, 100)

// A window, paged with a cursor.
buf, info = trades.Range(buf[:0], from, to, 1000)
for !info.Next.IsZero() {
	buf, info = trades.Continue(buf, info.Next, 1000) // appends to buf
}

// Or iterate; the body may call other series methods.
seq, st := trades.Entries(from, to)
if st == temporal.StatusOK {
	for e := range seq {
		_ = e
	}
}
```

When a query comes back empty, `Status` (or `ReadInfo.Status`) tells you
whether the data never existed (`NotFound`) or has been evicted (`Expired`),
which matters when sizing `Capacity`.

### Following a stream with a subscription

```go
sub, err := trades.Subscribe(temporal.SubscribeOptions{
	Start: temporal.StartOldest(),
	OnLag: temporal.LagFail,
})
if err != nil {
	return err
}
defer sub.Close()

buf := make([]temporal.Entry[Trade], 0, 256)
for {
	var err error
	buf, err = sub.Next(ctx, buf[:0], 256)
	switch {
	case err == nil:
		for _, e := range buf {
			handle(e)
		}
	case errors.Is(err, temporal.ErrLagged):
		var lag *temporal.LagError
		errors.As(err, &lag)
		log.Printf("missed %d records", lag.Missed)
		sub.Seek(temporal.StartOldest()) // resynchronize
	case errors.Is(err, temporal.ErrReset):
		sub.Seek(temporal.StartOldest()) // the series was cleared
	default:
		return err // context cancelled, series closed
	}
}
```

`LagSkip` instead jumps ahead automatically and counts what it skipped in
`sub.Skipped()`. For `select`-based loops use `Wake()` with `TryNext`.

### Values that contain slices: Clone

Records are copied by value. If `T` holds slices, maps or pointers, supply a
`Clone` function, for example an order-book update with a levels slice:

```go
type BookUpdate struct {
	Bids, Asks []Level
}

book, _ := temporal.New(temporal.Config[BookUpdate]{
	Capacity: 1 << 16,
	Clone: func(u BookUpdate) BookUpdate {
		return BookUpdate{
			Bids: append([]Level(nil), u.Bids...),
			Asks: append([]Level(nil), u.Asks...),
		}
	},
	CloneMode: temporal.CloneOnWrite, // producer may reuse its buffers
})
```

| `CloneMode` | Clones on | Use when |
|---|---|---|
| `CloneOnRead` (default) | every value copied out | producer hands over immutable data |
| `CloneOnWrite` | once, before storing | producer reuses its buffers |
| `CloneOnReadWrite` | both | strictest isolation |

### Watermarks and gated reads

A watermark says "every record with `EventTime <= W` has been seen". The
single owner of a series (feed adapter or derive loop) advances it; it is
monotonic.

```go
s, _ := temporal.New(temporal.Config[Trade]{
	Capacity:             1 << 16,
	TimeIndex:            temporal.TimeIndexBTree,
	GateReadsByWatermark: true,
})

s.Append(Trade{ID: 1}, temporal.InputMeta{EventTime: 100})
_, st := s.AsOf(100) // NotReady: a late record could still land at <= 100

s.SetWatermark(100)
_, st = s.AsOf(100) // OK, and final
```

With gating on, backward reads beyond the watermark return `StatusNotReady`,
and forward reads (`After`, `AtOrAfter`, `NextN`) return only records at or
below it. A watermark of `MaxTimestamp` declares the series complete (useful
for historical replay). Watermark changes do not wake subscribers; to wait for
completeness, use `WaitWatermark`:

```go
// Read the final value at t from a series another goroutine is still filling.
if _, err := s.WaitWatermark(ctx, t); err != nil {
	return err // ctx done, or the series closed before reaching t
}
e, st := s.AsOf(t)
```

### Derived series (bars, indicators)

Derived series are a loop you own. `NextState` returns the new records
together with the source's newest record and watermark, captured in one
atomic read, so you know exactly when you have caught up. With
`WatermarkUpdates` it also returns, with no records, when only the watermark
moved:

```go
func deriveNotional(ctx context.Context, src *temporal.Series[Trade], dst *temporal.Series[int64]) error {
	// WatermarkUpdates: also return when only the watermark moved, so
	// completeness declared after the last record is forwarded at once.
	sub, err := src.Subscribe(temporal.SubscribeOptions{Start: temporal.StartOldest(), WatermarkUpdates: true})
	if err != nil {
		return err
	}
	defer sub.Close()

	in := make([]temporal.Entry[Trade], 0, 256)
	out := make([]temporal.Input[int64], 0, 256)
	var res []temporal.AppendResult
	var last temporal.Ref
	for {
		var st temporal.SourceState
		in, st, err = sub.NextState(ctx, in[:0], 256)
		if err != nil {
			return err // ctx done, ErrClosed, ErrReset, or *LagError
		}
		out = out[:0]
		for _, e := range in {
			out = append(out, temporal.Input[int64]{
				Value: e.Value.Price * e.Value.Qty,
				Meta:  temporal.InputMeta{EventTime: e.Meta.EventTime},
			})
		}
		if len(in) > 0 {
			if res, err = dst.AppendBatch(res[:0], out); err != nil {
				return err // nothing was applied; retry from the same position
			}
			last = in[len(in)-1].Meta.Ref
		}
		if last == st.Newest {
			dst.SetWatermark(st.Watermark) // caught up: forward completeness
		}
	}
}
```

Because the loop is yours, so are goroutine lifetime, retries, checkpointing
and panic policy. The runnable version with a persisted checkpoint is
`ExampleSubscription_NextState` in `example_test.go`.

## Concurrency and ownership

- Every method of `Series` is safe for concurrent use. A `Subscription` is
  for one goroutine at a time (`Close` may be called from any).
- Reads return copies. Nothing returned is overwritten by later appends.
  Shallow copies do not copy what slices, maps or pointers inside `T` refer
  to; use `Clone` for those.
- `Clone` and key extractors run under the series lock. They must be fast and
  must **not** call any method of the same series (the mutex is not
  reentrant).
- **Panics are yours.** The library does not recover them. A panic in `Clone`
  or an extractor propagates out of `Append`/`AppendBatch`; if you recover it,
  the series is exactly as it was and keeps working.
- Readers that poll in a tight loop can delay a writer by up to about 1 ms
  (Go mutex starvation threshold). Block on `Next` or `Wake()` rather than
  spinning. Details in `docs/BENCHMARKS.md` section 4.

## Performance

Apple M4, Go 1.27.1, 32-byte value, capacity 65,536 (ring full, every append
evicts). Medians of 10 runs.

| Operation | `TimeIndexNone` | `TimeIndexOrdered` | `TimeIndexBTree` |
|---|---:|---:|---:|
| `Append` | 25.7 ns (39 M rec/s) | 27.0 ns (37 M rec/s) | 95.4 ns (10.5 M rec/s) |
| `AppendBatch(256)` | 4.7 us (55 M rec/s) | 4.9 us (52 M rec/s) | 25.2 us (10 M rec/s) |
| `AsOf` | n/a | 115 ns | 154 ns |
| `Range` (256 entries) | n/a | 1.5 us | 2.2 us |
| `ReadFrom` (256 entries) | 1.11 us | 1.11 us | 1.12 us |
| Memory per retained row | 64 B | 64 B | 99 B |

- Steady-state point reads and batch reads (with sufficient `cap(dst)`)
  allocate nothing; `Append`/`AppendBatch` allocate nothing with
  `TimeIndexNone` or `TimeIndexOrdered` and no key indexes.
- The B-tree costs about 70 ns per record on ingest; `TimeIndexOrdered` about
  1-2 ns. Neither affects reads by Seq or subscriptions.
- A unique key index adds roughly 70 ns per append (map operations) and about
  44 bytes per retained row.

Reproduce with `./scripts/bench.sh`; raw results are in `bench/`, analysis in
[docs/BENCHMARKS.md](docs/BENCHMARKS.md).

## Choosing capacity and modes

- **Capacity** is a count of records, not a duration. Size it for the longest
  lookback you need at the peak event rate; `StatusExpired` tells you when a
  query reached past it.
- **Memory** is about `Capacity x (sizeof(T) + 32)` bytes for
  `TimeIndexNone` and `TimeIndexOrdered`, plus ~35 B/row for the B-tree and
  ~30-45 B/row per key index, plus whatever `T` points to.
- **One series per stream** (per symbol and kind). A series is one mutex;
  sharding by stream keeps writers independent.

## Development

```bash
./scripts/check.sh    # gofmt, vet, tests (plain, -race, -tags temporaldebug)
./scripts/bench.sh    # benchmarks and the time-index mode comparison
```

Testing includes a reference-model property test, a fuzz target
(`go test -run='^$' -fuzz=FuzzSeries`), exact allocation assertions and
`-tags temporaldebug` builds that enable internal invariant checks.

Project documents live in [docs/](docs/):

| File | Contents |
|---|---|
| [temporal_series_requirements.md](docs/temporal_series_requirements.md) | The specification. |
| [IMPLEMENTATION_PLAN.md](docs/IMPLEMENTATION_PLAN.md) | Session plan and acceptance criteria. |
| [DECISIONS.md](docs/DECISIONS.md) | Design decisions and deviations from the spec, with rationale. |
| [PROGRESS.md](docs/PROGRESS.md) | Implementation log and requirement-to-test audit. |
| [BENCHMARKS.md](docs/BENCHMARKS.md) | Benchmark results and interpretation. |

## Status

All planned sessions through benchmarking are complete. Two deliberate
departures from the spec are recorded in DECISIONS.md (S12-01: panics pass
through; S12-02: derivation is caller-owned). Replay of captured real-feed
data (planned Session 12) is not done yet.
