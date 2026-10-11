---
title: "Temporal Series Engine - Technical and Functional Requirements / Architecture Specification"
version: "0.3"
date: "2026-10-07"
status: "Implementation specification"
audience: "Implementation agents and maintainers"
supersedes: "0.2 (2026-10-05)"
---

Temporal Series Engine
Technical and Functional Requirements / Architecture Specification

A bounded, typed, in-process time-series core for market data and derived series

Revision 0.3, 7 October 2026

Scope: one strongly typed Series[T] with a package-owned bounded ring, an optional and swappable event-time ordering capability, optional typed key indexes, a single mutex as the only consistency boundary, explicit query statuses, and Seq-based replay and subscriptions that work independently of event-time indexing.

Normative language: MUST, MUST NOT, SHOULD, SHOULD NOT and MAY are used as defined in RFC 2119. Functional requirements (tables with IDs) are primary. Struct layouts, interface shapes, method names and signatures in code blocks are the recommended realization and MAY be changed if every functional requirement still holds.

# 1. Executive Summary

This specification defines a new, self-contained Go package for a bounded, in-process, generic time series. Every component, including the bounded storage ring, is implemented from scratch inside the package; there is no dependency on, or compatibility obligation toward, any pre-existing ring-buffer implementation. The package stores arbitrary T values without serializing them to []byte or converting them to interface{} on the data path. It provides safe concurrent access, ordered temporal queries, optional typed key indexes, explicit query statuses, and replayable subscriptions for derived-series pipelines.

The architecture separates four concerns:

1. A compact append/evict ring addressed by local ingest sequence (Seq). Seq-based reads, replay and subscriptions depend only on the ring.
2. An optional event-time ordering capability, accessed through an internal `timeIndex` abstraction. A B-tree implementation is the v1 reference. A Series may also be created with no event-time index at all; its storage, replay and subscription semantics are then unchanged.
3. Optional typed key indexes (hash-map based).
4. One `sync.Mutex` owned by Series[T]. It protects every piece of mutable state; no component has its own lock.

Readers copy results into caller-owned memory while holding the lock. No public API returns memory that a later append can overwrite.

Market data is not modeled as a stream with monotonic timestamps. Event time, local receive time, source sequence and local ingest sequence are distinct. Physical storage order is ingest order because it is total and naturally supports cursors. When event-time ordering is enabled, event-time queries remain correct for late and out-of-order events. When it is disabled, they report this explicitly instead of silently scanning.

Every query reports a typed Status so callers can always distinguish these cases:
- "never existed" (NotFound);
- "existed but was evicted" (Expired);
- "belongs to a cleared generation" (Reset);
- "not yet safe to read" (NotReady);
- "this series has no event-time index" (TimeIndexDisabled).

Identity is a Ref{Gen, Seq}. Seq starts at 1 (zero is the invalid sentinel), and Gen changes on Clear, so stale identities are rejected rather than silently aliased. Paginated time reads continue through an opaque TimeCursor, so no index-specific type appears in the public API.

# 2. Change Log

## 2.1 Changes in Revision 0.3

| ID | Change | Rationale | Where |
|---|---|---|---|
| A-01 | Event-time ordering is separated from bounded storage, Seq-based replay and subscriptions. It becomes an internal capability (`timeIndex` interface). The Series core no longer depends on any concrete B-tree API. | The B-tree becomes a replaceable implementation detail, and its cost can be measured and paid separately. | 3, 13 |
| A-02 | `Config.TimeIndex` (TimeIndexMode) selects the implementation at construction. The choice is immutable for the life of the Series, and there is no runtime switching. v1 modes are TimeIndexNone (zero value) and TimeIndexBTree. | Predictable layout and cost; there are no migration semantics to define. | 13.3, 21 |
| A-03 | On a TimeIndexNone series, all Seq-based operations work normally. Every event-time query returns StatusTimeIndexDisabled (ErrTimeIndexDisabled) with no data. There is no hidden O(N) fallback scan in v1. | An API that normally implies an ordered index must not silently degrade to linear scans. | 7, 8, 13.5 |
| A-04 | `RangeAfter(TimeKey, ...)` is removed. Truncated time-ordered reads return an opaque `TimeCursor`, and `Continue` resumes them. The ordering key is now internal. | Public pagination semantics must not expose or depend on index structure. | 7.4, 8.4, 21 |
| A-05 | The v1 synchronization baseline is `sync.Mutex`, replacing `sync.RWMutex`. `RWMutex` is a permitted alternative only if benchmarks justify it. | The workload is write-dominated. Batch reads and CloneOnRead hold the lock for long enough that RWMutex gives a misleading impression of cheap concurrent reads. | 11 |
| A-06 | Abstraction cost is constrained: at most one interface dispatch per logical operation, batch application inside the implementation, and a nil fast path for TimeIndexNone. | Near-zero overhead for series without an event-time index. | 13.6 |
| A-07 | New statuses: StatusTimeIndexDisabled (no ordering capability) and StatusInvalid (malformed argument, for example a foreign or zero TimeCursor). A status precedence order is defined. | Q-012 completeness for new conditions. | 7 |
| A-08 | `Entries` returns its Status up front: `(iter.Seq[Entry[T]], Status)`. | An iterator cannot report "disabled" or "not ready" otherwise. | 8.3 |
| A-09 | Single-lock simplification: Series-owned scratch buffers collect index results without allocation. | Safe only because all access is serialized. Documented as a constraint on any RWMutex variant. | 11.2, 13.2 |
| A-10 | Stats reports the time-index mode. Retained Min/MaxEventTime is defined only when an ordering capability exists. | Diagnostics must not show fabricated values. | 21 |
| A-11 | Benchmarks, tests, implementation plan and Definition of Done are updated. This adds a common semantic suite for all time-index implementations, a NoTimeIndex-vs-BTree benchmark matrix, and structural dependency/lock checks. | Acceptance criteria for the architectural change. | 22, 23, 30, 31 |

## 2.2 Gaps resolved in Revision 0.2 (historical record)

Rows marked "superseded" were changed by Revision 0.3; the 0.3 section reference is authoritative.

| ID | Gap in 0.1 | Resolution in 0.2 | Where (0.3) |
|---|---|---|---|
| G-01 | Status vs. bool: Q-012/E-001/E-003 require NotFound/Expired/Lagged/NotReady, but Get/AsOf/Before/After returned (Entry[T], bool) and Range/Prev/Next returned only []T. | Typed Status enum plus ReadInfo for batch reads. Subscription conditions are errors (ErrLagged, ErrReset, ErrClosed). Exact per-method semantics are defined. | 7, 8, 17, 21 |
| G-02 | Expired vs NotFound for time queries was undefined (E-003). | The eviction envelope (min/max evicted EventTime) gives exact Expired for AsOf/Before/After/AtOrAfter and a conservative test for exact-time lookups. | 7.3, 15.3 |
| G-03 | Partial overlap of a range with evicted data was silent. | ReadInfo.Evicted flag; Status Expired when the result is empty but evicted data could have matched. | 7.4 |
| G-04 | Key indexes and generics: a generic method on Series[T] is not expressible before Go 1.27, and interface methods cannot be generic even in Go 1.27. | Separately typed handles UniqueIndex[T,K] / MultiIndex[T,K]. | 10 |
| G-05 | Unique-key collisions were undefined. | ConflictPolicy: Reject (default), Drop, Replace, KeepFirst. Validated before mutation. Eviction is guarded. | 10.3, 14 |
| G-06 | InputMeta and AppendResult were undefined; so were late and duplicate policies. | Types, policies and flag bits are defined. | 6, 9, 21 |
| G-07 | At/Latest/key lookups were missing from the API sketch. | FirstAt, LastAt, AtAll, Latest, LatestByEventTime and index-handle lookups added. | 8, 10, 21 |
| G-08 | Inclusivity was undefined. | Range [from,to); AsOf <=; Before <; After >; AtOrAfter >=; PrevN/NextN partition at ts. ReadFrom replaces RangeSeq. | 8.1 |
| G-09 | dst semantics and RLock hold time were undefined. | Append semantics into dst; limit capped by MaxReadBatch; Truncated flag. Superseded in part: continuation is now an opaque TimeCursor (A-04) and the lock is a Mutex (A-05). | 8.2, 8.4, 11.4 |
| G-10 | head/tail off-by-one. | Oldest/Newest defined; Cursor.Next is the next Seq to read. | 6.2, 17 |
| G-11 | Seq start, overflow and Clear identity. | Seq starts at 1; MaxUint64 is reserved; Gen and Ref are introduced. | 6.2, 15.4 |
| G-12 | Capacity validation. | Capacity >= 1, any value, preallocated. | 12.1 |
| G-13 | LatestEventTime was ambiguous. | MaxEventTime (retained) vs MaxObservedEventTime (generation). | 8, 19, 21 |
| G-14 | Clone scope was undefined. | CloneMode OnRead/OnWrite/OnReadWrite. | 11.5 |
| G-15 | RWMutex hazards and callback re-entrancy. | Callbacks must not call the Series; batches are bounded; no yield under lock. Superseded in part: the baseline lock is now sync.Mutex (A-05); the re-entrancy rules are unchanged. | 11 |
| G-16 | Callback panics could leave partial state. | Prepare/commit split; panics are recovered in prepare. | 9.3 |
| G-17 | Multiple writers and batch atomicity. | Append is concurrency-safe; AppendBatch is atomic with one notification. | 9.4 |
| G-18 | Notification and subscription lifecycle. | Wake channels, start positions, lag policies, Close semantics. | 17 |
| G-19 | Derived-series error and cursor semantics. | In-memory cursor, stop on error, atomic output, Version. | 18 |
| G-20 | Watermark semantics. | Monotonic, single owner, optional read gate. | 19 |
| G-21 | Age-based retention. | v1 is capacity-only. | 15.1 |
| G-22 | Multi-index eviction cost. | FIFO posting lists; empty lists deleted. | 10.4 |
| G-23 | Composite key+time index. | Deferred. | 10.6, 32 |
| G-24 | Flags meaning. | Bit allocation between adapter and series. | 6.3 |
| G-25 | Snapshot and known-as-of queries. | Deferred. | 16.4 |
| G-26 | Timestamp unit. | int64 Unix ns UTC; sentinels reserved. | 6.1 |
| G-27 | Stats shape. | Stats defined; no per-read counters. | 21 |
| G-28 | Iterator API. | Paged iterator; never yields under lock. Superseded in part: pages continue via TimeCursor and Entries returns a Status (A-08). | 8.3 |
| G-29 | Open decisions. | All resolved. | 29 |
| G-30 | Missing tests. | T-011..T-020. | 23 |
| G-31 | tidwall/btree.Map cannot take a struct key; BTreeG locks internally unless NoLocks. | BTreeG with a less function and NoLocks: true. Superseded in part: this is now the reference implementation behind the timeIndex capability, not a Series dependency (A-01). | 13.4 |
| G-32 | Fragile references and legacy ring language. | Removed; ring implemented from scratch. | 1, 28, 33 |
| G-33 | Key lookups cannot distinguish evicted from never-seen keys. | Documented NotFound for both in v1. | 10.5 |
| G-34 | Empty range and invalid timestamps. | Defined. | 8.1, 20 |

# 3. Architectural Decisions

| Decision | Recommendation | Reason |
|---|---|---|
| Primary logical order | Seq: strictly increasing local ingest sequence, starting at 1 per generation | Total order despite equal or late timestamps; ideal for cursors and capacity retention. |
| Public identity | Ref{Gen, Seq} | Seq alone is ambiguous across Clear; Gen makes stale identities detectable. |
| Separation of concerns | Storage/replay/subscriptions are independent of event-time ordering | Seq semantics must not depend on whether, or how, event time is indexed. |
| Event-time ordering | Optional internal capability behind a `timeIndex` interface, selected at construction (TimeIndexMode) | Arrival order differs from event time; the structure that orders it must be replaceable and its cost optional. |
| v1 time-index implementations | `btreeTimeIndex` (reference) and `noTimeIndex` (none) | B-tree handles arbitrary disorder; "none" gives near-zero overhead for replay-only series. |
| B-tree library | github.com/tidwall/btree BTreeG with NoLocks: true, confined to the B-tree implementation file | Generic, ordered, seekable, with path hints. Map cannot take a struct key, and the Series owns synchronization. |
| Time-read continuation | Opaque TimeCursor | Pagination semantics do not leak index types and survive implementation changes. |
| Key index | Built-in Go map[K]Seq (unique) and map[K]*postings (multi), inside typed handles | Exact-key lookup is a hash workload; the built-in map is battle-tested. |
| Generic key typing | Separate typed handles registered at construction; unexported non-generic registration interface | Keeps K out of Series[T]'s type; no interface{} on the value data path; works with or without Go 1.27 generic methods. |
| Synchronization | Exactly one sync.Mutex per Series (v1 baseline) | The write path is the hot path. One lock gives the simplest consistency model; RWMutex is allowed only if benchmarks justify it. |
| Query status | Typed Status and ReadInfo; subscription conditions as wrapped sentinel errors | Callers distinguish every no-data reason without guesswork. |
| Read result ownership | Copy-out into caller-owned slices (append semantics), bounded batch size | Prevents use-after-overwrite and bounds lock hold time. |
| Notification | Coalesced per-subscriber wake channel plus cursor replay | No payload copy per notification; zero allocations on Append. |
| Derived series | Asynchronous cursor consumer by default | Ingest latency stays independent of derivation CPU. |
| Storage ring | Package-owned, implemented from scratch, no public mutable slices, no internal lock, no knowledge of indexes | The Series is the concurrency and coordination unit. |

# 4. Scope, Goals, and Non-Goals

## 4.1 Goals

- Store T directly using Go generics. No application-level []byte encoding, no reflection, and no conversion of T to interface{} on normal query or append paths.
- Bound storage by row capacity, with deterministic eviction of the oldest record.
- Provide Seq-based reads, deterministic replay and subscriptions that work in every configuration.
- On series configured with an event-time ordering capability, support exact and ordered temporal queries with late, duplicated, and equal-timestamp events.
- Keep event-time indexing optional and swappable, so that its cost is paid only where needed and can be measured separately from storage and ingestion.
- Support concurrent callers that receive independent copies.
- Support optional typed key indexes (unique and non-unique) without forcing an index on every series.
- Report explicit, distinguishable statuses for every query.
- Make consistency rules explicit enough that no reader can observe a partially applied append.
- Be testable with the race detector, fuzzing, property tests against reference models, and deterministic replay fixtures.

## 4.2 Non-goals for v1

- Durable storage, crash recovery, WAL, replication, or persisted subscription cursors.
- SQL, joins, query planning, arbitrary predicates.
- MVCC or transactions across several series.
- Lock-free mutation of arbitrary T values.
- Automatic correction of venue-specific feed gaps (adapters own source synchronization).
- Zero-copy APIs exposing ring memory.
- Age- or event-time-based retention (v1 is capacity-only).
- Runtime switching of the time-index implementation of an existing Series.
- Implicit O(N) fallback scans for event-time queries on series without a time index.
- Composite key+time indexes, immutable snapshots, and known-as-of (Seq-cutoff) queries (see Section 32).

## 4.3 Platform

- The module MUST build with Go 1.23 or later, because iter.Seq and range-over-func are used for the paged iterator [14].
- CI SHOULD test on the currently supported Go releases (Go 1.26 and Go 1.27 as of this revision).
- The design MUST NOT depend on Go 1.27 generic methods [12]; typed index handles are used instead (Section 10).

# 5. Market-Data Semantics

## 5.1 Required time/order dimensions

| Field | Meaning | Monotonic? | Primary use |
|---|---|---|---|
| EventTime | Venue/exchange/matching-engine time of the event | No global guarantee | Temporal queries, candles, indicators, event studies. |
| ReceiveTime | Local wall-clock time when the adapter observed the message | Approximately, as local observation only | Latency measurement, feed diagnostics. |
| SourceSeq | Venue/feed sequence or update identifier, when available | Often monotonic within a feed/session/product scope; may reset | Gap/duplicate detection, feed integrity. |
| SourceEpoch | Local identifier for a connection/session/source restart | Changes on reconnect | Disambiguates SourceSeq resets. |
| Seq (IngestSeq) | Local strictly increasing sequence assigned on acceptance | Yes, within a generation | Storage order, retention, subscriptions, replay, identity. |
| Gen | Series generation; incremented by Clear | Yes | Invalidates identities across Clear. |

Real feeds show why these fields must remain distinct:

- **Binance Spot diff-depth stream** [1]. Each event carries event time plus first (U) and final (u) update IDs. The documented local-book procedure says to ignore an event whose u is below the local book's update ID. It also says: "If the event first update ID (U) is greater than the update ID of your local order book + 1, you have missed some events. Discard your local order book and restart the process from the beginning."
- **Binance USDⓈ-M futures diff-depth stream** [15]. It adds pu ("Final update Id in last stream (ie u in last stream)") and requires each new event's pu to equal the previous event's u; otherwise the book must be re-initialized from a snapshot.
- **Coinbase Advanced Trade WebSocket** [2]. Messages carry a sequence_num. Coinbase documents that a sequence number "greater than one integer value from the previous number" indicates a dropped message, and that a lower number can be ignored or represents out-of-order arrival.

These are representative patterns; the Series does not implement any of them (Section 24).

## 5.2 Storage-order rule

The series MUST store accepted records in Seq order and MUST NOT reorder physical storage to enforce EventTime monotonicity. Late events are valid unless rejected by the configured LatePolicy. When an event-time ordering capability is configured, it defines event-time order independently of storage.

## 5.3 Late and out-of-order events

- A record is late if its EventTime is less than MaxObservedEventTime minus Config.LateTolerance. MaxObservedEventTime is the largest EventTime accepted in the current generation before this record.
- Late records are always flagged with FlagLate. LatePolicy decides whether they are accepted (default) or rejected.
- Late detection uses an O(1) tracker and works identically in every TimeIndexMode.
- Accept-and-index is the default. A late event can appear in an EventTime range query after an earlier query of the same range returned without it.
- EventTime queries describe the current retained dataset, not an immutable past snapshot (Section 16).
- Deterministic reconstruction of "what was observed" uses Seq order (ReadFrom and subscriptions), which never depends on the time index.
- Derived calculations that depend on EventTime order MUST choose an explicit reorder/watermark policy (Section 18.5).

# 6. Data Model and Identity

## 6.1 Timestamps

- Timestamp is int64 Unix nanoseconds, UTC. The range covers years 1677-2262.
- math.MinInt64 (MinTimestamp) and math.MaxInt64 (MaxTimestamp) are reserved sentinels. Append MUST reject a record whose EventTime equals either sentinel (ErrInvalidInput). This guarantees that half-open ranges and strict predecessor/successor searches never overflow.
- ReceiveTime 0 means unknown. ReceiveTime is wall-clock and carries no monotonic-clock reading; adapters that need precise latency SHOULD measure it separately.

## 6.2 Sequence, generation and Ref

- **Seq.** Seq is uint64. The first record of each generation receives Seq 1. Seq 0 is the invalid sentinel (SeqNone), so the zero value of Ref is invalid.
- **Seq exhaustion.** math.MaxUint64 is reserved as a search sentinel and is never assigned. If the next Seq would be math.MaxUint64, Append MUST fail with ErrSeqExhausted rather than wrap. At 10^9 appends per second this takes about 584 years, so it is a correctness boundary, not an operational concern.
- **Gen.** Gen is uint32, starts at 1 for a new Series, and increments on every Clear. If Gen would overflow, Clear MUST fail with ErrGenExhausted.
- **Ref.** Ref{Gen, Seq} is the public record identity. Entries returned by queries carry their Ref in Meta.Ref.
- **Oldest and Newest.** These are the smallest and largest retained Seq of the current generation. When Len > 0, Len = Newest - Oldest + 1 and Len <= Capacity. When Len == 0, OldestRef and NewestRef return the zero Ref.

## 6.3 Flags

Flags is uint32.

| Bits | Owner | Defined values |
|---|---|---|
| 0-15 | Adapter-supplied via InputMeta.Flags | FlagGap (bit 0): adapter detected a source gap before this record. FlagSnapshot (bit 1): record comes from a snapshot or recovery. FlagReplay (bit 2): record comes from a file or recorded replay. FlagCorrection (bit 3): record corrects an earlier record. Bits 4-15: application-defined. |
| 16-31 | Series-set; MUST NOT be supplied by callers | FlagLate (bit 16), FlagSourceSeqNotIncreasing (bit 17). Bits 18-31 reserved. |

Append MUST reject InputMeta.Flags with any series-owned bit set (ErrInvalidInput).

## 6.4 Records

```go
type InputMeta struct {
    EventTime   Timestamp   // required; sentinels rejected
    ReceiveTime Timestamp   // 0 = unknown
    SourceSeq   uint64      // 0 = not provided
    SourceEpoch SourceEpoch
    Flags       Flags       // adapter bits (0-15) only
}

type Meta struct {
    EventTime   Timestamp
    ReceiveTime Timestamp
    SourceSeq   uint64
    SourceEpoch SourceEpoch
    Flags       Flags // adapter bits | series-set bits
    Ref         Ref   // assigned by the Series; never supplied by callers
}

type Entry[T any] struct {
    Meta  Meta
    Value T
}
```

Internally, the value and its metadata occupy one logical slot and are written under the Series lock. A reader never observes a slot whose metadata belongs to a different value. Gen is not stored per slot (it is series-wide); Meta.Ref.Gen is filled at copy-out.

## 6.5 Event-time ordering key (internal)

```go
// Internal; not part of the public API.
type timeKey struct {
    EventTime Timestamp
    Seq       Seq
}
```

The ordering is lexicographic: EventTime, then Seq. SourceSeq is never the tie-breaker because it may be absent, scoped, or non-unique. For equal EventTime, ingest order defines order. Every time-index implementation MUST realize exactly this order. Because the key is internal, the public API exposes event-time positions only through Entry.Meta (EventTime and Ref) and the opaque TimeCursor.

# 7. Status and Error Model

## 7.1 Status values

| Status | Meaning |
|---|---|
| StatusOK | At least one matching record was returned. |
| StatusNotFound | No matching record is retained, and no record evicted in the current generation could have matched. This includes a never-assigned or future Seq, an empty series, and an empty interval. |
| StatusExpired | No matching record is retained, but at least one record evicted in the current generation matched (exact) or may have matched (conservative, where stated). |
| StatusReset | The supplied Ref or TimeCursor belongs to an earlier generation (invalidated by Clear). |
| StatusNotReady | The query bound exceeds the series watermark and Config.GateReadsByWatermark is true (Section 19). |
| StatusTimeIndexDisabled | An event-time query was issued on a Series created with TimeIndexNone. No data is returned. |
| StatusInvalid | The request is malformed: a zero TimeCursor, or a TimeCursor produced by a different Series. |

Status.Err() maps OK to nil and the others to ErrNotFound, ErrExpired, ErrReset, ErrNotReady, ErrTimeIndexDisabled and ErrInvalidInput respectively, so callers may use errors.Is.

Lag is not a query status; it is a subscription condition reported as ErrLagged (Section 17). Write failures are errors (Section 9.5).

## 7.2 Per-method semantics for point lookups

The rows for time-based methods apply when the Series has an event-time ordering capability. Otherwise those methods return StatusTimeIndexDisabled (7.5).

| Method | OK when | NotFound when | Expired when | Reset when |
|---|---|---|---|---|
| Get(ref) | ref.Gen == Gen and Oldest <= ref.Seq <= Newest | ref is zero; ref.Gen > Gen; ref.Seq > Newest (not yet assigned) | ref.Gen == Gen and 1 <= ref.Seq < Oldest | ref.Gen < Gen |
| Latest() | Len > 0 | Len == 0 | never | never |
| LatestByEventTime() | Len > 0 | Len == 0 | never | never |
| AsOf(ts) | a retained record has EventTime <= ts | otherwise, and MinEvicted > ts or nothing evicted | no retained match and MinEvicted <= ts (exact) | never |
| Before(ts) | a retained record has EventTime < ts | otherwise, and MinEvicted >= ts or nothing evicted | no retained match and MinEvicted < ts (exact) | never |
| After(ts) | a retained record has EventTime > ts | otherwise, and MaxEvicted <= ts or nothing evicted | no retained match and MaxEvicted > ts (exact) | never |
| AtOrAfter(ts) | a retained record has EventTime >= ts | otherwise, and MaxEvicted < ts or nothing evicted | no retained match and MaxEvicted >= ts (exact) | never |
| FirstAt/LastAt(ts) | a retained record has EventTime == ts | otherwise, and ts outside [MinEvicted, MaxEvicted] | no retained match and MinEvicted <= ts <= MaxEvicted (conservative "may have existed") | never |

MinEvicted and MaxEvicted are the minimum and maximum EventTime over all records evicted in the current generation (Section 15.3). They reset on Clear.

## 7.3 Rationale for exactness

For AsOf and Before, "some evicted record would have satisfied the predicate" is equivalent to "the minimum evicted EventTime satisfies it", so Expired is exact. Symmetrically, After and AtOrAfter are exact using the maximum. Exact-equality lookups cannot be made exact without tombstones. The conservative range test is documented and acceptable, because Expired for an exact lookup is advisory.

The eviction envelope is maintained from ring metadata, independently of the time-index implementation. Every implementation therefore yields identical statuses.

## 7.4 Batch read results

```go
type ReadInfo struct {
    Status    Status     // OK if N > 0; otherwise per 7.2/7.5 applied to the whole query
    N         int        // number of entries appended to dst by this call
    Truncated bool       // limit reached while more matching retained records exist
    Evicted   bool       // records evicted in this generation could have matched (result may be incomplete at the old end)
    Next      TimeCursor // non-zero iff Truncated and the read supports continuation (8.4)
    LastRef   Ref        // Ref of the last returned entry (continue Seq reads with ReadFrom(LastRef.Seq+1))
    Oldest    Ref        // oldest retained Ref at the time of the read (diagnostics, ReadFrom)
}
```

- For a time range [from, to), Evicted is true iff something was evicted in this generation and the interval [MinEvicted, MaxEvicted] intersects [from, to).
- When N == 0 and Evicted is true, Status is Expired. When N == 0 and Evicted is false, Status is NotFound.

## 7.5 Status precedence

When several conditions apply, a query reports the first matching status in this order:

1. StatusTimeIndexDisabled: a time-based method on a TimeIndexNone series.
2. StatusInvalid: a malformed argument (zero or foreign TimeCursor).
3. StatusReset: a Ref or TimeCursor from an earlier generation.
4. StatusNotReady: the watermark gate (Section 19).
5. StatusOK / StatusExpired / StatusNotFound, according to the data.

# 8. Functional Query Requirements

The exact method names may change; the semantics are normative. "Requires time index" means the method returns StatusTimeIndexDisabled on a TimeIndexNone series (Q-016).

| ID | Requirement | Priority | Acceptance / rationale |
|---|---|---|---|
| Q-001 | Get(ref) returns one retained entry or NotFound/Expired/Reset per 7.2. Available in every mode. | MUST | O(1) slot lookup plus generation and bounds check. |
| Q-002 | Exact event-time lookup with explicit duplicate semantics: FirstAt(ts) (smallest Seq at ts), LastAt(ts) (largest Seq at ts), AtAll(dst, ts, limit) (all at ts, ascending Seq). Requires time index. | MUST | No implicit choice among duplicates. |
| Q-003 | AsOf(ts) returns the retained entry with the greatest (EventTime, Seq) such that EventTime <= ts (latest-ingested among ties). Requires time index. | MUST | Core trading operation; never looks ahead. |
| Q-004 | Before(ts): greatest (EventTime, Seq) with EventTime < ts. After(ts): smallest (EventTime, Seq) with EventTime > ts. AtOrAfter(ts): smallest (EventTime, Seq) with EventTime >= ts. Requires time index. | MUST | Strict and inclusive variants have distinct names. |
| Q-005 | Range(from, to) returns entries with from <= EventTime < to, ascending by (EventTime, Seq). Requires time index. | MUST | O(log N + K) search plus O(K) copy with the reference implementation. |
| Q-006 | PrevN(ts, n): the n entries with the greatest (EventTime, Seq) such that EventTime <= ts, returned ascending. NextN(ts, n): the n entries with the smallest (EventTime, Seq) such that EventTime > ts, returned ascending. Requires time index. | MUST | PrevN and NextN partition the timeline at ts with no overlap; PrevN's last element equals AsOf(ts). |
| Q-007 | ReadFrom(from Ref, limit): entries with Seq >= from.Seq in ascending Seq order (replay). If from.Seq < Oldest: no data, StatusExpired, ReadInfo.Oldest set. If from.Gen < Gen: StatusReset. Available in every mode. | MUST | Deterministic incremental replay with no silent skipping. |
| Q-008 | Latest() returns the newest-ingested entry (every mode). LatestByEventTime() returns the entry with the greatest (EventTime, Seq) (requires time index). Stats exposes MaxObservedEventTime (every mode) and retained Min/MaxEventTime (time index only). | MUST | Hot-path status and sync checks; both meanings defined. |
| Q-009 | Unique key lookup via UniqueIndex[T,K].Get(k). | SHOULD | Hash index, expected O(1). |
| Q-010 | Non-unique key lookup via MultiIndex[T,K].Get(dst, k, limit, order). | SHOULD | Returns copies, in ascending or descending Seq order. |
| Q-011 | Every batch read accepts a caller-provided dst and appends to it. | MUST | Zero allocations after warm-up when cap(dst) suffices. |
| Q-012 | Every query reports Status (point reads) or ReadInfo (batch reads) per Section 7. | MUST | NotFound, Expired, Reset, NotReady, TimeIndexDisabled and Invalid are distinguishable; Lagged via subscriptions. |
| Q-013 | Every batch read is bounded by min(limit, Config.MaxReadBatch) and reports Truncated. | MUST | Bounds lock hold time (11.4). |
| Q-014 | Truncated Range, RangeValues and AtAll reads return an opaque TimeCursor in ReadInfo.Next; Continue(dst, cursor, limit) resumes strictly after the last returned record, up to the original upper bound. | MUST | Lossless pagination without exposing index types (8.4). |
| Q-015 | A paged Entries(from, to) scan that never holds the lock while yielding; requires time index. | SHOULD | Ergonomic scans without lock hazards (8.3). |
| Q-016 | On a TimeIndexNone series, every event-time method (FirstAt, LastAt, AtAll, AsOf, Before, After, AtOrAfter, Range, RangeValues, Continue, PrevN, NextN, LatestByEventTime, Entries) returns StatusTimeIndexDisabled with no data. No fallback scan is performed. TimeIndexMode() reports the configured mode. | MUST | Capability absence is explicit and never silently O(N). |
| Q-017 | Seq-based reads (Get, Latest, ReadFrom), key-index lookups and subscriptions never depend on the event-time index. Their results, statuses and complexity are identical in every TimeIndexMode. | MUST | Replay and ingestion paths are isolated from event-time indexing. |

## 8.1 Inclusivity and edge rules

- Range uses half-open [from, to). If from >= to, the result is empty, Status is NotFound, and no error occurs (E-002).
- PrevN and NextN with n <= 0 return empty with StatusNotFound. n is capped by MaxReadBatch, and Truncated is set when the cap applies and more matching records exist. PrevN and NextN do not return a TimeCursor; callers needing more than MaxReadBatch records use Range plus Continue.
- AtAll(ts) with ts == MaxTimestamp returns NotFound, because the sentinel is never stored.
- All time-ordered batch results are ascending by (EventTime, Seq). MultiIndex results follow the requested Order by Seq.
- ReadFrom with from.Seq > Newest returns empty with StatusNotFound (nothing yet). ReadFrom with a zero Ref is treated as "from Oldest".

## 8.2 Destination buffer semantics

- Batch reads append results to dst and return the extended slice, like append and strconv.Append*. Callers reuse buffers by passing dst[:0].
- A nil dst is valid; the method allocates as needed.
- If cap(dst) is insufficient, the slice grows via append (allocation). Results are never truncated because of capacity, only by the effective limit.
- The effective limit is min(limit, Config.MaxReadBatch). A limit <= 0 means Config.MaxReadBatch.
- Values copied into dst are owned by the caller. With CloneMode OnRead, each value passes through Clone.

## 8.3 Scans and iterators

- The library MUST NOT expose any callback or iterator that runs user code while the Series lock is held. Go's sync.Mutex is not reentrant, so a user callback that queries the same Series under the lock deadlocks (11.3).
- `Entries(from, to Timestamp) (iter.Seq[Entry[T]], Status)` SHOULD be provided.
  - The Status is computed up front and covers StatusTimeIndexDisabled, StatusNotReady and an empty interval. When it is not OK, the returned sequence yields nothing.
  - Each page (up to MaxReadBatch entries) is copied under the lock. The lock is released, the page is yielded, and the scan resumes from an internal TimeCursor.
  - Each page is internally consistent, but the scan as a whole is not atomic. Records evicted between pages are skipped, and late records inserted behind the cursor position are not seen.

## 8.4 Time cursors (continuation)

```go
// TimeCursor is an opaque continuation token for time-ordered reads.
// Its contents are implementation-defined and may differ between time-index implementations.
type TimeCursor struct {
    series uint64    // identity of the producing Series (assigned at New)
    gen    Gen       // generation at the time of the read
    state  [3]uint64 // implementation-defined position and bounds
}

func (c TimeCursor) IsZero() bool
```

- A TimeCursor is returned in ReadInfo.Next only when a Range, RangeValues or AtAll read was truncated.
- `Continue(dst, c, limit)` and `ContinueValues(dst, c, limit)` return the next page. The page contains records strictly after the last returned record in (EventTime, Seq) order and below the original exclusive upper bound. Pages are ascending, and later pages may themselves return a new cursor.
- For the B-tree implementation the state encodes (EventTime, Seq) of the last returned record and the upper bound. Other implementations may encode different continuation state. Callers MUST NOT inspect or construct cursors.
- A cursor is valid only for the Series that produced it:
  - a zero cursor or a cursor from another Series returns StatusInvalid;
  - a cursor from an earlier generation returns StatusReset;
  - on a TimeIndexNone series, Continue returns StatusTimeIndexDisabled (no cursor can exist there).
- Late records whose (EventTime, Seq) falls before the cursor position are not returned by the continuation. Records evicted between pages are skipped, and ReadInfo.Evicted reflects the eviction envelope.
- Because the time-index mode is fixed per Series, a cursor is never presented to an implementation other than the one that produced it.

# 9. Write Path

## 9.1 Requirements

| ID | Requirement | Priority | Acceptance / rationale |
|---|---|---|---|
| W-001 | Append(value, meta) validates, assigns a Ref, evicts the oldest record if full, updates the configured indexes, and returns AppendResult. | MUST | One locked transition. |
| W-002 | AppendBatch(dst, items) is atomic: either all items are applied (subject to per-item Drop outcomes) or none, with a single notification. | MUST | Atomic multi-record commit for snapshots and derived outputs. |
| W-003 | A batch larger than Capacity fails with ErrBatchTooLarge. | MUST | Items in one transaction must not evict each other. |
| W-004 | All validation, key extraction, ingest cloning and conflict checks complete before any mutation. | MUST | E-007: no partial state on failure or panic. |
| W-005 | LatePolicy: LateAccept (default) or LateReject; lateness is always flagged. Behavior is identical in every TimeIndexMode. | MUST | Explicit late-event policy. |
| W-006 | SourceSeqPolicy: SourceSeqIgnore (default), SourceSeqFlag, SourceSeqReject. | MUST | SourceSeq scoping is feed-specific; the generic default is not to interpret it. |
| W-007 | Append is safe for concurrent callers; Seq order equals lock acquisition order. | MUST | No ordering promise between concurrent callers beyond that. |
| W-008 | Append after Close fails with ErrClosed. | MUST | Deterministic shutdown. |
| W-009 | Append SHOULD NOT allocate in steady state. Exceptions: amortized time-index node growth and per-key structures in key indexes. With TimeIndexNone and no key indexes, Append MUST NOT allocate. | SHOULD / MUST | Notification is allocation-free; the no-index path is allocation-free. |
| W-010 | The time index is updated with at most one interface call per committed transaction (Append or AppendBatch), and with none when TimeIndexNone. | MUST | Abstraction cost bounded (13.6). |

## 9.2 Policies

- **Late detection.** A record is late iff EventTime < MaxObservedEventTime - LateTolerance. MaxObservedEventTime is the generation's maximum before this record; no record is late when the generation is empty. LateReject returns ErrLate.
- **SourceSeq check.** This applies only when the policy is not Ignore and SourceSeq != 0.
  - If SourceEpoch equals the epoch of the last checked record and SourceSeq <= the last SourceSeq seen for that epoch, the record gets FlagSourceSeqNotIncreasing. SourceSeqReject returns ErrSourceSeqNotIncreasing instead.
  - A change of SourceEpoch resets the tracker.
  - This is a single-stream heuristic. For series that merge several sequence scopes (for example several products), keep the default Ignore and detect gaps in the adapter.
- **Exact deduplication.** Deduplication by source identifier (for example trade ID) is done with a UniqueIndex using ConflictDrop (Section 10.3), not by SourceSeq heuristics.
- **Gap detection.** This is the adapter's job; the adapter signals it with FlagGap (E-006).

## 9.3 Append transaction

1. Acquire the Series mutex. If closed, release it and return ErrClosed.
2. Prepare phase (no mutation):
   - validate InputMeta (sentinel timestamps, flag bits);
   - compute lateness and SourceSeq flags, and apply policies;
   - if CloneMode includes OnWrite, clone the value;
   - for each key index, extract the key and check conflicts, treating rows that this transaction will evict as absent;
   - for batches, simulate items sequentially, including intra-batch conflicts and Drop outcomes.

   Panics raised by Clone or extractors are recovered here and returned as *CallbackPanicError (errors.Is(err, ErrCallbackPanic)).
3. If any item is rejected, release the mutex and return the error. No state has changed.
4. Commit phase, for each applied item:
   - assign Seq = Newest + 1;
   - if Len == Capacity, evict Oldest: append its key (EventTime from ring metadata, Seq) to the Series-owned `delKeys` scratch, remove key-index entries per Section 14, and update MinEvicted/MaxEvicted and counters;
   - write value and metadata into the slot;
   - append the new key to the `insKeys` scratch;
   - insert key-index entries;
   - update MaxObservedEventTime, the SourceSeq tracker, and statistics.
5. If `s.time != nil`, call `s.time.Apply(delKeys, insKeys)` once for the whole transaction. The two sets are disjoint (Seq values differ), so applying all deletions and insertions together yields the same final state as interleaving them.
6. Release the mutex.
7. Wake subscribers (non-blocking, coalesced; Section 17.3).

The commit phase must not call user code. Time-index and map operations in the commit phase can fail only through runtime allocation failure, which is a process-level fault (Section 27). A single Append MAY call `Set`/`Delete` directly instead of `Apply`. Either way, at most one interface dispatch per key and no per-element dispatch inside the implementation.

## 9.4 Batch semantics

- Items are applied in slice order and receive consecutive Seqs; Drop outcomes consume no Seq.
- Unique-conflict detection covers retained rows and earlier items in the same batch.
- dst []AppendResult receives one result per item (append semantics).
- The time index receives one `Apply` call per batch, and exactly one wake-up is issued per batch.

## 9.5 Results and errors

```go
type Outcome uint8

const (
    OutcomeAppended Outcome = iota // record stored and indexed
    OutcomeDropped                 // not stored: a ConflictDrop unique index already maps the key
)

type AppendResult struct {
    Outcome  Outcome
    Ref      Ref    // assigned identity; zero when Dropped
    Flags    Flags  // series-set flags applied (FlagLate, FlagSourceSeqNotIncreasing)
    Evicted  Ref    // record evicted by this append; zero if none
    Existing Ref    // Dropped: retained record that holds the key; Replace: record that lost the mapping; KeepFirst: record that kept it
    Index    string // name of the index that produced Existing, if any
}
```

Write errors:
- ErrClosed
- ErrInvalidInput
- ErrLate
- ErrSourceSeqNotIncreasing
- ErrUniqueConflict, as *ConflictError{Index, Key, Existing Ref, BatchPos int}. Key is formatted with %v for the error message only.
- ErrBatchTooLarge
- ErrCallbackPanic, as *CallbackPanicError{Callback, Value any, Stack}
- ErrSeqExhausted

Every error return guarantees no state change.

# 10. Key Indexes

## 10.1 Requirements

| ID | Requirement | Priority | Acceptance / rationale |
|---|---|---|---|
| K-001 | Key indexes are typed handles UniqueIndex[T,K comparable] and MultiIndex[T,K comparable], constructed with a name and an extractor, and registered via Config.Indexes. | MUST | No generic method on Series[T] is needed. |
| K-002 | Indexes are fixed at construction; there is no dynamic registration in v1. | MUST | Predictable memory and layout; simpler invariants. |
| K-003 | A handle binds to exactly one Series; reusing it fails New with ErrIndexBound. Index names are unique per Series (ErrInvalidConfig). | MUST | Lookups know their series and lock. |
| K-004 | Keys and postings are stored in typed Go maps. T values and K keys are never converted to interface{} on append or lookup paths. | MUST | Type safety and no boxing. |
| K-005 | Extractor signature: func(e *Entry[T]) (K, bool), where ok=false means "not indexed". It runs in the prepare phase with Meta (including the tentative Ref) populated. It MUST be pure, deterministic and non-blocking; it must not retain e and must not call any Series method. | MUST | Panic and deadlock safety. |
| K-006 | The extracted key is stored per slot inside the index (a side array of K plus an indexed bit), so eviction never re-invokes the extractor. | MUST | Eviction cannot panic and stays correct even if T is misused. |
| K-007 | Unique index ConflictPolicy: Reject (default), Drop, Replace, KeepFirst (10.3). | MUST | Explicit collision behavior. |
| K-008 | Eviction removes a unique mapping only if it still points to the evicted Seq. | MUST | Older rows' eviction never deletes newer mappings. |
| K-009 | Multi-index posting lists are kept in ascending Seq order. Eviction pops the front in O(1), and empty lists are deleted from the map. | MUST | Bounded memory; O(1) eviction. |
| K-010 | Lookups take the Series mutex, copy entries (Clone OnRead applies), and return Status/ReadInfo. | MUST | Same ownership rules as other reads. |
| K-011 | Key indexes have no lock of their own and work identically in every TimeIndexMode. | MUST | Single consistency boundary (C-002). |

## 10.2 Why handles (Go generics constraint)

Before Go 1.27, a method could not declare its own type parameters, so Series[T] could not offer Lookup[K](k K) [13]. Go 1.27 (August 2026) permits generic methods on concrete types, but "methods of interfaces may not declare type parameters nor can interface methods be implemented by generic methods" [12]. Even with Go 1.27, a generic method on Series[T] would have to locate a type-erased index at call time and assert its type at runtime.

Typed handles avoid that. The handle is created with a concrete K, owns its typed maps, and is the lookup entry point. The Series sees only an unexported, non-generic interface used for registration and maintenance. This design builds on Go 1.23+ (Section 4.3).

```go
// Unexported methods: only this package can implement Index[T].
type Index[T any] interface {
    indexName() string
    bind(s *Series[T], capacity int) error
    prepare(e *Entry[T], willEvict func(Seq) bool) (prepared, error) // no mutation
    commit(p prepared, slot int, seq Seq)                           // under the Series mutex
    evict(slot int, seq Seq)                                        // under the Series mutex
    reset()                                                         // Clear
    stats() IndexStats
}
```

The prepared token is a small non-generic struct carrying per-item decisions. The typed key itself is held in a per-index scratch field (or a reusable scratch slice for batches). This is safe because all access is serialized by the Series mutex. No K is boxed.

## 10.3 Unique conflict policies

A conflict exists when the extracted key is already mapped to a retained Seq that the same transaction does not evict, or to an earlier item of the same batch.

| Policy | Append outcome | Index mapping afterwards | AppendResult |
|---|---|---|---|
| ConflictReject (default) | Fails with ErrUniqueConflict; no state change (the whole batch fails). | Unchanged | error |
| ConflictDrop | Item is not stored; no Seq consumed; no error. Intended for idempotent ingestion, for example trade-ID dedup after reconnect. | Unchanged | Outcome Dropped, Existing = holder |
| ConflictReplace | Item stored; mapping now points to the new Seq. The previous row stays retained (ring, time index if enabled) but is no longer reachable through this index. | key -> new Seq | Existing = previous holder |
| ConflictKeepFirst | Item stored (and time-indexed if enabled), but not entered into this index (indexed bit false). | Unchanged | Existing = holder |

With several unique indexes, each index's policy is evaluated in Config.Indexes order:
- any Reject fails the item;
- otherwise any Drop drops it (Drop wins over Replace and KeepFirst on other indexes), and no index is modified for a dropped item.

## 10.4 Non-unique (multi) indexes

- The index is a map[K]*postings, where postings is a FIFO of Seq. It is a slice with a head offset, compacted when the head passes half the length, or a chunked deque.
- Append pushes Seq at the back. Since Seq is increasing, each list stays sorted.
- Eviction pops the front, which MUST equal the evicted Seq (asserted in debug builds). Empty lists are deleted.
- Get(dst, k, limit, order) copies up to the effective limit in ascending or descending Seq order. Latest(k) returns the newest entry, and Count(k) returns the list length.

## 10.5 Status semantics for key lookups

- UniqueIndex.Get(k) returns OK with the mapped entry, or NotFound. v1 keeps no tombstones, so an evicted key and a never-seen key both return NotFound.
- MultiIndex.Get returns ReadInfo with Status OK if N > 0, otherwise NotFound, and Truncated when limited.

## 10.6 Out of scope: composite key + time index

A (Key, EventTime, Seq) ordered index is deferred to v2. Workaround: MultiIndex.Get returns a key's entries in Seq order, and callers filter by EventTime. If profiling shows key+time-range queries are hot, v2 may add a per-key ordered index behind the same capability approach as Section 13.

## 10.7 Map choice

The built-in Go map is the default. tidwall/hashmap (generic Map and Set, open addressing with Robin Hood hashing, xxh3, automatic shrinking on deletes) is a benchmark candidate only [8].

sync.Map MUST NOT be used. Its documentation states that "Most code should use a plain Go map instead, with separate locking or coordination, for better type safety and to make it easier to maintain other invariants along with the map content" [9].

# 11. Concurrency and Memory Safety

## 11.1 Requirements

| ID | Requirement | Priority | Acceptance / rationale |
|---|---|---|---|
| C-001 | Exactly one sync.Mutex per Series protects all mutable state: ring, metadata, time index, key indexes, bounds, generation, watermark, policy trackers, statistics, scratch buffers. | MUST | One consistency domain. |
| C-002 | No component (ring, time-index implementation, key index) has its own lock or other synchronization. The B-tree implementation is created with NoLocks: true. | MUST | No nested or redundant locking. |
| C-003 | Every read locates and copies its results while holding the mutex, and completes the copy before unlocking. | MUST | No borrowed memory. |
| C-004 | User callbacks (extractors, Clone) run only where documented and MUST NOT call any method of any Series. Derive functions run outside all Series locks. Index implementations never call Series methods or user code. | MUST | sync.Mutex is not reentrant. |
| C-005 | Notifications are sent only after the mutex is released. | MUST | Woken consumers can immediately read committed state. |
| C-006 | Read batches are bounded by MaxReadBatch. | MUST | Bounded writer latency. |
| C-007 | The mutex is released via defer on every path, including panics in read-path Clone calls. | MUST | A panicking reader cannot wedge the Series. |
| C-008 | No user code executes during lock-held iteration, and nothing is yielded while the lock is held. | MUST | Section 8.3. |

## 11.2 Synchronization model

```go
type Series[T any] struct {
    mu   sync.Mutex
    ring ring[T]   // values + compact metadata, preallocated; knows nothing about indexes
    time timeIndex // nil when TimeIndexNone; no own synchronization
    idx  []Index[T]

    gen    Gen
    oldest Seq // 0 when empty
    newest Seq // 0 when empty

    // Series-owned scratch, valid only under mu:
    keyScratch []timeKey // index query results, cap MaxReadBatch (allocated only if time != nil)
    delKeys    []timeKey // commit-phase evictions
    insKeys    []timeKey // commit-phase insertions

    // eviction envelope, watermark, policy trackers, stats, closed flag, series identity

    subs atomic.Pointer[[]*subscriber] // copy-on-write list, read without mu on notify
}
```

The Go memory model requires that "Programs that modify data being simultaneously accessed by multiple goroutines must serialize such access" [3]. Copying a slice header does not make concurrent access to its backing array safe.

Because access is fully serialized, the Series can keep single scratch buffers for index results and commit-phase keys. Reads and writes then avoid allocation without pools or per-call buffers.

## 11.3 Why sync.Mutex is the v1 baseline

- **The write path is the hot path.** The Series serves high-frequency ingestion. Batch reads and CloneOnRead hold the lock for a duration proportional to the batch size and Clone cost. Under RWMutex, a waiting writer blocks all new readers anyway (11.6), so the claimed benefit of concurrent reads largely disappears in a write-dominated workload. A single Mutex makes the cost model honest and the consistency model simpler.
- **Re-entrancy.** sync.Mutex is not reentrant. Any user code that calls back into the same Series while the lock is held deadlocks immediately and deterministically. Hence C-004 and Section 8.3.
- **No upgrades.** There is no lock upgrade path, so read-then-write patterns do not exist inside the Series. Every mutation re-validates under the lock it holds.

## 11.4 Lock hold time

- Copying K values under the mutex costs O(K x sizeof(T)) plus the Clone cost. During that time it blocks the writer and all other callers.
- Implementations MUST cap K at MaxReadBatch (default 4096). The default SHOULD be tuned using the p99 write-latency benchmarks of Section 22.
- Long scans use pagination (Continue, Entries) so the lock is released between pages.

## 11.5 Reference types inside T and Clone

A shallow copy does not deep-copy maps, slices or pointers inside T. Supported modes:

- **Default.** T obeys value-immutability by contract. This is recommended for hot market data: numbers, arrays, small fixed-size fields.
- **Config.Clone with Config.CloneMode:**
  - CloneOnRead (zero value; the default when Clone is set). Clone runs for every value copied out: Get, all time reads, ReadFrom, Continue, Entries, index lookups and subscription batches. It runs under the mutex, so its cost adds directly to lock hold time.
  - CloneOnWrite. Clone runs once in the prepare phase, so the producer can reuse its buffers after Append.
  - CloneOnReadWrite. Both.
- Clone MUST NOT call Series methods. Panics in Clone during Append are recovered with no state change. Panics during reads propagate after the deferred Unlock.

## 11.6 Permitted alternative: sync.RWMutex

`sync.RWMutex` MAY replace `sync.Mutex` only if benchmarks on representative workloads (Section 22.4) show a material benefit, and only if all of these hold:

- there is still exactly one lock per Series, and C-002..C-008 hold unchanged;
- Series-owned scratch buffers are no longer shared by readers: read paths must use per-call stack buffers, caller-provided buffers, or a pool, and must stay allocation-free in steady state;
- the documented behavior is accounted for in latency analysis: "If any goroutine calls RWMutex.Lock while the lock is already held by one or more readers, concurrent calls to RWMutex.RLock will block until the writer has acquired (and released) the lock ... Note that this prohibits recursive read-locking." [9]

RWMutex is an optimization choice, not an architectural requirement.

## 11.7 Optional future optimization: immutable pages

If benchmarks prove copy-out is a bottleneck, v2 may seal fixed-size immutable pages that can be read without the lock, while the active page stays synchronized. This is not a v1 dependency.

# 12. Storage Engine Design

## 12.1 Ring

- The ring is implemented from scratch in ring.go as an unexported type. It has no lock, exposes no slices, and knows nothing about time indexes or key indexes.
- Capacity MUST be >= 1. A power of two is not required: slot = (seq - 1) mod capacity, and implementations MAY use a mask when capacity is a power of two.
- Storage is preallocated at New: values []T and metadata (EventTime, ReceiveTime, SourceSeq, SourceEpoch, Flags). Seq is derivable from slot and bounds, but MAY be stored for debug assertions.
- New returns ErrInvalidConfig for any of these: Capacity <= 0, MaxReadBatch < 0, an unknown policy or TimeIndexMode value, or duplicate index names.
- An evicted slot is overwritten by the incoming record in the same transaction. Clear zeroes all slots so that references inside T can be garbage collected.
- The ring alone provides Seq-addressed access. Get, Latest, ReadFrom and subscriptions use only the ring and the Oldest/Newest bounds.

## 12.2 Bounds

```
empty:      oldest = newest = 0
non-empty:  1 <= oldest <= newest, newest - oldest + 1 = Len <= Capacity
retained:   oldest <= s <= newest
append:     s = newest + 1; if Len == Capacity { evict oldest; oldest++ }
```

# 13. Event-Time Ordering Capability

Event-time ordering is a capability of a Series, separate from bounded storage, Seq-based replay and subscriptions. The Series accesses it only through the internal `timeIndex` interface. The interface describes the capability, not a particular data structure.

## 13.1 Requirements

| ID | Requirement | Priority | Acceptance / rationale |
|---|---|---|---|
| TI-001 | The Series core depends only on the internal timeIndex interface. Concrete implementations live in their own files, and no other file references a concrete index API (for example tidwall/btree types). | MUST | B-tree is replaceable without touching the Series core. |
| TI-002 | The implementation is selected at construction via Config.TimeIndex and is immutable for the life of the Series. Clear keeps the mode. There is no runtime switching. | MUST | Predictable layout and cost. |
| TI-003 | Implementations have no lock or other synchronization. Every call happens with the Series mutex held. | MUST | Single consistency boundary. |
| TI-004 | Implementations MUST NOT call Series methods, key-index methods, or user code. | MUST | No re-entrancy or deadlock. |
| TI-005 | Implementations return no borrowed memory. Results are returned by value or appended into a caller-supplied slice, and the implementation retains no reference to argument slices after the call. | MUST | Copy-out discipline inside the package. |
| TI-006 | Implementations order keys by (EventTime, Seq), and their key set equals exactly the set of retained records of the current generation. | MUST | Query semantics independent of implementation. |
| TI-007 | At most one interface dispatch per logical operation (Set, Delete, Apply, a point lookup, a scan). Batches and scans are processed inside the implementation without per-element dispatch. | MUST | Abstraction cost bounded. |
| TI-008 | TimeIndexNone is represented by a nil `timeIndex` in the Series. Every write-path call site is guarded (`if s.time != nil`), so there is no per-record work beyond the nil check. | MUST | Near-zero cost without an index. |
| TI-009 | Event-time queries on a TimeIndexNone series return StatusTimeIndexDisabled. No fallback scan exists in v1. | MUST | No hidden O(N) behavior. |
| TI-010 | Disabling the time index does not change Seq, Gen, retention, eviction, key-index, late-detection, watermark, ReadFrom or subscription semantics. | MUST | Isolation of concerns. |
| TI-011 | Every implementation passes the common semantic test suite (23.1). | MUST | Interchangeability. |
| TI-012 | In steady state, implementations allocate nothing except amortized internal growth. | SHOULD | Allocation-sensitive workload. |
| TI-013 | The public API exposes no index-specific types (ordering keys, path hints, iterators). Continuation uses TimeCursor. | MUST | Public semantics survive implementation changes. |

## 13.2 Internal interface

```go
// timeIndex is the internal event-time ordering capability.
// All methods are called with Series.mu held. Implementations hold no lock,
// call no Series method or user code, and retain no reference to argument slices.
type timeIndex interface {
    // Mutation.
    Set(k timeKey)
    Delete(k timeKey)
    Apply(del, ins []timeKey) // one call per committed transaction; del and ins are disjoint
    Clear()

    // Point primitives (keys only; the Series resolves Seq -> slot).
    AsOf(ts Timestamp) (timeKey, bool)      // greatest key with EventTime <= ts
    Before(ts Timestamp) (timeKey, bool)    // greatest key with EventTime <  ts
    After(ts Timestamp) (timeKey, bool)     // smallest key with EventTime >  ts
    AtOrAfter(ts Timestamp) (timeKey, bool) // smallest key with EventTime >= ts
    Min() (timeKey, bool)
    Max() (timeKey, bool)

    // Ordered scans. Each appends to dst and reports whether more matching keys remain.
    Ascend(dst []timeKey, start timeKey, to Timestamp, limit int) ([]timeKey, bool) // keys >= start with EventTime < to
    Descend(dst []timeKey, start timeKey, limit int) ([]timeKey, bool)              // keys <= start, descending

    Len() int
}
```

The method set MAY be reduced to the primitives actually used. A single `Ascend(start, to)` replaces a timestamp-only `Range`, because Range, Continue, AtAll and NextN differ only in their start key and bound.

**Two-step reads.** A time query asks the index for keys in the Series-owned `keyScratch` (capacity MaxReadBatch). The Series then copies the corresponding ring slots into the caller's dst. Index results are keys only; values are never stored in or returned by the index.

| Series query | timeIndex primitive |
|---|---|
| AsOf(ts) | AsOf(ts) |
| Before(ts) | Before(ts) |
| After(ts) | After(ts) |
| AtOrAfter(ts) | AtOrAfter(ts) |
| FirstAt(ts) | AtOrAfter(ts), then check EventTime == ts |
| LastAt(ts) | AsOf(ts), then check EventTime == ts |
| AtAll(ts) | Ascend(start {ts, 0}, to ts+1) |
| Range(from, to) | Ascend(start {from, 0}, to) |
| Continue(cursor) | Ascend(start {last.EventTime, last.Seq+1}, cursor bound). last.Seq+1 cannot overflow because MaxUint64 is never assigned. |
| PrevN(ts, n) | Descend(start {ts, MaxUint64}, n), reversed into dst |
| NextN(ts, n) | Ascend(start {ts, MaxUint64}, to MaxTimestamp, n). The start is effectively strict because MaxUint64 is never assigned, and the bound includes everything because MaxTimestamp is never stored. |
| LatestByEventTime() | Max() |
| Stats Min/MaxEventTime | Min(), Max() |
| Eviction and insertion | Apply(del, ins), or Delete/Set for a single item |
| Clear | Clear() |

## 13.3 Modes and selection

```go
type TimeIndexMode uint8

const (
    TimeIndexNone  TimeIndexMode = iota // no event-time ordering capability
    TimeIndexBTree                      // btreeTimeIndex (reference implementation)
)
```

- The mode is set in Config.TimeIndex and is immutable after New. Additional modes MAY be added later without changing the storage or subscription model.
- The zero value is TimeIndexNone, so event-time indexing is an explicit opt-in with a visible cost. A missing opt-in fails loudly: the first event-time query returns StatusTimeIndexDisabled.
- Typical TimeIndexNone uses:
  - replay or relay buffers;
  - intermediate derived series consumed only through subscriptions;
  - high-rate raw feeds whose temporal queries are served by a downstream series.
- Series.TimeIndexMode() reports the configured mode, and Stats includes it.

## 13.4 Reference implementation: btreeTimeIndex

- **Type.** The implementation wraps btree.BTreeG[timeKey] as an ordered set, with less(a, b) = a.EventTime < b.EventTime || (a.EventTime == b.EventTime && a.Seq < b.Seq). It contains exactly one key per retained record. The slot is derived from the key's Seq, so no value is stored.
- **Map is not usable.** In tidwall/btree, Map is declared as Map[K ordered, V any] [6]. A struct key does not satisfy the ordered constraint, and packing (EventTime, Seq) into one ordered scalar would need 128 bits.
- **Locking.** BTreeG uses an internal sync.RWMutex unless Options.NoLocks is true ("NoLocks will disable locking. Otherwide a sync.RWMutex is used to ensure all operations are safe across multiple goroutines.") [6]. The implementation MUST be created with btree.NewBTreeGOptions(less, btree.Options{NoLocks: true}), because the Series mutex already serializes access (TI-003).
- **Path hints.** Inserts cluster at the newest end and deletes at the oldest end. The implementation SHOULD keep two btree.PathHint values internally and use SetHint/DeleteHint inside Apply [5][6]. Hints are implementation state and never appear outside the file.
- **Iteration.** Ascend(pivot, fn) scans "[pivot, last]" and Descend(pivot, fn) scans "[pivot, first]" [6]. An IterG obtained from BTreeG.Iter() requires Release. Iteration callbacks are internal closures that only append keys to dst; they never run user code.
- **Confinement.** The tidwall/btree import appears only in time_index_btree.go (TI-001).

| Operation | Complexity |
|---|---|
| Insert (including late events) | O(log N) |
| AsOf / Before / After / AtOrAfter | O(log N) |
| Ascend (Range, Continue, NextN) | O(log N + K) |
| Descend (PrevN) | O(log N + K) |
| Delete evicted key | O(log N) |

## 13.5 No time index: TimeIndexNone

- The Series stores a nil `timeIndex`. Write paths perform only the nil check (TI-008).
- A zero-size `noTimeIndex` type MAY exist to give the common test harness and capability reporting a concrete value. If it does, its mutators are no-ops and its primitives report "absent", but the Series never routes hot-path calls through it.
- Still fully supported: Append, AppendBatch, Get, Latest, ReadFrom, subscriptions and derivers, key indexes, Clear, watermark, late detection (FlagLate and LatePolicy via MaxObservedEventTime), SourceSeq policy, eviction envelope, and Stats.MaxObservedEventTime.
- Not supported (StatusTimeIndexDisabled): every method listed in Q-016. Stats.MinEventTime and Stats.MaxEventTime are zero and documented as undefined.
- Deterministic replay is performed through ReadFrom and subscriptions, exactly as with an index.

## 13.6 Abstraction cost

- One interface dispatch per logical operation is acceptable: `Apply` per transaction, one point primitive per point query, one scan call per page.
- Implementations process batches and scans internally without further interface calls per element.
- For TimeIndexNone, the cost on the write path MUST be limited to the guard:

```go
if s.time != nil {
    s.time.Apply(s.delKeys, s.insKeys)
}
```

- Benchmarks (22.3) MUST demonstrate that the abstraction cost is measurable separately from the B-tree cost. They should show that TimeIndexNone throughput is bounded by ring and notification cost alone.

## 13.7 Future implementations

New implementations MAY be added as new TimeIndexMode values without changing the storage, replay or subscription model, provided they pass the common suite (TI-011) and satisfy TI-001..TI-013:

- **monotonicTimeIndex.** A dense append-only key array with binary search, for feeds whose EventTime is monotonic or nearly so. Late events are either rejected by the mode's contract or routed to a small side B-tree, with scans merging the two sequences.
- **blockTimeIndex.** Per-block min/max EventTime summaries over ring segments, giving coarse pruning followed by in-block scans. Useful when data is approximately ordered and memory per row must be minimal.

Each new implementation must be justified by the benchmark comparison of Section 22.3, and its continuation state must fit TimeCursor's opaque representation.

## 13.8 Why not a map or a sorted slice as the general implementation

- **A Go map has no order.** "When iterating over a map with a range loop, the iteration order is not specified and is not guaranteed to be the same from one iteration to the next. If you require a stable iteration order you must maintain a separate data structure that specifies that order." [7]
- **A time-sorted slice** is excellent when EventTime is monotonic, but costs O(N) per late insert. It is therefore a candidate only for a specialized mode (13.7), not for the general-purpose reference implementation.

# 14. Index Maintenance and Eviction

```
commit(item, seq):                           // under Series.mu
    if len == capacity:
        old = oldest
        delKeys = append(delKeys, timeKey{ring.eventTime(old), old})
        for each key index ix:
            ix.evict(slot(old), old)
        update MinEvicted/MaxEvicted, evicted counter
        oldest++
    ring.write(slot(seq), item)
    insKeys = append(insKeys, timeKey{item.EventTime, seq})
    for each key index ix:
        ix.commit(prepared[ix], slot(seq), seq)
    newest = seq

end of transaction:
    if time != nil { time.Apply(delKeys, insKeys) }
    delKeys, insKeys = delKeys[:0], insKeys[:0]

UniqueIndex.evict(slot, seq):
    if indexed[slot] && m[keys[slot]] == seq { delete(m, keys[slot]) }
    indexed[slot] = false

MultiIndex.evict(slot, seq):
    if indexed[slot]:
        p = m[keys[slot]]; assert p.front() == seq; p.popFront()
        if p.empty() { delete(m, keys[slot]) }
    indexed[slot] = false
```

When TimeIndexNone, the `delKeys`/`insKeys` appends MAY be skipped entirely.

## 14.1 Index consistency invariants

- Every retained Seq occupies exactly one slot, and Len = Newest - Oldest + 1.
- If a time index is configured, it contains exactly one key per retained Seq, with matching EventTime, and nothing else (Len() == Len).
- Every unique mapping k -> s points to a retained s whose stored key equals k and whose indexed bit is set.
- For each unique index, every retained row with the indexed bit set is the target of its key's mapping. The exception is rows superseded under ConflictReplace, whose indexed bit is cleared when superseded.
- Every posting list is non-empty, strictly increasing, and contains exactly the retained indexed Seqs with that key.
- After a commit, no reader can observe a mix of old and new state.

# 15. Retention, Expiration, and Clear

## 15.1 Retention

- Capacity-based retention is REQUIRED and is the only v1 policy.
- Age or event-time retention is deferred. If added later, it MUST be defined against a watermark, because evicting by EventTime while accepting late events is otherwise ill-defined.

## 15.2 Expired identities

Any query for an evicted Ref MUST return StatusExpired (or ErrLagged for subscriptions), never a different record.

## 15.3 Eviction envelope

- Per generation, the Series maintains HasEvicted, MinEvicted (minimum EventTime ever evicted) and MaxEvicted (maximum EventTime ever evicted).
- They are updated in O(1) per eviction from ring metadata, in every TimeIndexMode.
- They drive the Expired and Evicted rules of Section 7, and reset on Clear.

## 15.4 Clear

Clear acquires the mutex and then:
1. increments Gen;
2. zeroes the ring;
3. calls `time.Clear()` if a time index is configured;
4. resets all key indexes;
5. resets Oldest/Newest to empty, so the next Seq is 1;
6. resets the eviction envelope, MaxObservedEventTime, the SourceSeq tracker and the watermark.

It then releases the mutex and wakes all subscribers. The TimeIndexMode is unchanged.

- Clear is O(Capacity) because slots are zeroed for garbage collection.
- After Clear:
  - Get with any earlier-generation Ref returns StatusReset;
  - Continue with an earlier-generation TimeCursor returns StatusReset;
  - subscriptions return ErrReset (Section 17).
- Clear on a closed Series returns ErrClosed.

# 16. Query Consistency Model

## 16.1 Live query semantics

Each query is a point-in-time read of the current retained contents under one acquisition of the Series mutex, and sees one coherent state. Results are not historically immutable: a later late event can change the answer to the same time query.

## 16.2 Cross-consumer safety

Concurrent callers each receive independent copies. Reused dst buffers remain owned by their caller.

## 16.3 No implicit look-ahead

AsOf(ts) and PrevN(ts, n) MUST never return a record with EventTime > ts. After, AtOrAfter and NextN have distinct names so they cannot be substituted accidentally. These guarantees hold for every time-index implementation.

## 16.4 Deferred: snapshots and known-as-of queries

A Snapshot API (a consistent multi-query view) and AsOf with a Seq cutoff ("what was known by Seq s") are deferred to v2. A correct known-as-of query needs either per-record visibility filtering (O(K) scans) or versioned indexes. Deterministic replay via ReadFrom and subscriptions already covers backtest/live parity in v1.

# 17. Subscriptions

## 17.1 Requirements

| ID | Requirement | Priority | Acceptance / rationale |
|---|---|---|---|
| S-001 | Subscribe(opts) returns a *Subscription[T] with a Cursor (Gen, Next), where Next is the next Seq to read. | MUST | Unambiguous cursor semantics. |
| S-002 | Start positions: StartOldest (Oldest at subscribe time), StartNext (Newest + 1: only future records), StartAt(ref) (ref.Seq; validated on first Next). | MUST | Explicit starting position. |
| S-003 | Next(ctx, dst, limit) blocks until at least one record is available, ctx is done, the subscription or Series is closed, or a lag/reset condition occurs. It returns up to the effective limit entries in ascending Seq and advances the cursor. | MUST | Batch drain by Seq. |
| S-004 | Lag: if Cursor.Next < Oldest, then with LagFail (default) Next returns no data and an error matching ErrLagged (*LagError{Cursor, Oldest, Missed}) and does not move the cursor. With LagSkip the cursor jumps to Oldest, Missed is added to Skipped(), and data is returned without error. | MUST | E-008, deterministic lag. |
| S-005 | A single Next call never returns partial data together with a lag error. Lag is evaluated under the same mutex acquisition as the copy. | MUST | Deterministic consumer state. |
| S-006 | Reset: if Cursor.Gen < Gen, Next returns ErrReset until the caller calls Seek. Seek(StartOldest/StartNext/StartAt) moves the cursor into the current generation. | MUST | Explicit resynchronization after Clear. |
| S-007 | Notifications are coalesced and never block the producer. | MUST | Slow consumers cannot stall ingest. |
| S-008 | Close() is idempotent and may be called concurrently with a blocked Next (which then returns ErrClosed). It removes the subscriber from the notify list. | MUST | Unsubscribe cleanup. |
| S-009 | After Series.Close, Next drains the remaining retained records, then returns ErrClosed. | MUST | Graceful shutdown. |
| S-010 | TryNext(dst, limit) returns immediately (possibly with zero entries and a nil error). | SHOULD | Polling and select-based consumers. |
| S-011 | Wake() exposes the subscriber's wake channel for use in select loops. | SHOULD | Integration with other event sources. |
| S-012 | A Subscription is used by one goroutine at a time (Close excepted). | MUST | Simple cursor ownership. |
| S-013 | Subscriptions use only the ring, bounds and generation. Their behavior and cost are identical in every TimeIndexMode. | MUST | Q-017, TI-010. |

## 17.2 Next algorithm

```
loop:
    Lock
    if closedSub: Unlock; return ErrClosed
    if cur.Gen < gen: Unlock; return ErrReset
    if cur.Next < oldest (and Len > 0):
        if LagFail: Unlock; return &LagError{cur, Ref{gen, oldest}, oldest - cur.Next}
        skipped += oldest - cur.Next; cur.Next = oldest
    if cur.Next <= newest:
        copy [cur.Next, min(newest, cur.Next + limit - 1)] into dst (Clone OnRead applies)
        cur.Next = last + 1
        Unlock; return dst, nil
    if seriesClosed: Unlock; return ErrClosed
    Unlock
    select { case <-wake: continue; case <-ctx.Done(): return ctx.Err(); case <-subClosed: return ErrClosed }
```

StartAt(ref) behavior:
- ref.Gen < Gen yields ErrReset.
- ref.Seq > Newest + 1 with ref.Gen == Gen is clamped to Newest + 1, so the subscription waits for future data.

## 17.3 Notification mechanism

- Each subscriber owns a wake chan struct{} with buffer 1.
- After releasing the mutex, Append, AppendBatch, Clear and Close load the copy-on-write subscriber slice (atomic.Pointer) and perform a non-blocking send to each wake channel. This is O(subscribers), allocation-free and coalescing: a full buffer means a wake-up is already pending.
- A lost wake-up is impossible. A consumer re-checks state under the mutex after every wake, and any commit after its check leaves a token in the buffer. Spurious wake-ups are harmless.
- Subscribe and Close replace the subscriber slice, so allocation happens only on subscribe and unsubscribe.
- Watermark changes do not wake Next, because Next returns only records. See Section 19 for readiness.

# 18. Derived Series

## 18.1 Requirements

| ID | Requirement | Priority | Acceptance / rationale |
|---|---|---|---|
| D-001 | StartDerived runs a worker goroutine that consumes a source subscription in Seq order and appends outputs to a destination Series. | SHOULD | Managed derivation. |
| D-002 | The derive function runs outside all Series locks and may read other series. | MUST | No coupling to ingest latency; no deadlocks. |
| D-003 | Outputs produced from one input batch are appended with one AppendBatch. The processed cursor advances only after that append succeeds. | MUST | No silent cursor advance on failure. |
| D-004 | On a derive-function error, append error, ErrLagged (LagFail) or ErrReset, the worker stops, records the error, and does not advance the cursor past the failed batch. | MUST | Deterministic failure. |
| D-005 | The processed cursor is in-memory only in v1. Checkpoint() returns it so applications can persist it externally. | MUST | No durability claim in v1. |
| D-006 | Rebuild is deterministic given the source records from a cursor, the derivation Version and the configuration. The worker records Version in its status. | MUST | Audit and replay. |
| D-007 | The worker exposes status: state, processed cursor, lag in records (source Newest - processed), lag in time (source MaxObservedEventTime - last processed EventTime), last error, and version. | MUST | Freshness monitoring. |
| D-008 | Derivation works regardless of the TimeIndexMode of the source or destination. Source and destination modes are chosen independently. | MUST | Subscriptions do not depend on the time index (S-013). |

## 18.2 Shape

```go
type DeriveFunc[In, Out any] func(batch []Entry[In], out []Input[Out]) ([]Input[Out], error)

type DeriveOptions struct {
    Name      string
    Version   string    // derivation algorithm/config version, recorded in status
    Start     StartFrom // default StartOldest
    BatchSize int       // default MaxReadBatch of the source
    OnLag     LagPolicy // default LagFail
    Watermark WatermarkMode
}

type WatermarkMode uint8

const (
    WatermarkPropagate WatermarkMode = iota // default: see 18.4
    WatermarkManual                         // derive function owner calls dst.SetWatermark
)

func StartDerived[In, Out any](ctx context.Context, src *Series[In], dst *Series[Out],
    fn DeriveFunc[In, Out], opts DeriveOptions) (*Deriver, error)
```

- The derive function owns its algorithm state. A failed batch may have partially updated that state, so D-004 stops the worker. Resuming requires a rebuild from a checkpoint or from source data, not a retry of the same batch.
- Intermediate derived series that are consumed only by further subscriptions SHOULD use TimeIndexNone. Series that strategy code queries by event time use TimeIndexBTree.

## 18.3 Provenance and output metadata

- The derive function produces the output InputMeta. Recommended mapping:
  - EventTime: the derivation's semantic time (for example candle close time, or the EventTime of the last contributing input);
  - ReceiveTime: local time of derivation;
  - SourceSeq: Seq of the last contributing source record;
  - SourceEpoch: SourceEpoch(source Gen);
  - the low 16 adapter flag bits may carry derivation-specific flags.
- If the full contributing range is required, Out SHOULD carry FirstSourceSeq/LastSourceSeq fields. Meta stays fixed-size for all series.

## 18.4 Watermark propagation

With WatermarkPropagate, after a successful batch whose last record equals the source Newest observed in the same read, the worker sets the destination watermark to the source watermark captured in that read (monotonic; Section 19). Otherwise the destination watermark is unchanged.

## 18.5 Late events

- Process raw events in Seq order and treat EventTime as data.
- Algorithms that need chronological order MUST implement an explicit reorder buffer with a watermark in the derivation layer.
- Corrections are emitted as new derived records (optionally with FlagCorrection), not as in-place rewrites.

# 19. Watermarks and Readiness

| Concept | Definition | API |
|---|---|---|
| MaxEventTime | Largest EventTime currently retained (time index only). | Stats().MaxEventTime, LatestByEventTime() |
| MaxObservedEventTime | Largest EventTime accepted in this generation (survives eviction; every mode). | Stats().MaxObservedEventTime |
| Processed cursor | Last source Seq whose outputs a deriver has committed. | Deriver.Status().Processed |
| Watermark | Time through which the owner declares the series complete under its policy. | SetWatermark(ts) bool, Watermark() Timestamp |
| SourceHealthy | Adapter-specific feed health. | Outside the Series (adapter). |

- Exactly one writer owns the watermark: the adapter for a raw series, the deriver for a derived series. Watermarks work in every TimeIndexMode.
- SetWatermark is monotonic: a value <= the current watermark is ignored and returns false. The initial value, and the value after Clear, is MinTimestamp.
- With Config.GateReadsByWatermark = true, time-based reads whose upper bound exceeds the watermark return StatusNotReady with no data:
  - AsOf, Before, LastAt, FirstAt, AtAll and PrevN when ts > watermark;
  - Range when to - 1 > watermark;
  - Continue when its cursor's bound - 1 > watermark;
  - After, AtOrAfter and NextN always, because they look forward.
- On TimeIndexNone series these reads return StatusTimeIndexDisabled first (7.5). Seq-based reads, Latest and key lookups are never gated.
- Multi-series readiness ("evaluate candle T only when all required series have watermark >= T") belongs to a higher-level strategy context.

# 20. Error and Edge Semantics

| ID | Requirement | Priority | Acceptance / rationale |
|---|---|---|---|
| E-001 | Get of a non-retained Ref returns NotFound (never assigned), Expired (evicted in this generation) or Reset (older generation). | MUST | 7.2. |
| E-002 | Range with from >= to returns zero results, StatusNotFound, no error. | MUST | Half-open intervals. |
| E-003 | AsOf with no retained match returns Expired iff an evicted record of this generation would have matched, otherwise NotFound. | MUST | 7.2, 15.3. |
| E-004 | Duplicate EventTime values are supported and ordered by Seq. | MUST | 6.5. |
| E-005 | Duplicate SourceSeq values are representable; flagging or rejection happens only by explicit policy. | MUST | 9.2. |
| E-006 | SourceSeq gaps do not alter the Series; adapters signal them with FlagGap. | MUST | 6.3. |
| E-007 | Any failed Append/AppendBatch leaves no state change, including after a recovered callback panic. | MUST | 9.3. |
| E-008 | Subscriber lag beyond retention is detected deterministically and reports the cursor, the oldest retained Ref and the number missed. | MUST | 17. |
| E-009 | EventTime equal to MinTimestamp or MaxTimestamp is rejected with ErrInvalidInput. | MUST | 6.1. |
| E-010 | Series-owned flag bits in InputMeta are rejected with ErrInvalidInput. | MUST | 6.3. |
| E-011 | A unique conflict under ConflictReject returns *ConflictError identifying the index and the existing Ref. | MUST | 10.3. |
| E-012 | A key equal to that of a row evicted by the same transaction is not a conflict. | MUST | 10.3. |
| E-013 | Operations on a closed Series: writes and Clear fail with ErrClosed; reads keep working on retained data; subscriptions drain, then return ErrClosed. | MUST | 17. |
| E-014 | Next with a cancelled ctx returns ctx.Err() without moving the cursor. | MUST | 17. |
| E-015 | Any event-time query on a TimeIndexNone series returns StatusTimeIndexDisabled with no data. This applies even when the series is empty or the watermark gate would apply. | MUST | 7.5, Q-016. |
| E-016 | Continue with a zero or foreign TimeCursor returns StatusInvalid; with an earlier-generation cursor, StatusReset. | MUST | 8.4. |

# 21. Public API Sketch

```go
package temporal

import (
    "context"
    "iter"
    "math"
    "time"
)

// ---------- identity and time ----------

type Seq uint64

const SeqNone Seq = 0 // invalid; first assigned Seq is 1; math.MaxUint64 is never assigned

type Gen uint32

type Ref struct {
    Gen Gen
    Seq Seq
}

func (r Ref) IsZero() bool { return r.Seq == SeqNone }

type Timestamp int64 // Unix nanoseconds, UTC

const (
    MinTimestamp Timestamp = math.MinInt64 // reserved sentinel
    MaxTimestamp Timestamp = math.MaxInt64 // reserved sentinel
)

func FromTime(t time.Time) Timestamp
func (ts Timestamp) Time() time.Time

type SourceEpoch uint32
type Flags uint32

const (
    FlagGap          Flags = 1 << 0
    FlagSnapshot     Flags = 1 << 1
    FlagReplay       Flags = 1 << 2
    FlagCorrection   Flags = 1 << 3
    AdapterFlagsMask Flags = 0x0000FFFF

    FlagLate                   Flags = 1 << 16
    FlagSourceSeqNotIncreasing Flags = 1 << 17
    SeriesFlagsMask            Flags = 0xFFFF0000
)

type InputMeta struct { /* Section 6.4 */ }
type Meta struct { /* Section 6.4 */ }
type Entry[T any] struct { Meta Meta; Value T }
type Input[T any] struct { Value T; Meta InputMeta }

// TimeCursor is an opaque continuation token for time-ordered reads (Section 8.4).
type TimeCursor struct { /* private, implementation-defined */ }
func (c TimeCursor) IsZero() bool

// ---------- status and errors ----------

type Status uint8

const (
    StatusOK Status = iota
    StatusNotFound
    StatusExpired
    StatusReset
    StatusNotReady
    StatusTimeIndexDisabled
    StatusInvalid
)

func (s Status) String() string
func (s Status) Err() error

type ReadInfo struct { /* Section 7.4 */ }

var (
    ErrNotFound, ErrExpired, ErrReset, ErrNotReady error
    ErrTimeIndexDisabled                           error
    ErrLagged, ErrClosed                           error
    ErrInvalidConfig, ErrInvalidInput              error
    ErrLate, ErrSourceSeqNotIncreasing             error
    ErrUniqueConflict, ErrBatchTooLarge            error
    ErrCallbackPanic, ErrSeqExhausted              error
    ErrGenExhausted, ErrIndexBound                 error
)

type LagError struct {
    Cursor Cursor
    Oldest Ref
    Missed uint64
}
func (e *LagError) Error() string
func (e *LagError) Is(target error) bool // target == ErrLagged

type ConflictError struct {
    Index    string
    Key      string // fmt %v of the key, for diagnostics only
    Existing Ref
    BatchPos int
}

type CallbackPanicError struct {
    Callback string // "extractor:<index>" or "clone"
    Value    any
    Stack    []byte
}

// ---------- configuration ----------

type TimeIndexMode uint8

const (
    TimeIndexNone TimeIndexMode = iota
    TimeIndexBTree
)

type LatePolicy uint8

const (
    LateAccept LatePolicy = iota
    LateReject
)

type SourceSeqPolicy uint8

const (
    SourceSeqIgnore SourceSeqPolicy = iota
    SourceSeqFlag
    SourceSeqReject
)

type CloneMode uint8

const (
    CloneOnRead CloneMode = iota
    CloneOnWrite
    CloneOnReadWrite
)

type Config[T any] struct {
    Name                 string
    Capacity             int           // required, >= 1
    TimeIndex            TimeIndexMode // zero value = TimeIndexNone; immutable after New
    MaxReadBatch         int           // 0 = default 4096
    Clone                func(T) T     // optional
    CloneMode            CloneMode
    LatePolicy           LatePolicy
    LateTolerance        time.Duration // 0 = any EventTime below the max is late
    SourceSeqPolicy      SourceSeqPolicy
    GateReadsByWatermark bool
    Indexes              []Index[T]
}

func New[T any](cfg Config[T]) (*Series[T], error)

// ---------- series ----------

type Series[T any] struct { /* private */ }

// writes
func (s *Series[T]) Append(v T, m InputMeta) (AppendResult, error)
func (s *Series[T]) AppendBatch(dst []AppendResult, items []Input[T]) ([]AppendResult, error)
func (s *Series[T]) SetWatermark(ts Timestamp) bool
func (s *Series[T]) Clear() error
func (s *Series[T]) Close() error

// Seq-based reads (every TimeIndexMode)
func (s *Series[T]) Get(ref Ref) (Entry[T], Status)
func (s *Series[T]) Latest() (Entry[T], Status)
func (s *Series[T]) ReadFrom(dst []Entry[T], from Ref, limit int) ([]Entry[T], ReadInfo)

// event-time point reads (StatusTimeIndexDisabled when TimeIndexNone)
func (s *Series[T]) LatestByEventTime() (Entry[T], Status)
func (s *Series[T]) AsOf(ts Timestamp) (Entry[T], Status)
func (s *Series[T]) Before(ts Timestamp) (Entry[T], Status)
func (s *Series[T]) After(ts Timestamp) (Entry[T], Status)
func (s *Series[T]) AtOrAfter(ts Timestamp) (Entry[T], Status)
func (s *Series[T]) FirstAt(ts Timestamp) (Entry[T], Status)
func (s *Series[T]) LastAt(ts Timestamp) (Entry[T], Status)

// event-time batch reads (append to dst; bounded by min(limit, MaxReadBatch))
func (s *Series[T]) AtAll(dst []Entry[T], ts Timestamp, limit int) ([]Entry[T], ReadInfo)
func (s *Series[T]) Range(dst []Entry[T], from, to Timestamp, limit int) ([]Entry[T], ReadInfo)
func (s *Series[T]) RangeValues(dst []T, from, to Timestamp, limit int) ([]T, ReadInfo)
func (s *Series[T]) Continue(dst []Entry[T], c TimeCursor, limit int) ([]Entry[T], ReadInfo)
func (s *Series[T]) ContinueValues(dst []T, c TimeCursor, limit int) ([]T, ReadInfo)
func (s *Series[T]) PrevN(dst []Entry[T], ts Timestamp, n int) ([]Entry[T], ReadInfo)
func (s *Series[T]) NextN(dst []Entry[T], ts Timestamp, n int) ([]Entry[T], ReadInfo)
func (s *Series[T]) Entries(from, to Timestamp) (iter.Seq[Entry[T]], Status) // paged; never yields under lock

// bounds and diagnostics
func (s *Series[T]) Len() int
func (s *Series[T]) Cap() int
func (s *Series[T]) Gen() Gen
func (s *Series[T]) TimeIndexMode() TimeIndexMode
func (s *Series[T]) OldestRef() Ref
func (s *Series[T]) NewestRef() Ref
func (s *Series[T]) Watermark() Timestamp
func (s *Series[T]) Stats() Stats
func (s *Series[T]) CheckInvariants() error // full rebuild-and-compare; tests/debug

// subscriptions (every TimeIndexMode)
func (s *Series[T]) Subscribe(opts SubscribeOptions) (*Subscription[T], error)

// ---------- key indexes ----------

type ConflictPolicy uint8

const (
    ConflictReject ConflictPolicy = iota
    ConflictDrop
    ConflictReplace
    ConflictKeepFirst
)

type Order uint8

const (
    Ascending Order = iota
    Descending
)

type Extractor[T any, K comparable] func(e *Entry[T]) (key K, ok bool)

type Index[T any] interface { /* unexported methods; Section 10.2 */ }

type UniqueIndex[T any, K comparable] struct { /* private: map[K]Seq, keys []K, indexed bitset */ }
func NewUniqueIndex[T any, K comparable](name string, fn Extractor[T, K], p ConflictPolicy) *UniqueIndex[T, K]
func (ix *UniqueIndex[T, K]) Get(k K) (Entry[T], Status)
func (ix *UniqueIndex[T, K]) Contains(k K) bool
func (ix *UniqueIndex[T, K]) Len() int

type MultiIndex[T any, K comparable] struct { /* private: map[K]*postings, keys []K, indexed bitset */ }
func NewMultiIndex[T any, K comparable](name string, fn Extractor[T, K]) *MultiIndex[T, K]
func (ix *MultiIndex[T, K]) Get(dst []Entry[T], k K, limit int, o Order) ([]Entry[T], ReadInfo)
func (ix *MultiIndex[T, K]) Latest(k K) (Entry[T], Status)
func (ix *MultiIndex[T, K]) Count(k K) int
func (ix *MultiIndex[T, K]) Keys() int

// ---------- subscriptions ----------

type StartFrom struct { /* private */ }
func StartOldest() StartFrom
func StartNext() StartFrom
func StartAt(ref Ref) StartFrom

type LagPolicy uint8

const (
    LagFail LagPolicy = iota
    LagSkip
)

type SubscribeOptions struct {
    Name  string
    Start StartFrom // zero value = StartNext
    OnLag LagPolicy
}

type Cursor struct {
    Gen  Gen
    Next Seq // next Seq to read
}

type Subscription[T any] struct { /* private */ }
func (s *Subscription[T]) Next(ctx context.Context, dst []Entry[T], limit int) ([]Entry[T], error)
func (s *Subscription[T]) TryNext(dst []Entry[T], limit int) ([]Entry[T], error)
func (s *Subscription[T]) Seek(from StartFrom) error
func (s *Subscription[T]) Cursor() Cursor
func (s *Subscription[T]) Skipped() uint64
func (s *Subscription[T]) Wake() <-chan struct{}
func (s *Subscription[T]) Close() error

// ---------- statistics ----------

type IndexStats struct {
    Name      string
    Unique    bool
    Keys      int
    Conflicts uint64 // rejected
    Dropped   uint64
    Replaced  uint64
    KeptFirst uint64
}

type Stats struct {
    Name                   string
    Gen                    Gen
    TimeIndex              TimeIndexMode
    Capacity, Len          int
    Oldest, Newest         Ref
    Appended, Evicted      uint64 // lifetime
    Dropped, Rejected      uint64 // lifetime
    Late, SourceSeqNotIncr uint64 // flagged records, lifetime
    GapFlags               uint64 // records carrying FlagGap, lifetime
    Clears                 uint64
    MinEventTime           Timestamp // retained; defined only when Len > 0 and TimeIndex != TimeIndexNone
    MaxEventTime           Timestamp // retained; defined only when Len > 0 and TimeIndex != TimeIndexNone
    MaxObservedEventTime   Timestamp // this generation; every mode
    HasEvicted             bool
    MinEvicted, MaxEvicted Timestamp // this generation
    Watermark              Timestamp
    Subscribers            int
    Indexes                []IndexStats
}
```

Stats is not a hot-path call and may allocate (the Indexes slice). Per-read query counters are intentionally omitted to keep read paths free of extra bookkeeping.

# 22. Performance Requirements and Benchmark Plan

No absolute latency target is fixed before representative feeds and hardware are measured.

## 22.1 Workload matrix

| Dimension | Values |
|---|---|
| Time-index mode | TimeIndexNone, TimeIndexBTree (plus future modes as they are added). |
| T size | 16B, 32B, 64B, 128B, 256B; plus a reference-heavy type with Clone. |
| Batch read size | 1, 8, 64, 256, 1K, 4K (MaxReadBatch), plus paged scans of 16K+. |
| Capacity | 1K, 64K, 1M, 10M where memory permits; include a non-power-of-two capacity. |
| Readers | 1, 2, 8, 32 concurrent. |
| Writers | 1 hot writer; multi-goroutine Append contention separately; AppendBatch sizes 8/64/256/1024. |
| Event-time disorder | 0%, 0.1%, 1%, 10%, adversarial reverse burst. |
| Timestamp ties | 0%, 1%, 10%, 100% within micro-batches. |
| Key indexes | none; 1 unique; 1 unique + 1 multi; high- vs low-cardinality keys. |
| Queries | Get, Latest, ReadFrom, subscriber drain, AsOf, Before, After, Range (+Continue), PrevN, NextN, unique Get, multi Get. |
| Allocation mode | nil dst vs preallocated dst. |

## 22.2 Acceptance metrics

- In steady state, without Clone, these allocate zero times: Get, Latest, AsOf, Before, After, FirstAt, LastAt and UniqueIndex.Get.
- Batch reads, including Continue, allocate zero times when cap(dst) suffices. Index results use the Series-owned key scratch.
- With TimeIndexNone and no key indexes, Append and AppendBatch allocate zero times. With TimeIndexBTree, the only allocations are amortized B-tree growth. Notification allocates nothing.
- Writer latency percentiles (p50/p99) are reported as a function of reader count, batch size and MaxReadBatch.
- Subscriber notification cost is independent of payload size and does not copy T.
- Memory overhead per retained row is reported for each time-index implementation and each key index (including the per-slot key array).

## 22.3 Time-index comparison (mandatory)

Benchmarks MUST compare at least TimeIndexNone and TimeIndexBTree on:

```text
Append
AppendBatch(8/64/256/1024)
ReadFrom
Subscription.Next
AsOf          (BTree only; None verifies StatusTimeIndexDisabled cost)
Range         (BTree only; None verifies StatusTimeIndexDisabled cost)
```

and report for each:

```text
records/s
ns/op
allocs/op
B/op
p50/p99 write latency
```

- The None/BTree difference on Append, AppendBatch, ReadFrom and Subscription.Next isolates the cost of event-time indexing from storage and ingestion.
- ReadFrom and Subscription.Next MUST show no statistically significant difference between modes (Q-017).
- Future implementations (13.7) are added to this matrix together with the disorder and tie dimensions of 22.1.
- google/btree is not a candidate dependency: its repository was archived by the owner on 24 October 2025 and is read-only [10].

## 22.4 Lock comparison (informative)

An experimental RWMutex variant MAY be benchmarked against the Mutex baseline under the same matrix (readers 1/2/8/32, batch sizes up to MaxReadBatch, CloneOnRead on/off). It is adopted only under the conditions of Section 11.6.

# 23. Verification, Testing, and Robustness Requirements

| ID | Requirement | Priority | Acceptance / rationale |
|---|---|---|---|
| T-001 | Run go test -race for the whole package and all concurrency tests. | MUST | The Go Data Race Detector article states it "only finds races that happen at runtime, so it can't find races in code paths that are not executed", so concurrency tests must exercise real interleavings [4]. |
| T-002 | Property-test the Series against a reference model (23.2), in every TimeIndexMode. | MUST | Catches wraparound and eviction inconsistencies. |
| T-003 | Fuzz arbitrary sequences of Append/AppendBatch/queries/Clear/Subscribe/Continue with capacities 1..8 and long overwrite runs. | MUST | Edge-heavy coverage. |
| T-004 | Duplicate timestamps, late events, sentinel timestamps. | MUST | Ordinary market-data cases. |
| T-005 | Concurrent callers copying while a writer overwrites. | MUST | No races, no torn values. |
| T-006 | Reference-heavy T with each CloneMode, verifying deep ownership. | SHOULD | Clone semantics. |
| T-007 | Subscriber replay across batches, LagFail and LagSkip. | MUST | Deterministic consumer state. |
| T-008 | Deterministic record/replay fixtures from real feed captures. | SHOULD | Event-time/sequence semantics, derivation. |
| T-009 | Heap/GC profiles under sustained ingestion. | MUST | Allocation sensitivity. |
| T-010 | CheckInvariants rebuild-and-compare after random operations. | SHOULD | Proves index consistency. |
| T-011 | Unique collisions under every ConflictPolicy, single and batch, including intra-batch duplicates and multiple indexes. | MUST | 10.3. |
| T-012 | Collision with the row being evicted in the same transaction (capacity 1 and full ring). | MUST | E-012. |
| T-013 | Eviction under Replace/KeepFirst never removes a newer mapping. | MUST | K-008. |
| T-014 | Extractor and Clone panics during Append/AppendBatch leave state unchanged and return *CallbackPanicError. | MUST | E-007. |
| T-015 | A Clone panic during a read does not leave the mutex held (a subsequent Append succeeds). | MUST | C-007. |
| T-016 | Generation semantics: Get with an old Ref returns Reset; Continue with an old cursor returns Reset; subscriptions return ErrReset until Seek; Seq restarts at 1. | MUST | 15.4. |
| T-017 | Status matrix of Section 7.2 for every point query, including Expired exactness for AsOf/Before/After/AtOrAfter, and the precedence of 7.5. | MUST | Q-012. |
| T-018 | Notification coalescing: many appends produce bounded wake-ups; no lost wake-ups under stress; the producer never blocks with stalled consumers. | MUST | 17.3. |
| T-019 | Cancellation and close: ctx cancel, Subscription.Close and Series.Close each unblock Next with the specified error and the cursor unchanged. | MUST | E-013, E-014. |
| T-020 | Watermark monotonicity and NotReady gating; deriver failure does not advance its cursor. | MUST | 18, 19. |
| T-021 | Common timeIndex semantic suite, run against every implementation and against a test-only reference implementation (a sorted slice). It checks every primitive (Set, Delete, Apply, AsOf, Before, After, AtOrAfter, Min, Max, Ascend, Descend, Len, Clear) under disorder, ties, eviction patterns and limits. | MUST | TI-011. |
| T-022 | Mode equivalence: identical input streams applied to a TimeIndexNone series and a TimeIndexBTree series yield identical AppendResults, Refs, statuses and results for every Seq-based read, key lookup and subscription, and identical Stats except the time-index-only fields. | MUST | TI-010, Q-017. |
| T-023 | On TimeIndexNone, every method in Q-016 returns StatusTimeIndexDisabled with no data and no state change, including on empty series and with the watermark gate enabled. | MUST | E-015. |
| T-024 | TimeCursor continuation: pages concatenate to the full Range result when there is no concurrent mutation; late inserts behind the cursor are not returned; eviction between pages is reflected in ReadInfo.Evicted; zero or foreign cursors return Invalid; an old-generation cursor returns Reset. | MUST | 8.4, E-016. |
| T-025 | Structural checks in CI: (a) only time_index_btree.go imports github.com/tidwall/btree; (b) no time-index or key-index implementation declares a sync.Mutex, sync.RWMutex or atomic field; (c) the B-tree is constructed with NoLocks: true. | MUST | TI-001, C-002. |
| T-026 | Allocation tests: zero allocations for TimeIndexNone Append/AppendBatch without key indexes, and for steady-state reads per 22.2. | MUST | W-009, 22.2. |

The Go Data Race Detector article notes that "for a typical program, memory usage may increase by 5-10x and execution time by 2-20x" [4]. Race-enabled stress tests should therefore use reduced sizes.

## 23.1 Common time-index suite

- The suite is written once against the internal timeIndex interface.
- It is instantiated for btreeTimeIndex, for every future implementation, and for a test-only sliceTimeIndex reference (a sorted []timeKey with linear insert and delete).
- For each random operation sequence, all implementations must return identical results.
- At the Series level, the same scenario suite runs in every TimeIndexMode. Event-time expectations apply to modes with an ordering capability; for TimeIndexNone the expectation is StatusTimeIndexDisabled (T-023), and Seq-level expectations are identical (T-022).

## 23.2 Reference model

- The oracle keeps a plain []Entry[T] in Seq order, plus the generation, eviction envelope and naive per-index maps.
- Expected results, including Status and ReadInfo flags, are computed by linear scans and sorting.
- It is used only in tests.

## 23.3 Invariants asserted after random operations

- Len = Newest - Oldest + 1 (or 0), and Len <= Capacity.
- If a time index is configured: its Len equals the Series Len, and every key maps to a retained Seq with matching EventTime.
- Temporal results are sorted by (EventTime, Seq), and Seq replay is strictly increasing.
- The unique and multi index invariants of Section 14.1 hold.
- After Clear, old Refs and old TimeCursors return Reset, and the next Seq is 1.

# 24. Operational Model for Market-Data Ingestion

```
venue WebSocket / UDP / FIX / file replay
          |
          v
feed adapter
  - decode venue format
  - validate SourceSeq / detect gaps (venue rules)
  - reconnect / snapshot recovery, new SourceEpoch per session
  - assign ReceiveTime, normalize EventTime to Unix ns UTC
  - set adapter flags (FlagGap, FlagSnapshot, FlagReplay)
  - SetWatermark when the venue/adapter can declare completeness
          |
          v
Series[T].Append / AppendBatch      (TimeIndexMode chosen per series)
          |
          +--> Seq reads / ReadFrom / subscriptions   (every mode)
          +--> event-time queries                     (TimeIndexBTree)
          +--> derived series
```

The Series does not know Binance update-ID rules, Coinbase sequence numbers, FIX sequence numbers or any other venue protocol. It stores normalized metadata and exposes it for diagnostics. Choose the TimeIndexMode per series according to whether anything queries it by event time.

# 25. End-to-End Usage Example

```go
type Trade struct {
    ID    uint64
    Price int64
    Qty   int64
    Side  uint8
}

byID := temporal.NewUniqueIndex[Trade, uint64]("trade_id",
    func(e *temporal.Entry[Trade]) (uint64, bool) { return e.Value.ID, true },
    temporal.ConflictDrop) // idempotent re-ingestion after reconnect

trades, err := temporal.New(temporal.Config[Trade]{
    Name:      "btcusdt.trades",
    Capacity:  2_000_000,
    TimeIndex: temporal.TimeIndexBTree, // strategy code queries this series by event time
    Indexes:   []temporal.Index[Trade]{byID},
})
if err != nil { /* handle */ }

res, err := trades.Append(Trade{ID: 91827364, Price: 104250, Qty: 3, Side: Buy}, temporal.InputMeta{
    EventTime:   1760000000123456789,
    ReceiveTime: 1760000000124000000,
    SourceSeq:   91827364,
    SourceEpoch: 42,
})
// res.Outcome == OutcomeAppended, res.Ref identifies the record

last, st := trades.AsOf(candleTime)
switch st {
case temporal.StatusOK:
    _ = last
case temporal.StatusExpired:
    // data for candleTime was evicted: widen capacity or rebuild from history
case temporal.StatusNotFound:
    // nothing at or before candleTime has ever been retained in this generation
case temporal.StatusTimeIndexDisabled:
    // configuration error: this series has no event-time index
}

buf := make([]temporal.Entry[Trade], 0, 256)
buf, info := trades.PrevN(buf[:0], candleTime, 100) // ascending, last element == AsOf
buf, info = trades.Range(buf[:0], candleTime-60e9, candleTime, 0)
for !info.Next.IsZero() {
    buf, info = trades.Continue(buf, info.Next, 0) // opaque continuation, no index types exposed
}

t, st := byID.Get(91827364)

// A replay-only relay series: no event-time index, Seq semantics unchanged.
relay, _ := temporal.New(temporal.Config[Trade]{Name: "relay", Capacity: 1 << 20}) // TimeIndexNone
_, st = relay.AsOf(candleTime) // st == StatusTimeIndexDisabled

sub, _ := trades.Subscribe(temporal.SubscribeOptions{Start: temporal.StartOldest()})
defer sub.Close()
batch := make([]temporal.Entry[Trade], 0, 1024)
for {
    var err error
    batch, err = sub.Next(ctx, batch[:0], 0)
    var lag *temporal.LagError
    switch {
    case err == nil:
        // process batch
    case errors.As(err, &lag):
        // continuity lost: rebuild from checkpoint, then sub.Seek(temporal.StartOldest())
    case errors.Is(err, temporal.ErrReset):
        // series cleared: resync, then sub.Seek(temporal.StartOldest())
    default:
        return // ctx cancelled or closed
    }
}
```

# 26. Proposed Package Structure

```
temporal/
    series.go              // Series[T], Config, New, write path, Clear/Close
    types.go               // Seq, Gen, Ref, Timestamp, Flags, InputMeta, Meta, Entry, timeKey
    status.go              // Status, ReadInfo, TimeCursor, errors
    ring.go                // internal bounded ring, implemented from scratch; no public slices, no lock
    time_index.go          // timeIndex interface, TimeIndexMode, construction by mode
    time_index_btree.go    // btreeTimeIndex: the only file importing tidwall/btree
    time_index_none.go     // optional zero-size noTimeIndex (tests / capability reporting)
    key_index.go           // Index[T] interface, UniqueIndex, MultiIndex, postings
    query.go               // point/batch reads, Continue, Entries pager
    subscription.go        // Subscription, cursor, wake channels
    derive.go              // StartDerived, Deriver
    watermark.go           // watermark and read gating
    stats.go               // Stats, IndexStats
    invariants.go          // CheckInvariants
    time_index_test.go     // common timeIndex semantic suite + sliceTimeIndex reference
    bench_test.go          // reproducible performance suite (incl. None vs BTree matrix)
    fuzz_test.go           // fuzz/property tests
    model_test.go          // reference model
    structure_test.go      // import and lock-field structural checks (T-025)
```

The public Series API is the only boundary. The ring and every time-index implementation are unexported implementation details.

# 27. Reliability and Failure Handling

- Allocation failure and runtime faults are process-level; the library does not attempt recovery from them.
- Only callbacks in the prepare phase have recovered panics (with no state change). Read-path panics propagate after the mutex is released.
- Time-index implementations never execute user code and therefore never panic on valid inputs. Debug builds assert Apply/Delete preconditions (every deleted key exists; no inserted key exists).
- Derivers stop on error and never silently skip records.
- Diagnostics cover:
  - appends, evictions, drops and rejections;
  - late records, SourceSeq-not-increasing records and adapter gap flags;
  - clears and subscriber count;
  - per-index conflict counters;
  - subscriber Skipped() and deriver lag;
  - the time-index mode.
- CheckInvariants is available for tests and debug builds.
- Append never starts goroutines; subscribers and derivers own their goroutines.

# 28. Dependencies and Library Assessment

| Component | Choice | Notes |
|---|---|---|
| Bounded storage | Internal ring, implemented from scratch | Optimized for this Series contract; no external ring dependency. |
| Event-time ordering (TimeIndexBTree) | github.com/tidwall/btree (BTreeG), confined to time_index_btree.go | v1.8.2 is the current release [6]. Provides generics, Map/Set/BTreeG types, copy-on-write Copy, path hints, Seek/Ascend/Descend and iterators [5][6]. Use BTreeG with NoLocks: true. The Series core does not import it (TI-001). |
| Event-time ordering (TimeIndexNone) | None | No dependency, no per-record work beyond a nil check. |
| Key indexes | Built-in Go map | Default for maturity and simplicity [7][9]. |
| Alternative hash map | github.com/tidwall/hashmap | Benchmark candidate only [8]. |
| Alternative B-tree | github.com/tidwall/btype | B-tree based Map/Set/Array/Table/Queue collections. A younger project with smaller adoption; consider only for future benchmarks behind the timeIndex interface [11]. |
| google/btree | Not selected | Archived by the owner on 24 October 2025 [10]. |

There is no single mature Go package that combines bounded ring semantics, generic typed values, optional event-time indexing, safe copy-out reads, explicit retention statuses and cursor-based subscriptions. A small purpose-built layer over the Go map and a swappable ordered index is therefore justified.

MVCC databases such as cilium/statedb are out of scope. Its documentation describes it as an in-memory Go database using "Multi-Version Concurrency Control (MVCC)", where readers obtain a snapshot "with a simple atomic pointer read".

# 29. Implementation Decisions (resolved)

1. Timestamp unit: int64 Unix nanoseconds UTC, named type Timestamp; sentinels reserved (6.1).
2. Capacity semantics: fixed row count, any value >= 1, preallocated; no age retention in v1 (12.1, 15.1).
3. Clear semantics: Gen increments, Seq restarts at 1, old Refs and TimeCursors return Reset, subscriptions get ErrReset, TimeIndexMode unchanged (15.4).
4. T ownership: immutable-by-contract default; optional Clone with CloneOnRead (default), CloneOnWrite, CloneOnReadWrite (11.5).
5. Duplicate SourceSeq: stored by default (SourceSeqIgnore); SourceSeqFlag/SourceSeqReject are opt-in; exact dedup via UniqueIndex + ConflictDrop (9.2).
6. Subscription API: concrete *Subscription[T], Next(ctx, dst, limit), StartOldest/StartNext/StartAt, LagFail/LagSkip, per-subscriber coalesced wake channel, context cancellation, independent of time index (17).
7. Key indexes: configured at construction as typed handles; no dynamic registration (10).
8. AsOf with Seq cutoff: deferred to v2 (16.4).
9. Unique conflicts: ConflictReject default; Drop/Replace/KeepFirst available (10.3).
10. Iterators: paged iter.Seq with up-front Status only; no yield or callback under lock (8.3).
11. Minimum Go version: 1.23; no dependency on Go 1.27 generic methods (4.3).
12. Event-time ordering: an internal timeIndex capability selected at construction (TimeIndexNone default, TimeIndexBTree), immutable per Series; no fallback scans in v1 (13).
13. Time-read continuation: opaque TimeCursor; RangeAfter(TimeKey) removed (8.4).
14. Synchronization: exactly one sync.Mutex per Series; RWMutex only if benchmarks justify it under 11.6.

# 30. Implementation Plan

1. **Foundations.** types.go and status.go: identity, timestamps, flags, Status (including TimeIndexDisabled and Invalid), TimeCursor, errors.
2. **Ring.** ring.go: bounded store with bounds arithmetic; unit tests for wraparound at capacities 1..8 and non-powers of two.
3. **Series core with TimeIndexNone.** New/Config validation, the Mutex, Append (prepare/commit), Get/Latest/ReadFrom, Clear/Close, Stats; reference-model tests. This proves Seq/Gen/retention semantics without any event-time index.
4. **Event-time ordering capability and time queries:**
   - the timeIndex interface and its contract (13.1, 13.2);
   - btreeTimeIndex as the reference implementation (BTreeG, NoLocks, path hints inside Apply);
   - the TimeIndexNone fast path (nil guard, StatusTimeIndexDisabled);
   - the common semantic suite for all implementations, plus the sliceTimeIndex reference (T-021..T-023);
   - all time queries with the Section 7 status rules and the eviction envelope.
5. **Batch reads and pagination.** dst/limit semantics, TimeCursor and Continue, the Entries pager (T-024); allocation benchmarks.
6. **AppendBatch.** Sequential prepare simulation, with a single Apply per batch.
7. **Key indexes.** UniqueIndex and MultiIndex with conflict policies and eviction rules (T-011..T-014).
8. **Subscriptions.** Wake channels and lag/reset/close semantics (T-007, T-018, T-019); mode-independence tests (T-022).
9. **Watermark and derivation.** Watermark and read gating; StartDerived (T-020).
10. **Benchmarks and structural checks.** The None vs BTree benchmark matrix (22.3) and the structural CI checks (T-025).
11. **Sustained runs.** Race/fuzz/property/benchmark runs on captured market-data traces.
12. **Optimizations last.** Only then consider additional time-index modes (13.7), an RWMutex variant (11.6) or immutable pages (11.7), based on profiles.

# 31. Definition of Done

General:
- No public API exposes memory that a later append can overwrite.
- Every query reports Status or ReadInfo exactly per Section 7, and the status matrix and precedence tests pass.
- go test -race passes under dedicated reader/writer/subscriber stress tests.
- Randomized reference-model tests prove the ring, the configured time index and all key indexes stay equivalent, including all conflict policies and Clear.
- AsOf and range queries are correct for equal timestamps, late events, SourceEpoch changes, wraparound and generations.
- Subscribers process every record within retention and report deterministic lag or reset when they cannot.
- Failed appends, including recovered callback panics, leave no state change.
- Steady-state hot paths meet the allocation targets of Section 22.2.
- There is no serialization, reflection, []byte keys, or conversion of T or K to interface{} on normal paths.
- The synchronization model is documented in package comments and enforced structurally: the Series is the only consistency boundary.

Event-time ordering capability (architectural acceptance criteria):
- The Series core contains no direct dependency on a concrete B-tree API (T-025a).
- btreeTimeIndex and the TimeIndexNone path pass the common semantic test suite (T-021..T-023).
- No index implementation (time or key) contains its own lock or synchronization (T-025b).
- One Series has exactly one consistency boundary: one mutex (C-001).
- ReadFrom and subscriptions work fully without an event-time index (T-022).
- Disabling the event-time index does not change Seq, Gen or retention semantics (T-022).
- The public API requires no knowledge of B-tree-specific types; continuation uses the opaque TimeCursor (TI-013).
- Benchmarks measure the cost of event-time indexing separately from the storage and ingestion path (22.3).

# 32. Deferred to v2

- Age or event-time retention defined against watermarks.
- Additional time-index implementations: monotonicTimeIndex and blockTimeIndex (13.7).
- Explicitly documented fallback scans for event-time queries on TimeIndexNone series, only if a real need arises (rejected for v1).
- RWMutex variant, only under Section 11.6 conditions.
- Composite (Key, EventTime, Seq) index.
- Snapshot API and known-as-of (Seq-cutoff) queries.
- Tombstones for distinguishing evicted keys in key indexes.
- Immutable sealed pages for lock-free reads.
- Persisted subscription and deriver cursors.

# 33. References

[1] Binance Spot API documentation, WebSocket Streams: Diff. Depth Stream and "How to manage a local order book correctly". https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md

[2] Coinbase Developer Documentation, Advanced Trade WebSocket Overview (sequence numbers) and WebSocket Channels. https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-overview and https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels

[3] The Go Programming Language, The Go Memory Model. https://go.dev/ref/mem

[4] The Go Programming Language, Data Race Detector. https://go.dev/doc/articles/race_detector

[5] tidwall/btree repository (README, PATH_HINT.md). https://github.com/tidwall/btree

[6] pkg.go.dev, github.com/tidwall/btree v1.8.2 (Map, BTreeG, Options, PathHint, iterators). https://pkg.go.dev/github.com/tidwall/btree@v1.8.2

[7] The Go Blog, Go maps in action (iteration order, concurrency). https://go.dev/blog/maps

[8] tidwall/hashmap, generic open-addressing Robin Hood hash map. https://github.com/tidwall/hashmap

[9] pkg.go.dev, package sync (Mutex and RWMutex semantics; Map guidance). https://pkg.go.dev/sync

[10] google/btree repository, archived by the owner on 24 October 2025. https://github.com/google/btree

[11] tidwall/btype, B-tree based collection types for Go. https://github.com/tidwall/btype

[12] Go 1.27 Release Notes (generic methods; interface restriction). https://go.dev/doc/go1.27

[13] Type Parameters Proposal, "No parameterized methods"; and proposal golang/go#77273, generic methods. https://go.googlesource.com/proposal/+/refs/heads/master/design/43651-type-parameters.md and https://go.dev/issue/77273

[14] pkg.go.dev, package iter (iter.Seq, range-over-func, Go 1.23). https://pkg.go.dev/iter

[15] Binance USDⓈ-M Futures documentation, "How to manage a local order book correctly" (U, u, pu). https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly

# 34. Design Notes for Reviewers

The central choice remains conservative: one lock per Series and copy-out results make concurrency and lifetimes easy to reason about. Revision 0.3 makes the lock a plain Mutex. In a write-dominated workload with long batch reads, a reader/writer lock buys little and complicates the cost model; it remains available as a measured optimization.

The second choice is unchanged: preserve ingest order physically and order event time logically. Arrival order, exchange event time and venue sequence are not interchangeable, and the Series never pretends they are.

Revision 0.3 adds a third choice: event-time ordering is a capability, not a structural dependency.
- Storage, Seq-based replay and subscriptions form the core, and they are fully functional without any event-time index.
- The B-tree is the reference implementation of an internal interface. It is selected per series, invisible in the public API (continuation is an opaque TimeCursor), and replaceable by specialized implementations once benchmarks justify them.
- Series that nobody queries by event time pay nothing for it.

Every no-data answer still says why. Venue-specific synchronization stays in adapters, and multi-series readiness stays in a strategy layer built on watermarks.
