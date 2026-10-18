// Package temporal is an in-memory, bounded, time-aware series engine for
// market data such as trades and order-book updates.
//
// A Series retains the newest Capacity records of one stream. Records are
// stored in arrival order and can be read by arrival position (Seq), by event
// time when an event-time index is configured, and by key through optional
// key indexes. Subscriptions replay records in arrival order; derived series
// are built on them by the caller (see Subscription.Read).
//
// # Identity: Seq, Gen and Ref
//
// Every accepted record receives a Seq, a strictly increasing ingest sequence
// that starts at 1 in each generation. Storage, retention, replay (ReadFrom) and
// subscriptions follow Seq order, which is arrival order, never EventTime order.
// Seq 0 (SeqNone) is invalid and math.MaxUint64 is never assigned; when the next
// Seq would reach it, Append fails with ErrSeqExhausted.
//
// Gen is the generation. It starts at 1 and increments on every Clear, which
// also restarts Seq at 1. Ref{Gen, Seq} is the public record identity: Get
// reports StatusReset for a Ref from an earlier generation, StatusExpired for a
// Ref evicted in the current generation, and StatusNotFound for a Ref that was
// never assigned.
//
// A Series retains the newest Capacity records. Appending to a full Series
// evicts the oldest one in the same transaction.
//
// # Time
//
// Timestamp is int64 Unix nanoseconds, UTC; MinTimestamp and MaxTimestamp are
// reserved and rejected as EventTime. Each record carries EventTime (venue
// time), ReceiveTime (local observation), SourceSeq and SourceEpoch (feed
// sequence and session), and Flags (adapter bits 0-15, series bits 16-31).
//
// A record is late when its EventTime is below the generation's
// MaxObservedEventTime minus Config.LateTolerance. Late records are always
// flagged with FlagLate; Config.LatePolicy accepts (default) or rejects them.
// Accepted late records are indexed by event time, so a later query of the same
// time range can return them: event-time queries describe the current retained
// data, not an immutable snapshot. Deterministic replay of what was observed
// uses Seq order.
//
// # Event-time ordering capability
//
// Config.TimeIndex selects the capability at construction. TimeIndexBTree
// orders records by (EventTime, Seq) and enables AsOf, Before, After,
// AtOrAfter, FirstAt, LastAt, AtAll, Range, RangeValues, Continue,
// ContinueValues, PrevN, NextN, LatestByEventTime and Entries.
// TimeIndexOrdered enables the same methods with the same results for feeds
// whose EventTime never decreases: it keeps no extra structure and answers by
// binary search over the stored records, so Append costs about the same as
// TimeIndexNone. It requires LatePolicy LateReject with LateTolerance 0, so an
// out-of-order record fails with ErrLate. TimeIndexNone (the zero value) has
// no ordering cost on Append; those methods return StatusTimeIndexDisabled and
// never fall back to a scan. Seq reads, key lookups and subscriptions behave
// identically in every mode.
//
// Among records with equal EventTime, ingest order decides: AsOf returns the
// latest ingested, FirstAt the earliest, LastAt the latest. AsOf and PrevN never
// return a record with EventTime above the query time.
//
// # Statuses
//
// Point reads return a Status and batch reads a ReadInfo, so every empty answer
// says why: NotFound (nothing could match), Expired (a record evicted in this
// generation matched, or may have for exact-time lookups), Reset (an earlier
// generation's Ref or cursor), NotReady (beyond the watermark with
// Config.GateReadsByWatermark), TimeIndexDisabled, or Invalid (a zero or foreign
// TimeCursor). When several apply, the first in that precedence wins:
// TimeIndexDisabled, Invalid, Reset, NotReady, then the data statuses.
// Status.Err maps each status to a sentinel error for errors.Is.
//
// Batch reads append to a caller-provided dst (like append) and are bounded by
// min(limit, Config.MaxReadBatch). Truncated Range, RangeValues and AtAll reads
// return an opaque TimeCursor in ReadInfo.Next; Continue resumes strictly after
// the last returned record. Entries pages through a range without holding the
// lock while yielding.
//
// # Watermarks
//
// The single owner of a series (an adapter, or a derive loop) declares
// completeness with SetWatermark, which is monotonic. With
// Config.GateReadsByWatermark, backward reads whose bound exceeds the watermark
// return StatusNotReady, and forward reads (After, AtOrAfter, NextN) return
// only records at or below the watermark, which are final: no late record can
// arrive in front of them. A watermark of MaxTimestamp declares the series
// complete. Watermark changes do not wake subscribers; WaitWatermark blocks
// until the watermark reaches a given time, or the Series closes below it.
//
// # Key indexes
//
// UniqueIndex and MultiIndex are typed handles registered through
// Config.Indexes; they are the lookup entry points (UniqueIndex.Get,
// MultiIndex.Get and so on). A unique index resolves collisions with a
// ConflictPolicy: Reject (fail the append), Drop (skip the record, for
// idempotent re-ingestion), Replace (move the key to the new record) or
// KeepFirst (store the record without indexing it). A key held by a record that
// the same append evicts is free. Extractors run before any mutation; they must
// be pure and must not call Series methods.
//
// # Writes
//
// Append and AppendBatch validate and prepare everything before mutating
// anything, so every returned error leaves no data state change. AppendBatch is
// atomic and behaves exactly like the same Appends in order, with one
// notification. A batch larger than Capacity fails with ErrBatchTooLarge;
// AppendAll accepts any number of items by splitting them into AppendBatch
// chunks, atomic per chunk only.
//
// Close stops writes. CloseWithError also records a cause (Err): writes and
// subscriptions that have drained the Series then return a *ClosedError, which
// matches both ErrClosed and the cause with errors.Is, so a producer failure
// reaches every reader.
//
// The library does not recover panics. A panic in Clone or a key extractor
// propagates out of Append or AppendBatch; because callbacks run before any
// mutation and the lock and scratch buffers are released on the way out, a
// caller that recovers finds the Series unchanged and usable. A panic in Clone
// during a read propagates the same way after the lock is released.
//
// # Ownership and Clone
//
// Reads return copies; no returned memory is overwritten by later appends. A
// shallow copy does not copy what slices, maps or pointers inside T refer to.
// For such T, set Config.Clone: CloneOnRead (the default when Clone is set)
// clones every value copied out, CloneOnWrite clones once before storing so the
// producer may reuse its buffers, CloneOnReadWrite does both. Clone must not
// call Series methods.
//
// # Subscriptions and derived series
//
// Subscribe returns a cursor-based reader. Next blocks until records are
// available and returns them in Seq order. A subscriber that falls behind
// retention gets a *LagError (LagFail, the default) or skips ahead (LagSkip);
// after Clear it gets ErrReset until Seek. Wake-ups are coalesced and never
// block producers.
//
// A derived series (bars, indicators, filtered streams) is a loop the caller
// owns: read the source with Subscription.Read, compute outputs outside any
// lock, append them to the destination with one AppendBatch, advance a
// checkpoint only after that append succeeds, and forward the source watermark
// with SetWatermark once the batch reached Update.State.Newest. The library
// starts no goroutines; ExampleSubscription_Read shows the complete loop, and
// ExampleSubscription_TryRead the non-blocking form for loops over several
// sources.
//
// # Synchronization model
//
// Every Series is protected by exactly one sync.Mutex. The ring, the optional
// event-time index, key indexes, bounds, generation, watermark, policy trackers,
// statistics and scratch buffers are all guarded by it; no component has a lock
// of its own.
//
//   - Reads locate and copy their results while holding the mutex and return
//     caller-owned copies (copy-out). No read returns memory that a later write
//     could overwrite.
//   - User code (Clone, key extractors) runs only in documented places and must
//     not call any Series method: sync.Mutex is not reentrant, so a callback that
//     re-enters the same Series deadlocks.
//   - No iterator yields while the mutex is held. Paged scans copy one page under
//     the lock, release it, and then yield.
//   - Subscribers are woken only after the mutex is released.
//
// Reads hold the mutex for time proportional to the batch size and the Clone
// cost, which blocks writers; MaxReadBatch bounds it.
//
// # Allocation
//
// In steady state, Get, Latest, the point event-time reads and UniqueIndex.Get
// do not allocate, and batch reads do not allocate when cap(dst) suffices.
// With TimeIndexNone or TimeIndexOrdered and no key indexes, Append and
// AppendBatch do not allocate. TimeIndexBTree adds amortized B-tree node
// allocations; key indexes add per-key map costs.
package temporal
