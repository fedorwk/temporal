// Package temporal is an in-memory, bounded, time-aware series engine for
// market data such as trades and order-book updates.
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
package temporal
