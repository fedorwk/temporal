// Package temporal is an in-memory, bounded, time-aware series engine for
// market data such as trades and order-book updates.
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
