# Design Decisions

Decisions not settled by `temporal_series_requirements.md`, or deviations from it. The spec stays authoritative for everything not listed here.

## Pre-coding decisions (from IMPLEMENTATION_PLAN.md §3)

| ID | Topic | Decision | Spec refs |
|---|---|---|---|
| P-01 | Module path | `github.com/fedorwk/temporal` (settled 2026-10-11; renaming later is a one-line `go.mod` change plus import rewrites). Package lives in `temporal/`. | §26 |
| P-02 | Batch conflict semantics | **Settled. Sequential**: item *i* conflicts with rows still retained when item *i* is applied (i.e. not evicted by items 0..*i*, including itself) and with earlier applied items. `AppendBatch(items)` ≡ the same `Append` calls in order, except all-or-nothing and one notification. Reason: a whole-transaction "rows this transaction evicts" rule is circular when Drop outcomes change how many rows get evicted. Also gives a simple test oracle. | §9.3, 9.4, 10.3, E-012 |
| P-03 | Prepare-phase order and precedence | Per item: (1) validate sentinels/flag bits → (2) late check + LatePolicy → (3) SourceSeq check + policy → (4) Seq-exhaustion check → (5) Clone (OnWrite) → (6) per index in `Config.Indexes` order: extract + conflict. Any Reject anywhere fails the call; else any Drop drops the item. Dropped items do not update MaxObservedEventTime, the SourceSeq tracker, or record counters other than `Dropped`. | §9.2, 9.3, 10.3 |
| P-04 | "No state change" vs diagnostic counters | **Settled.** `Stats.Rejected` and `IndexStats.Conflicts` increment on failed appends (operators of a live feed need to see rejections; otherwise they are invisible). "No state change" means data state: ring, bounds, Seq, Gen, indexes, trackers, envelope, watermark. | §9.5, 21 |
| P-05 | `ReadInfo.Evicted` intervals | Range: `[from,to)` vs `[MinEvicted,MaxEvicted]`. AtAll: `[ts,ts+1)`. Continue: `[last.EventTime, bound)`. PrevN: `HasEvicted && MinEvicted <= ts`. NextN: `HasEvicted && MaxEvicted > ts`. ReadFrom: true iff status is Expired. | §7.2, 7.4, 8.4 |
| P-06 | ReadFrom edge cases | `from.Seq == 0` → start at Oldest, Gen ignored. `from.Gen < Gen` (Seq≠0) → Reset. `from.Gen > Gen` → NotFound. `ReadInfo.Next` always zero; continuation via `LastRef.Seq+1`. | §7.2, 8.1, Q-007 |
| P-07 | Subscription edge cases | StartOldest on empty series → `Next = Newest+1`. `StartAt(zero Ref)` ≡ StartOldest. `StartAt` with `ref.Gen > Gen` → first `Next` returns `ErrInvalidInput`. `Subscribe` on a closed Series succeeds and drains retained records, then `ErrClosed` (consistent with E-013). `limit <= 0` → MaxReadBatch. | §17 |
| P-08 | Close idempotency | `Series.Close` returns nil on every call; only writes/Clear/SetWatermark observe the closed state. `SetWatermark` after Close returns false. | §15.4, E-013 |
| P-09 | Empty AppendBatch | No-op: returns `dst` unchanged, nil error, no Seq, no wake. Closed check still first (`ErrClosed`). | §9.4 |
| P-10 | Overflow-safe arithmetic | Late threshold uses saturating `MaxObserved - LateTolerance`. Gating: `Range` gated iff `to != MinTimestamp && to-1 > wm`; same for cursor bounds. `LateTolerance < 0` → `ErrInvalidConfig`. | §5.3, 19 |
| P-11 | Index prepare signature | Replace `willEvict func(Seq) bool` with `evictBelow Seq` (evictions are always a prefix from Oldest) → no closure allocation. Batches use per-index typed scratch (`[]K` by batch position + reusable pending `map[K]pendingRef`, cleared with `clear`). | §10.2 |
| P-12 | Deriver needs consistent source snapshot | Internal subscription method returns the batch together with `{newest, watermark, maxObserved, gen}` captured under the same lock. Deriver keeps its own `processed` cursor, advanced only after `dst.AppendBatch` succeeds. A panic in the derive function is recovered and treated as a derive error. | §18.1, 18.4 |
| P-13 | Watermark gate for forward reads | **Settled, overrides §19's "always" rule.** Gating stays off by default. With `GateReadsByWatermark`, before the first `SetWatermark` the watermark is MinTimestamp. Backward reads gate as written in §19. Forward reads are gated on the *answer*, not the query: a record with EventTime <= watermark is final (no late arrival can land between `ts` and it), so it may be returned. `After`/`AtOrAfter`: find the match as usual; if it exists and its EventTime <= wm → OK; otherwise NotReady (if no match and the envelope says Expired, Expired wins as in the normal precedence, since evicted data never returns). `NextN`: return matches with EventTime <= wm, up to n (bound = `wm+1`, saturating); if none → NotReady; `Truncated` when the cap n or MaxReadBatch cut off more matches <= wm. Reason: the library targets live trading (trades, order-book updates); "always NotReady" would make forward reads useless on gated series, while this rule keeps the no-look-ahead guarantee that gating exists for. Record the deviation in `DECISIONS.md`. | §19 |
| P-14 | TimeCursor encoding | `series` = global atomic counter starting at 1 (0 = never valid). `state = {uint64(last.EventTime), last.Seq, uint64(bound)}`. Cursor gen > current gen → Invalid. | §8.4 |
| P-15 | Entries iterator | One page buffer allocated per `Entries` call. If a later page returns non-OK (e.g. Reset after Clear), iteration ends silently; documented. | §8.3 |
| P-16 | Ring layout | Struct-of-arrays: `values []T` and `meta []slotMeta{eventTime, receiveTime int64; sourceSeq uint64; epoch, flags uint32}` (32 B/slot). Seq not stored, except a debug-only `seq []Seq` under `temporaldebug`. Bounds live in a small `bounds` value type inside the Series. | §12 |
| P-17 | Clone configured with nil func | `CloneMode` is ignored when `Clone == nil`. | §11.5 |
| P-18 | Error-path reflection | `ConflictError.Key` uses `fmt.Sprint(k)` on the error path only; acceptable under "no reflection on normal paths". | §9.5, 31 |
| P-19 | Implementation order | Key indexes (S6) **before** AppendBatch (S7), so the batch simulation is written once against the final index interface. Swaps spec §30 steps 6 and 7. | §30 |

## Session decisions

| ID | Session | Decision | Rationale | Spec refs |
|---|---|---|---|---|
| S1-01 | 1 | The package lives in `temporal/`, so its import path is `github.com/fedorwk/temporal/temporal`. | Follows the plan's layout (§2); keeps docs and scripts out of the package directory. | §26 |
| S1-02 | 1 | `go.mod` declares `go 1.23` with no toolchain line; the local toolchain is Go 1.27.1. | Spec targets Go 1.23+; no Go 1.27-only features are used. | §4.3 |
| S1-03 | 1 | `bounds.empty()` is `oldest == 0`. Normally `newest` is also 0 when empty; a test hook may move `newest` to place the next Seq near exhaustion. | Lets S2 exhaustion tests start at any Seq without filling the ring. | §12.2 |
| S1-04 | 1 | `Cursor` (subscription position) is declared in `types.go` in S1, because `LagError` needs it. | Avoids a forward dependency on subscription.go. | §17, §21 |
| S1-05 | 1 | `Index[T]` is a stub with only `indexName()` until S6; `validate()` already rejects nil indexes and duplicate names. | Config needs the type now; binding (`ErrIndexBound`) comes with the real handles. | §10.2, K-003 |
| S1-06 | 1 | `assertf` call sites must be wrapped in `if debugAssert { ... }`. | Avoids evaluating and boxing arguments in normal builds (zero-allocation paths). | §22.2 |
| S1-07 | 1 | `TimeCursor.IsZero()` checks `series == 0`; Series identities start at 1. | A valid cursor always has a non-zero series id (P-14). | §8.4 |
