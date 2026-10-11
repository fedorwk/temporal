# Temporal Series Engine — Implementation Plan

Source spec: `temporal_series_requirements.md` rev 0.3 (2026-10-07). Plan date: 2026-10-11.
Audience: a coding agent working in multiple sessions, plus the human reviewing each session.

The spec is authoritative on semantics. This plan decides **order, file layout, internal design choices the spec leaves open, and acceptance gates**. Code sketches in the spec are recommendations; where this plan deviates, the deviation is listed in §3 and must be logged in `DECISIONS.md`.

---

## 1. How to use this plan

### 1.1 Session protocol (every session)

**Start**
1. Read `PROGRESS.md` and `DECISIONS.md` (create them in Session 1).
2. Read the spec sections listed under the session's **Read** heading. Do not rely on memory of earlier sessions.
3. Run `./scripts/check.sh` and confirm the tree is green before changing anything.

**During**
- Implement only the session's scope. If a later-session concern blocks you, add the smallest hook and note it in `PROGRESS.md`.
- Every requirement ID you implement gets at least one test whose name or comment cites the ID (e.g. `TestGet_StatusMatrix // Q-001, E-001`).
- Any semantic choice not settled by the spec or by §3 below → stop, write the question into `PROGRESS.md` under "Open questions", pick the most conservative behavior, and continue.

**End (exit gate)**
1. `./scripts/check.sh` passes (gofmt, vet, `go test -race ./...`, `go test -tags temporaldebug ./...`).
2. Every item in the session's **Exit criteria** is met.
3. Update `PROGRESS.md`: what was done, test names per requirement ID, open questions, known gaps.
4. Append any new design choices to `DECISIONS.md` (ID, decision, rationale, spec refs).
5. One commit per session (or a few logical commits), message `session N: <summary>`.

### 1.2 Kickoff prompt template

> You are implementing **Session N** of `IMPLEMENTATION_PLAN.md` for the Temporal Series Engine in this repository. Follow §1.1 of the plan exactly. Read `PROGRESS.md`, `DECISIONS.md`, the plan's Session N section, and the spec sections it lists. Implement only Session N's scope, meet its exit criteria, then update `PROGRESS.md`/`DECISIONS.md` and commit. Do not change public semantics defined in `temporal_series_requirements.md`; if something is ambiguous, record it as an open question and choose the conservative behavior.

---

## 2. Repository layout and conventions

```
temporal_2/
  go.mod                         // module path: see P-01; `go 1.23`
  docs/  temporal_series_requirements.md
  IMPLEMENTATION_PLAN.md
  PROGRESS.md  DECISIONS.md  BENCHMARKS.md (S11)
  scripts/check.sh  scripts/bench.sh (S11)
  temporal/
    doc.go                 // package docs: sync model, ownership, statuses, modes
    types.go               // Seq, Gen, Ref, Timestamp, Flags, InputMeta, Meta, Entry, Input, timeKey
    status.go              // Status, ReadInfo, TimeCursor, errors
    config.go              // Config, enums, validation        (split out of series.go)
    debug_on.go/debug_off.go // build tag temporaldebug: assertions
    ring.go                // ring[T] + bounds
    series.go              // Series[T], New, Append, Clear, Close
    batch.go               // AppendBatch prepare simulation     (split out of series.go)
    query.go               // Seq reads + event-time reads, Continue, Entries
    watermark.go           // SetWatermark, gating helpers
    time_index.go          // timeIndex interface, newTimeIndex(mode)
    time_index_btree.go    // ONLY importer of tidwall/btree
    time_index_none.go     // zero-size noTimeIndex (harness/capability only)
    key_index.go           // Index[T], UniqueIndex, MultiIndex, postings
    subscription.go
    derive.go
    stats.go
    invariants.go
    *_test.go              // see per-session lists; model_test.go grows every session
```

Conventions:
- Single package `temporal`. No sub-packages in v1.
- Go 1.23 language level (`iter`, range-over-func, `clear`, `min`/`max`). No Go 1.27 generic methods.
- Errors: sentinels created with `errors.New("temporal: ...")`. Typed errors implement `Is` for their sentinel.
- Debug assertions behind build tag `temporaldebug` (`const debugAssert`); zero cost in normal builds.
- Randomized tests: deterministic seeds printed on failure; iteration count via `-temporal.iters` flag, reduced in `-short` and under `-race` (use a `race`/`!race` build-tag constant).
- Every `s.mu.Lock()` is immediately followed by `defer s.mu.Unlock()` on read paths (C-007). Write paths may unlock explicitly before notify, but must use a deferred unlock or recover wrapper around anything that can panic.

---

## 3. Decisions settled before coding

The spec leaves the following open or slightly inconsistent. These are the plan's resolutions; the human may override any of them before the relevant session. Copy them into `DECISIONS.md` in Session 1.

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

---

## 4. Session overview

| # | Session | Main output | Key requirement IDs |
|---|---|---|---|
| 1 | Foundations and ring | module, types, status, config, ring, check script | §6, §7.1, §12, G-11 |
| 2 | Series core on the Seq path (TimeIndexNone) | New, Append, Get/Latest/ReadFrom, Clear/Close, Stats, reference model v1 | Q-001/007/008/017, W-001/004–009, E-001/005–010/013 |
| 3 | Event-time ordering capability | timeIndex, btreeTimeIndex, slice reference, common suite, write-path wiring, structural checks | TI-001–013, W-010, T-021, T-025 |
| 4 | Event-time point queries and status rules | AsOf/Before/After/AtOrAfter/FirstAt/LastAt/LatestByEventTime, precedence, watermark gating | Q-002–004/012/016, E-003/004/015, T-017, T-023 |
| 5 | Batch time reads and pagination | Range/RangeValues/AtAll/PrevN/NextN, TimeCursor, Continue, Entries | Q-005/006/011/013–015, E-002/016, T-024 |
| 6 | Key indexes (single-record Append) | UniqueIndex, MultiIndex, conflict policies, eviction rules | K-001–011, Q-009/010, E-011/012, T-011–014 |
| 7 | AppendBatch | atomic batch with sequential simulation, one Apply, one wake | W-002/003/010, T-011/012/014 batch parts |
| 8 | Subscriptions | Subscribe/Next/TryNext/Seek/Close, wake channels, lag/reset/close | S-001–013, E-008/014, T-007/018/019 |
| 9 | Derived series and watermark propagation | StartDerived, Deriver status/checkpoint | D-001–008, T-020 |
| 10 | Verification hardening | fuzzing, full model in all modes, mode equivalence, race stress, Clone modes | T-002/003/005/006/010/022 |
| 11 | Benchmarks, allocation acceptance, docs, DoD audit | bench matrix, None-vs-BTree report, alloc tests, examples, DoD checklist | §22, T-009, T-026, §31 |
| 12 | (Optional, needs data) Trace replay fixtures | record/replay fixtures from captured feeds | T-008, §30.11 |

Rough size: S1 small; S2, S4, S6, S8 medium; S3, S5, S7, S10 large; S9, S11 medium.

---

## 5. Sessions

### Session 1 — Foundations and ring

**Read:** §3, §6, §7.1, §7.4, §8.4 (TimeCursor shape), §12, §21 (identity, status, config sections), §26; this plan §2–§3.

**Build**
1. `git init` (the folder is not a repo yet). Check `go version` ≥ 1.23. `go mod init <P-01>` with `go 1.23`. Create `PROGRESS.md`, `DECISIONS.md` (seed with P-01..P-19), `scripts/check.sh`.
2. `doc.go`: package comment stub stating the synchronization model (one mutex per Series, copy-out reads, no user code under lock, no yields under lock).
3. `types.go`: `Seq`, `SeqNone`, unexported `seqMax = math.MaxUint64`; `Gen`; `Ref` + `IsZero`; `Timestamp`, `MinTimestamp`, `MaxTimestamp`, `FromTime`, `Time`; `SourceEpoch`; `Flags` + all constants and masks; `InputMeta`, `Meta`, `Entry[T]`, `Input[T]`; internal `timeKey` and `keyLess(a, b)`.
4. `status.go`: `Status` + `String` + `Err`; all sentinel errors from §21; `LagError`, `ConflictError`, `CallbackPanicError` (with `Error`, `Is`); `ReadInfo`; `TimeCursor` + `IsZero` + internal `makeCursor(id, gen, last timeKey, bound)` / `(c TimeCursor) decode()`.
5. `config.go`: `TimeIndexMode`, `LatePolicy`, `SourceSeqPolicy`, `CloneMode`, `ConflictPolicy`, `Order`, `LagPolicy`; `Config[T]`; `validate()` returning a normalized internal config: Capacity ≥ 1, MaxReadBatch ≥ 0 (0 → 4096), enums in range, LateTolerance ≥ 0. Leave a hook for index validation (S6).
6. `debug_on.go` / `debug_off.go`: `const debugAssert`, `assertf(cond bool, format string, args ...any)` (args evaluated only in debug builds; document to keep call sites cheap).
7. `ring.go`: `ring[T]` per P-16 with `newRing(capacity)`, `slot(seq) int` (`(seq-1) % cap`, mask fast path for powers of two), `put(seq, *T, *slotMeta)`, `value(seq) *T`, `meta(seq) *slotMeta`, `eventTime(seq)`, `zero()`. `bounds{oldest, newest Seq}` with `len()`, `empty()`, `retained(seq)`, `nextSeq()`, `push(capacity) (evicted Seq, didEvict bool)`, `reset()`.

**Tests:** `types_test.go` (FromTime/Time round-trip incl. pre-1970 and ns precision; masks disjoint and covering; Ref.IsZero), `status_test.go` (Err mapping table; `errors.Is`/`errors.As` for every typed error), `config_test.go` (validation table incl. each `ErrInvalidConfig` case), `ring_test.go` (capacities 1..8, 10, 1000, 1024; push 5×capacity; retained slots unique; evicted Seq == previous oldest; `zero()` clears a pointer-holding T).

**Exit criteria:** check script green; no exported Series yet; `DECISIONS.md` seeded.

---

### Session 2 — Series core on the Seq path (TimeIndexNone only)

**Read:** §5.3, §6.2–6.4, §7.2 (Get, Latest rows), §7.5, §8 (Q-001, Q-007, Q-008, Q-011, Q-013, Q-017), §8.2, §9.1–9.3, §9.5, §11, §12, §15, §20 (E-001, E-005–E-010, E-013), §21 (Series, Stats); P-03, P-04, P-06, P-08, P-10, P-17.

**Build**
1. `series.go`: `Series[T]` per §11.2 minus the time index (added S3) and subscribers (S8). Fields: `mu`, `ring`, `b bounds`, `gen`, `id` (global atomic counter), envelope `{has bool; min, max Timestamp}`, `maxObserved` + `hasObserved`, SourceSeq tracker `{valid bool; epoch; last}`, `watermark`, `closed`, counters, normalized config.
2. `New`: validate, preallocate ring, `gen = 1`, `watermark = MinTimestamp`.
3. `Append`: lock → closed check → **prepare** (P-03 steps 1–5; Clone via `callClone` that recovers panics into `*CallbackPanicError{Callback: "clone", Value, Stack: debug.Stack()}`) → **commit** (assign `newest+1`, evict if full and update envelope + counters, write slot, update trackers and counters) → unlock → `s.notify()` (no-op stub until S8). Fill `AppendResult`.
4. Reads: `Get`, `Latest`, `ReadFrom` (P-06; effective limit; `Truncated`, `LastRef`, `Oldest`). A single `copyOut(dst *Entry[T], seq)` helper applies Clone OnRead and fills `Meta.Ref.Gen`.
5. `Len`, `Cap`, `Gen`, `TimeIndexMode`, `OldestRef`, `NewestRef`, `Watermark`, `SetWatermark` (monotonic; false after Close), `Clear` (`ErrGenExhausted`, zero ring, reset bounds/envelope/trackers/watermark, wake stub), `Close` (P-08).
6. `stats.go`: `Stats` (time-index fields zero for now).
7. `invariants.go`: `CheckInvariants` v1: bounds math, `Len <= Cap`, envelope sanity, MaxObserved ≥ every retained EventTime (within the generation).
8. `export_test.go` hooks: set next Seq / set Gen, for exhaustion tests.
9. `model_test.go`: reference model (plain `[]Entry` in Seq order, gen, envelope, maxObserved, SourceSeq tracker, watermark). Random op generator: Append (random metas incl. late, sentinel times, forbidden flag bits, epochs), Get (old-gen, future, evicted, retained, zero refs), Latest, ReadFrom, Clear, SetWatermark, Close. Compare results, errors (`errors.Is`), Status, ReadInfo, entries, and Stats.

**Tests:** model property test (T-002, None mode); Get status matrix (E-001, T-016 part, Seq restarts at 1); E-009, E-010; `ErrSeqExhausted`, `ErrGenExhausted`; W-008; late tolerance incl. saturation; SourceSeq Ignore/Flag/Reject and epoch reset; Close semantics (reads still work, writes fail); concurrent Append from N goroutines under `-race` (W-007: Seqs unique and contiguous); Clone panic on Append → no state change (T-014 part); Clone panic on Get → next Append succeeds (T-015); `testing.AllocsPerRun` = 0 for Append (no index, None), Get, Latest, ReadFrom with sufficient `cap(dst)` (T-026 part).

**Exit criteria:** all of the above green; package docs updated with Seq/Gen/Ref semantics.

---

### Session 3 — Event-time ordering capability

**Read:** §6.5, §13 (all), §14, §23.1, T-021, T-025, §28; P-16.

**Build**
1. `go get github.com/tidwall/btree@v1.8.2` (run `go list -m -versions github.com/tidwall/btree` first; pkg.go.dev hints a newer version may exist — record the choice in `DECISIONS.md`).
2. `time_index.go`: `timeIndex` interface (§13.2; trim unused primitives later, not now) and `newTimeIndex(mode) timeIndex` (nil for None).
3. `time_index_btree.go`: `btreeTimeIndex{tr *btree.BTreeG[timeKey]; insHint, delHint btree.PathHint}` created with `btree.NewBTreeGOptions(keyLess, btree.Options{NoLocks: true})`. `Apply` uses `DeleteHint`/`SetHint` with separate hints. Point primitives via pivot keys (relies on Seq 0 and MaxUint64 never being assigned):

   | Primitive | Implementation |
   |---|---|
   | AsOf(ts) | first of `Descend({ts, seqMax})` |
   | Before(ts) | first of `Descend({ts, 0})` |
   | After(ts) | first of `Ascend({ts, seqMax})` |
   | AtOrAfter(ts) | first of `Ascend({ts, 0})` |

   `Ascend(dst, start, to, limit)` collects up to `limit` keys with `EventTime < to` and peeks one more to report `more`; `Descend` likewise. **Check allocations**: if the `Ascend`/`Descend` closures escape and allocate, switch to a struct-held `IterG` reused via `Init`/`Seek`/`Next`/`Prev` (`Release` only at Clear/teardown). Debug assertions: deleted keys exist; inserted keys are new.
4. `time_index_none.go`: zero-size `noTimeIndex` (no-op mutators, absent results); never used on hot paths.
5. Series wiring: `time timeIndex` field; `delKeys`, `insKeys` scratch; in commit append keys only when `s.time != nil`; after commit `if s.time != nil { s.time.Apply(s.delKeys, s.insKeys) }` then reset scratch; Clear calls `time.Clear()`; Stats Min/MaxEventTime from `Min()`/`Max()`; `keyScratch` (cap MaxReadBatch+1) allocated only when `time != nil`; `TimeIndexMode()` returns the configured mode.
6. `CheckInvariants`: when a time index exists, `time.Len() == Len` and a chunked full ascend matches the retained set exactly (same EventTimes from ring metadata).
7. `time_index_slice_test.go`: `sliceTimeIndex` reference (sorted `[]timeKey`, linear insert/delete).
8. `structure_test.go` (T-025) using `go/parser` + `go/ast`: (a) only `time_index_btree.go` imports `github.com/tidwall/btree`; (b) no struct in `time_index_*.go` or `key_index.go` has a field of type `sync.Mutex`, `sync.RWMutex`, or any `sync/atomic` type; (c) `NewBTreeGOptions` is called with a composite literal setting `NoLocks: true`. Also ban `sync.Map`, `reflect` and `unsafe` imports in non-test files.

**Tests:** `time_index_test.go` common suite (T-021) over `{btree, slice}` factories: deterministic scenarios (ties, reverse bursts, FIFO eviction patterns, empty index, limit 0/1/exact/over, timestamps at Min+1/Max−1) and a randomized differential that compares every primitive after every op. Series in BTree mode: random Append/Clear with `CheckInvariants` after each step. Alloc: None Append still 0; BTree Append steady-state reported (not asserted zero).

**Exit criteria:** T-021 and T-025 green; Seq-path model tests from S2 also pass in BTree mode.

---

### Session 4 — Event-time point queries, status precedence, watermark gating

**Read:** §7 (all), §8 (Q-002–Q-004, Q-008, Q-012, Q-016), §13.2 mapping table, §16.3, §19, §20 (E-003, E-004, E-015); P-10, P-13.

**Build**
1. `query.go`: a precedence helper used by every event-time method: Disabled → (Invalid/Reset for cursors, S5) → NotReady → data. Methods: `AsOf`, `Before`, `After`, `AtOrAfter`, `FirstAt` (AtOrAfter + equality), `LastAt` (AsOf + equality), `LatestByEventTime` (`Max`).
2. Envelope helpers implementing §7.2 exactly: `expiredLE(ts)`, `expiredLT(ts)`, `expiredGT(ts)`, `expiredGE(ts)`, `expiredEQ(ts)` (conservative).
3. `watermark.go`: gating predicates per §19 with P-10 overflow handling; gate only when `GateReadsByWatermark`.
4. Model: add a time-ordered view (sort retained by (EventTime, Seq)) and expected statuses by linear scan + envelope.

**Tests:** T-017 full matrix, table-driven per method: OK; NotFound on empty; NotFound with irrelevant evictions; Expired exactly at the boundaries (`ts == MinEvicted`, `MinEvicted−1`, `MaxEvicted`, `MaxEvicted+1`). Precedence table (§7.5), incl. Disabled beating NotReady on an empty series. T-004: duplicate timestamps (FirstAt = smallest Seq, LastAt = largest, AsOf = latest-ingested among ties), late events, sentinel timestamps. T-023 point methods on None: no data, no state change (Stats and CheckInvariants equal before and after), with the gate on and off. §16.3 no-look-ahead property over random data. Zero allocations for all point reads (T-026 part). Clone panic in a time read leaves the mutex free (T-015).

**Exit criteria:** status matrix and precedence tests green in both modes (None expects Disabled).

---

### Session 5 — Batch time reads, TimeCursor pagination, Entries

**Read:** §7.4, §8 (Q-005, Q-006, Q-011, Q-013–Q-016), §8.1–8.4, §13.2 mapping table, §19 (Range/Continue gating), E-002, E-016, T-024; P-05, P-14, P-15.

**Build**
1. Internal two-step read: index → `keyScratch` → copy ring slots into dst. Two small copy loops (`[]Entry[T]` and `[]T`), no interface boxing.
2. `Range`, `RangeValues`, `AtAll` (`Ascend({ts,0}, ts+1)`; `ts == MaxTimestamp` → NotFound), `PrevN` (`Descend({ts, seqMax})`, copied in reverse so dst is ascending), `NextN` (`Ascend({ts, seqMax}, MaxTimestamp)`), `Continue`, `ContinueValues`.
3. `ReadInfo`: N, Truncated (from `more`), Evicted per P-05, Status (OK / Expired / NotFound), `Next` cursor only when truncated for Range/RangeValues/AtAll/Continue*, LastRef, Oldest.
4. Cursor validation per §8.4 and P-14, slotted into the precedence helper (Disabled → Invalid → Reset → NotReady).
5. `Entries(from, to) (iter.Seq[Entry[T]], Status)`: status computed up front; each page copied under the lock into the iterator's page buffer, lock released, then entries yielded; resumes from an internal cursor. Never yields under the lock.

**Tests:** T-024 (all bullets: pages concatenate to the full Range result; late insert behind the cursor not returned; late insert ahead of it returned; eviction between pages sets Evicted; zero/foreign cursor → Invalid; old-gen cursor → Reset; None → Disabled even with a foreign cursor). E-002. Properties: PrevN's last == AsOf; PrevN ∪ NextN partition the timeline at ts with no overlap. Limit semantics (≤0 → MaxReadBatch; capped; Truncated). dst append semantics (prefix preserved; nil dst). T-023 for all batch methods. Zero allocations with sufficient `cap(dst)` for Range, Continue, PrevN, NextN, AtAll. Entries deadlock guard: call `s.Append` inside the range loop under a test timeout.

**Exit criteria:** all event-time methods in §21 implemented and covered by the model in both modes.

---

### Session 6 — Key indexes (single-record Append)

**Read:** §9.3, §9.5, §10 (all), §14, §14.1, §20 (E-011, E-012), T-011–T-014; P-03, P-11, P-18.

**Build**
1. `key_index.go`: exported `Index[T]` interface with unexported methods (P-11 shape):
   `indexName()`, `bind(s *Series[T], capacity int) error`, `prepare(e *Entry[T], evictBelow Seq, pos int) (decision, error)`, `commit(slot int, seq Seq, d decision)`, `evict(slot int, seq Seq)`, `reset()`, `stats() IndexStats`. `decision` is a small non-generic struct (kind: insert / replace / keepFirst / drop; existing Ref). The typed key stays in the index's own scratch (single field now, slice by position in S7).
2. `UniqueIndex[T,K]`: `m map[K]Seq`, `keys []K` per slot, `indexed` bitset, policy, counters, bound Series pointer. Replace clears the previous holder's indexed bit. Eviction deletes only if `m[k] == seq` (K-008).
3. `MultiIndex[T,K]`: `m map[K]*postings`; postings = `[]Seq` + head offset, compacted when head passes half; empty lists deleted; optional free list of postings structs to reduce churn. Eviction asserts front == seq (debug).
4. Series integration: `New` validates all indexes first (unique names → `ErrInvalidConfig`; already bound → `ErrIndexBound`), then binds all (no partial binding). Append prepare step 6 with extractor panics recovered as `CallbackPanicError{Callback: "extractor:<name>"}`; extractor receives the (OnWrite-cloned) value with tentative `Ref{gen, newest+1}`. Commit order: evict on all indexes → ring write → commit on all indexes. Fill `AppendResult.Existing`/`Index`.
5. Lookups: `UniqueIndex.Get/Contains/Len`, `MultiIndex.Get/Latest/Count/Keys`, all under `s.mu` with copy-out and Clone OnRead.
6. `Stats.Indexes`; `CheckInvariants` rebuilds expected maps from retained rows' stored keys + indexed bits and compares (§14.1).
7. Model: naive per-index maps with sequential policy semantics.

**Tests:** T-011 single-append part (every policy; several indexes with mixed policies; Drop beats Replace/KeepFirst; Reject fails the item). T-012 (capacity 1 and a full ring: a key equal to the row being evicted is not a conflict). T-013. T-014 extractor panic → no state change (Stats, CheckInvariants and model equal). K-003 errors. Mode independence: identical results with None and BTree. Zero allocations for `UniqueIndex.Get` in steady state.

**Exit criteria:** §14.1 invariants checked after every random op in both modes.

---

### Session 7 — AppendBatch

**Read:** §9.1 (W-002, W-003, W-009, W-010), §9.3, §9.4, §10.3; P-02, P-03, P-09, P-11.

**Build**
1. `batch.go`: `AppendBatch(dst, items)`: closed check → empty no-op (P-09) → `len(items) > Capacity` → `ErrBatchTooLarge` → **prepare simulation** over a simulated state: next Seq, simulated oldest/len (each item's `evictBelow`), maxObserved, SourceSeq tracker, per-index pending keys (reusable map cleared per batch). Per-item plans go into a Series-owned `[]itemPlan` scratch; OnWrite clones into a Series-owned `[]T` scratch (zeroed after commit so references can be collected).
2. Errors carry the batch position (`ConflictError.BatchPos`; other errors wrapped so `errors.Is` still matches the sentinel).
3. Commit: apply plans in order (dropped items consume no Seq), one `time.Apply` per batch, one `notify` per batch, one `AppendResult` per item appended to dst.
4. `delKeys`/`insKeys`/plan scratch grow amortized; preallocate modestly at New.

**Tests:** model check that AppendBatch ≡ sequential Appends when all succeed, and leaves the model unchanged when any item fails. T-011 batch (intra-batch duplicates under each policy; Replace chains within one batch; multiple indexes). T-012 batch (Capacity-sized batch on a full ring whose keys match rows evicted earlier in the same batch). T-014 batch (panic at item k → no state change). W-003. LateReject failing mid-batch. Exactly one Apply per batch (counting `timeIndex` wrapper injected in an internal test). Zero allocations for AppendBatch with None and no indexes in steady state.

**Exit criteria:** fuzz-style randomized batch tests (sizes 0..Capacity+1, capacities 1..8) green in both modes.

---

### Session 8 — Subscriptions

**Read:** §17 (all), E-008, E-013, E-014, T-007, T-016, T-018, T-019; P-07, P-08.

**Build**
1. `subscription.go`: `subscriber{wake chan struct{} (buffer 1); done chan struct{}; closeOnce sync.Once}`; `Series.subs atomic.Pointer[[]*subscriber]`, replaced copy-on-write under `s.mu` on Subscribe/Close. Real `notify()`: load the slice, non-blocking send to each wake channel; called after unlock by Append, AppendBatch, Clear, Close.
2. `StartFrom` + `StartOldest/StartNext/StartAt`; `SubscribeOptions`; `Cursor`.
3. `Subscription[T]`: `Next` (algorithm §17.2), `TryNext`, `Seek`, `Cursor`, `Skipped`, `Wake`, `Close`. Shared internal `pollLocked(dst, limit) (dst, snapshot, err, gotData bool)` returning the P-12 snapshot (used by S9).
4. Lag evaluated in the same lock acquisition as the copy (S-005); cursor advanced only after a successful copy.

**Tests:** T-007 (replay across batches; LagFail error contents and unchanged cursor; LagSkip and `Skipped()`). S-005. T-016 subscription part (ErrReset until Seek; every Seek variant). T-018: a producer with stalled consumers never blocks; wake buffer length ≤ 1; stress with several producers and consumers checking every record arrives exactly once, in order (reduced size under race). T-019: ctx cancel, `Subscription.Close`, `Series.Close` each unblock `Next` with the specified error and unchanged cursor; S-009 drain-then-ErrClosed. S-013 mode independence. Append stays at zero allocations with subscribers attached.

**Exit criteria:** `go test -race -count=20 -run Subscription` green.

---

### Session 9 — Derived series and watermark propagation

**Read:** §18 (all), §19, T-020; P-12, P-13.

**Build**
1. `derive.go`: `DeriveFunc`, `DeriveOptions`, `WatermarkMode`, `StartDerived`, `Deriver` with `Status()`, `Checkpoint() Cursor`, `Stop()`, `Done() <-chan struct{}`, `Err()`. `DeriverStatus{Name, Version, State (Running/Stopped/Failed), Processed Cursor, LagRecords, LagTime, LastErr}`. The Deriver may use its own small mutex for its status; it is outside the Series consistency model and outside the T-025 checks.
2. Worker loop: internal poll with snapshot → `fn(batch, outScratch[:0])` outside all locks (panic → error) → `dst.AppendBatch` (skipped when no outputs) → advance `processed` → WatermarkPropagate: if the batch's last Seq == snapshot.newest, `dst.SetWatermark(snapshot.watermark)`. Any error (fn, append, ErrLagged under LagFail, ErrReset) → state Failed, error recorded, `processed` not advanced. ctx cancel / Stop → Stopped.
3. Lag metrics: LagRecords = source Newest − processed; LagTime = source MaxObservedEventTime − last processed EventTime.

**Tests:** T-020: watermark monotonic, initial MinTimestamp, reset on Clear (gating already covered in S4); deriver failure paths each leave `Processed` unchanged (fn error, AppendBatch error such as a unique Reject or ErrBatchTooLarge, LagFail, source Clear); Version recorded; propagation only when caught up; WatermarkManual leaves dst untouched. D-008: None→BTree and BTree→None pipelines. D-006: two derivers over the same source produce identical destinations.

**Exit criteria:** deriver tests green under `-race -count=10`.

---

### Session 10 — Verification hardening

**Read:** §23 (all), §31.

**Build / tests**
1. `fuzz_test.go`: `FuzzSeries` decodes bytes into op sequences (capacity 1..8, both modes, 0–2 indexes with random policies; ops: Append, AppendBatch, every query, Continue with stale/foreign cursors, Clear, Close, Subscribe/TryNext/Seek, SetWatermark) checked against the model plus `CheckInvariants` after each op. Commit a seed corpus.
2. Full-model property test in every mode with long overwrite runs (T-002).
3. T-022 differential: the same stream through None and BTree series → identical AppendResults, Refs, statuses, Seq reads, key lookups, subscription batches, and Stats except time-only fields.
4. T-005 race stress: writers overwriting while readers copy, using a T with an internal checksum to detect torn values; subscribers attached.
5. T-006: reference-heavy T (slices, maps) under each CloneMode; mutate returned values and producer buffers; verify isolation.
6. T-010 complete: `CheckInvariants` after random operations in all suites.
7. Lock-discipline audit recorded in `DECISIONS.md`: every lock site, every user-code call site (Clone, extractors, derive fn) and which phase it runs in.

**Exit criteria:** `go test -race ./...` green; `go test -run=^$ -fuzz=FuzzSeries -fuzztime=10m` with no failures; statement coverage reported (target ≥ 90% for the package).

---

### Session 11 — Benchmarks, allocation acceptance, docs, DoD audit

**Read:** §22 (all), §31, T-009, T-026.

**Build**
1. `bench_test.go`: the mandatory §22.3 comparison (None vs BTree: Append, AppendBatch 8/64/256/1024, ReadFrom, Subscription.Next, AsOf, Range) reporting records/s (`b.ReportMetric`), ns/op, allocs/op, B/op. A practical subset of the §22.1 matrix by default; the full matrix behind an env var. Data generators for disorder (0, 0.1, 1, 10%, reverse burst) and ties.
2. Write-latency harness: p50/p99 per Append with 1/2/8/32 concurrent readers and varying batch size/MaxReadBatch (preallocated sample buffer, sorted afterwards).
3. Memory per retained row by mode and per index (heap delta / capacity).
4. `alloc_test.go`: consolidated T-026 assertions (`testing.AllocsPerRun == 0`) for every item in §22.2.
5. T-009: sustained-ingestion test with heap profile and a check that HeapInuse is bounded after warm-up.
6. `scripts/bench.sh`: `-count=10`, `benchstat` None vs BTree; `BENCHMARKS.md` with results and the Q-017 check (ReadFrom and Subscription.Next show no significant difference between modes).
7. `doc.go` finished; testable `Example*` functions mirroring §25.
8. DoD audit: walk §31 line by line in `PROGRESS.md`, citing the test that proves each item.

**Exit criteria:** every §31 item has evidence or an explicit, justified gap.

---

### Session 12 (optional) — Trace replay fixtures

Needs captured feed data from the human (e.g. Binance diff-depth or Coinbase messages normalized to `Input[T]`). Build record/replay fixtures (T-008), run sustained race/fuzz/benchmark runs on them (§30 step 11). v2 items (§32) stay out of scope.

---

## 6. Requirement traceability

| Area | IDs → session |
|---|---|
| Queries | Q-001 S2 · Q-002 S4 (FirstAt/LastAt), S5 (AtAll) · Q-003/004 S4 · Q-005/006 S5 · Q-007 S2 · Q-008 S2 (Latest, MaxObserved), S3/S4 (Min/Max, LatestByEventTime) · Q-009/010 S6 · Q-011/013 S2 (ReadFrom), S5 · Q-012 S4, S5 · Q-014/015 S5 · Q-016 S4, S5 · Q-017 S2, S8, S10 |
| Writes | W-001 S2 · W-002/003 S7 · W-004 S2, S6, S7 · W-005–W-008 S2 · W-009 S2, S7, S11 · W-010 S3, S7 |
| Key indexes | K-001–K-011 S6 (batch interplay S7) |
| Concurrency | C-001–C-008 S2 baseline, re-checked every session, audited S10 |
| Time index | TI-001–TI-008, TI-010–TI-012 S3 · TI-009 S4 · TI-013 S5 |
| Subscriptions | S-001–S-013 S8 |
| Derivation | D-001–D-008 S9 |
| Edge cases | E-001 S2 · E-002 S5 · E-003/004 S4 · E-005–E-010 S2 · E-011/012 S6, S7 · E-013 S2, S8 · E-014 S8 · E-015 S4, S5 · E-016 S5 |
| Tests | T-001 all · T-002 S2→S10 · T-003 S10 · T-004 S4 · T-005 S10 · T-006 S2 (basic), S10 · T-007 S8 · T-008 S12 · T-009 S11 · T-010 S2→S10 · T-011/012 S6, S7 · T-013 S6 · T-014 S2, S6, S7 · T-015 S2, S4 · T-016 S2, S5, S8 · T-017 S4 · T-018/019 S8 · T-020 S9 · T-021 S3 · T-022 S10 (partial from S6) · T-023 S4, S5 · T-024 S5 · T-025 S3 · T-026 S2, S4, S5, S7, S11 |

---

## 7. Risks and watch-points

- **B-tree closure allocations.** `Ascend`/`Descend` callbacks may heap-allocate and break zero-allocation reads (§22.2). Measure in S3; fall back to a reused `IterG`.
- **Batch prepare complexity (S7).** The highest-risk code. Keep P-02 sequential semantics and let the model test (batch ≡ sequential Appends) carry correctness.
- **Sentinel overflow.** Late threshold, gating `to-1`, AtAll `ts+1`, Continue `last.Seq+1`. Each needs a boundary test.
- **Lock hold time with Clone OnRead.** Expected and documented; MaxReadBatch is the only lever in v1.
- **Race-test runtime.** The race detector costs 2–20× time and 5–10× memory (§23); size stress tests through a build-tag constant.
- **Mode-equivalence noise in benchmarks.** Q-017 requires "no statistically significant difference" for ReadFrom/Subscription.Next; use `-count=10` + benchstat, and keep the `time` field off hot struct cache lines if needed.
- **Spec questions:** none open. P-01, P-02, P-04 and P-13 were settled on 2026-10-11 (P-13 deviates from §19; tests must cover the forward-read gate at `EventTime == wm`, `wm+1`, and `wm == MaxTimestamp`).

## 8. Out of scope (v1)

Everything in spec §4.2 and §32: durability, age retention, extra time-index modes, RWMutex variant, composite key+time index, snapshots/known-as-of, tombstones, immutable pages, persisted cursors.
