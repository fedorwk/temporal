# Architecture Improvements

Possible deepening refactors, from an architecture review on 2026-10-16. Each needs its own design pass before code changes; the ones that are done name the decision that records how.

The review focused on the phase-5 hot spots (subscription, watermark, write path). There is no `GLOSSARY.md` or `docs/adr/`; domain terms follow the spec and README, and `docs/DECISIONS.md` is treated as the decision log.

Vocabulary: a **module** is anything with an interface and an implementation. It is **deep** when a lot of behaviour sits behind a small interface and **shallow** when the interface is nearly as complex as the implementation. A **seam** is where an interface lives. **Locality** means change and verification concentrate in one place; **leverage** means callers get more behaviour per unit of interface.

| # | Candidate | Strength | Files |
|---|---|---|---|
| 1 | One admission state for Append and AppendBatch | Strong · **done (S13-01)** | series.go, batch.go, ring.go |
| 2 | A change-signal module for Series wake-ups | Strong · **done (S13-02)** | subscription.go, watermark.go, series.go, batch.go |
| 3 | Let the key-index set own its transaction protocol | Worth exploring · **done (S13-03)** | key_index.go, series.go, batch.go |
| 4 | Narrow the Subscription read interface | Worth exploring · **done (S13-04)** | subscription.go, example_test.go, README.md |
| 5 | One status-precedence step for event-time point reads | Speculative | query_time.go, watermark.go, query_batch.go |

**Top recommendation: 1.** P-02 is the library's central write-path promise, and today it depends on two copies of the same rules that nothing forces to agree. Folding them into one value makes that equivalence structural, removes the most parameters, and sets up candidate 3. Candidate 2 is a close second if the next feature touches wake-ups again.

---

## 1. One admission state for Append and AppendBatch

**Strength:** Strong · in-process

**Status:** Done, see DECISIONS S13-01. `admission.go` holds the state and its rules (`check`, `nextSeq`, `evictBelow`, `push` + `observe`, `reset`); commit admits through `s.adm` and the batch prepare phase runs the same calls on a copy. Admitting is split into `push` and `observe` so each inlines; Append was not turned into a one-item batch, to keep the S12-01 no-callback path as it was.

**Files:** `series.go` (`appendLocked`, `prepareMeta`, `commit`), `batch.go` (`prepareBatch`), `ring.go` (`bounds`)

**Problem.** P-02 (AppendBatch ≡ the same Appends in order) is held up by two hand-kept copies of the state-advance rules:

| Rule | Real state (`commit`, `appendLocked`) | Simulated state (`prepareBatch`) |
|---|---|---|
| Bounds advance | `s.b.push(capacity)` | local `switch` on `length`/`oldest`/`next`, re-implementing `bounds.push` |
| Late tracking | `s.maxObserved`, `s.hasObserved` | `maxObserved`, `hasObserved` locals |
| SourceSeq tracker | `s.srcSeq.observe(...)` | `trk.observe(...)` on a copy |
| `evictBelow` | computed in `appendLocked` | computed again in the loop body |

`prepareMeta(m, maxObserved, hasObserved, &trk, next)` takes five arguments only so it can be fed either copy. On top of that there are four entry functions (`Append`, `appendWithCallbacks`, `AppendBatch`, `appendBatchWithCallbacks`).

**Solution.** Gather bounds, late tracking and the SourceSeq tracker into one copyable value. The real commit advances `s.adm`; the batch simulation copies it (`sim := s.adm`) and advances the copy. Append can then run as a one-item batch through the same prepare loop.

```
before                                   after
commit ──► push / maxObserved / srcSeq    commit ───────┐
prepareBatch ──► switch / locals / trk    prepareBatch ─┴─► admission state
                 (same rules, twice)                        admit(meta) → plan | err
                                                            advance(meta)
                                                            evictBelow()
```

**Wins**
- Locality: the advance rules live in one place
- P-02 equivalence becomes structural, not maintained by hand
- `prepareMeta` loses four parameters
- Admission can be tested without a Series
- The model test stays the oracle

---

## 2. A change-signal module for Series wake-ups

**Strength:** Strong · in-process · hot spot (4 of the last 6 commits)

**Status:** Done, see DECISIONS S13-02. `signal.go` holds `changeSignal`: writers call `records()`, `watermark()` or `closed()` after unlocking, and WaitWatermark takes its channel from `watermarkWait()`. Three methods instead of one `fire(change)`, so the Append path inlines to the old notify loop; `dbgNotifies` moved into the module as `dbgFired`.

**Files:** `subscription.go` (`notify`, `notifyWatermark`, `subs`), `watermark.go` (`wmWait`, `takeWatermarkWaiters`), `series.go` (`Clear`, `CloseWithError`), `batch.go`

**Problem.** The rule "which event wakes whom" is spread over five writers and two channel mechanisms:

```
Append ─────────┐
AppendBatch ────┼──► notify ───────────► sub.wake
Clear ──────────┤                            ▲
CloseWithError ─┴──► takeWatermarkWaiters ─► wmWait chan
SetWatermark ──────► takeWatermarkWaiters    │
                └──► notifyWatermark ────────┘
notify ··debug··► dbgNotifies
```

Each phase-5 feature (WaitWatermark, WatermarkUpdates, CloseWithError) had to edit several of these call sites. The coalescing test reads `dbgNotifies` because the interface can't show the behaviour.

**Solution.** One module owns the subscriber list and the watermark waiters. After unlocking, writers report one of "records committed", "watermark advanced" or "closed", and the module decides who wakes. WaitWatermark is built on the same module.

**Wins**
- Locality: wake policy lives in one module
- The next wake-related feature touches one file
- Coalescing is tested through the module's own interface
- `dbgNotifies` can go
- "Close wakes everyone" can no longer be forgotten at a call site

---

## 3. Let the key-index set own its transaction protocol

**Strength:** Worth exploring · in-process

**Status:** Done, see DECISIONS S13-03. `key_index_set.go` holds `keyIndexes[T]` (`prepare`, `beginBatch`/`endBatch`, `commit`, `drop`, `evict`, `reset`); the batch overlay moved from `Index[T]` to the internal `conflictIndex` that only `UniqueIndex` implements, and the set is tested without a Series.

**Files:** `key_index.go` (`Index[T]`), `series.go` (`prepareIndexes`, `commitDrop`, `commit`), `batch.go` (`prepareBatch`)

**Problem.** `Index[T]` has 13 methods, and callers must also know their order: `beginBatch → prepare → stage → commit / noteDropped → endBatch`, plus `evict`, `reset`, `stats`, `checkLocked`. The Series drives that protocol from three places. The Drop-wins and first-index precedence rules (S6-05) live in `Series.prepareIndexes`, not with the indexes. `MultiIndex` implements four of the methods as no-ops. Index behaviour can only be tested through a whole Series.

**Solution.** Put the per-index protocol and decision-combining behind one index-set module that the write path calls a few times per item (roughly prepare, commit, evict, reset). `UniqueIndex` and `MultiIndex` stay as internal adapters behind it.

**Wins**
- Leverage: the Series learns about four calls instead of thirteen
- S6-05 precedence lives in one place
- Index-set tests don't need a ring
- Pairs naturally with candidate 1

**Decision log:** respects S6-01 and P-11 (typed per-position scratch, no boxing); that scratch stays inside the set.

---

## 4. Narrow the Subscription read interface

**Strength:** Worth exploring · in-process · hot spot (phase 5)

**Status:** Done, see DECISIONS S13-04. `Read`/`TryRead` return `([]Entry[T], Update, error)` with `Update{State, Records, WatermarkAdvanced}`; `NextState` and `TryNextState` were removed (breaking; gotrade is pinned and migrates on its next bump). `Next`, `TryNext`, `All` and the Seek behaviour are unchanged, and the drain rule is now `Records == 0 && !WatermarkAdvanced`.

**Files:** `subscription.go`, `example_test.go` (derive loop), `README.md`

**Problem.** `Next`, `NextState`, `TryNext`, `TryNextState` and `All` are five thin variants of one internal `poll → (dst, SourceState, got, err)`. Several details leak to callers:
- `got` means either "new records" or "the watermark moved".
- The TryNextState drain rule ("call until a call returns neither records nor a changed watermark") exists only in a doc comment.
- `lastWM` is reset on `Seek`, which changes when the next watermark-only return happens.

Each phase-5 commit widened the interface by one method or option.

**Solution.** Make the single locked read step the interface. It reports records, source state and "watermark advanced" as distinct facts. Blocking reads and the `All` iterator become thin conveniences on top. The derive loop reads one result shape and has no drain rule to remember.

**Wins**
- The interface shrinks; behaviour is unchanged
- The drain rule becomes a return value
- One test surface for read semantics

**Decision log:** stays inside S12-02 (derive loops belong to the caller, no library goroutines). It is a breaking change to a public interface, so weigh it against users already on `NextState`/`TryNextState`.

---

## 5. One status-precedence step for event-time point reads

**Strength:** Speculative · in-process

**Files:** `query_time.go`, `watermark.go` (`gatedAt`, `finalAt`, `complete`), `query_batch.go` (plans)

**Problem.** Status precedence (Disabled → NotReady → data → Expired/NotFound) is re-stated in six point reads (`AsOf`, `Before`, `After`, `AtOrAfter`, `FirstAt`, `LastAt`). Each pairs a lookup with the right envelope predicate (`expiredLE`, `expiredLT`, `expiredGT`, `expiredGE`, `expiredEQ`) by hand.

**Solution.** Describe each point read as (lookup, direction, envelope test) and run one precedence step. The batch reads already do this with `ascendPlan`; point reads would follow the same shape.

**Wins**
- Locality: precedence rules are written once
- The five `expired*` helpers fold in

**Why speculative:** this code hasn't changed since session 5, so the payoff only comes if more point reads are added.
