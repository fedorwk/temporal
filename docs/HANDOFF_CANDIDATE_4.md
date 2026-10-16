# Handoff: candidate 4, narrow the Subscription read interface

Written 2026-10-16 at the end of the session that implemented candidates 1-3 of `docs/ARCHITECTURE_IMPROVEMENTS.md`. Read this first, then the candidate's section in that file.

## Where things stand

- Branch `claude/arch-improvements-48da71` (worktree `.claude/worktrees/arch-improvements-48da71`). It has not been pushed and has no PR yet; the user will ask for the PR separately.
- Commits on top of `0b2feb6` (the candidates doc):
  - `5f8f05c` candidate 1, admission state (`admission.go`), decision **S13-01**
  - `71bebb3` candidate 2, change signal (`signal.go`), decision **S13-02**
  - `8e8a94f` candidate 3, key-index set (`key_index_set.go`), decision **S13-03**
- `scripts/check.sh` is green on `8e8a94f`.
- Candidate 4 is **not started**. It is a breaking change to a public interface, so the user must agree to the interface before any code is written. Candidate 5 (speculative) also needs a go-ahead.

## The problem, in the code

`subscription.go` has five read methods that are thin wrappers over one locked step, `poll(dst, limit) (dst, SourceState, got bool, err)`:

| Method | Blocks | Returns state | Notes |
|---|---|---|---|
| `Next(ctx, dst, limit)` | yes | no | `NextState` without the state |
| `NextState(ctx, dst, limit)` | yes | yes | loops `poll` + waits on `sub.wake`/`ctx`/`sub.done`; checks `ctx.Err()` first (S8-01) |
| `TryNext(dst, limit)` | no | no | one `poll`, drops `got` |
| `TryNextState(dst, limit)` | no | yes | one `poll`, zero state on error (S12-10) |
| `All(ctx, limit)` | yes | no | `iter.Seq2` over `Next`; ends silently on plain `ErrClosed`; a break `Seek`s back to the first unyielded record (S12-09) |

What leaks to callers:
- `got` means "new records" **or** "the watermark moved" (WatermarkUpdates, S12-08). `TryNext`/`TryNextState` throw `got` away, so a caller can't tell "watermark advanced" from "nothing".
- The TryNextState drain rule exists only in a doc comment: after a wake-up, call until a call returns neither records nor a changed watermark, because one wake can stand for both a watermark advance and a close, and the advance is reported first.
- `Subscription.lastWM` (the watermark last returned) is reset to `MinTimestamp` by `Seek`, so the first poll after a Seek reports a watermark-only update.
- Each phase-5 commit widened this surface by a method or option.

The derive-loop users are `README.md` ("Derived series" section, `deriveNotional`), `ExampleSubscription_NextState` and the loop at `example_test.go:178`, `doc.go` (lines ~8 and ~138), and gotrade's multi-source pipelines, which use `TryNextState` + `Wake()` (S12-10).

## The goal (from the candidate)

Make the single locked read step the interface. It reports records, source state and "watermark advanced" as distinct facts. Blocking reads and `All` become thin conveniences on top, the derive loop reads one result shape, and the drain rule becomes a return value instead of a comment.

## Design starting point (to propose to the user, not decided)

A sketch to put in front of the user; adjust freely:

```go
// Update describes what one read found.
type Update struct {
	State     SourceState // captured under the same lock as the records
	Watermark bool        // State.Watermark is newer than the last one this subscription reported
}

// Poll is one non-blocking read step: it appends up to min(limit, MaxReadBatch)
// records and reports what changed. len(new records) == 0 && !u.Watermark
// means nothing is pending: wait on Wake() (or use Wait).
func (sub *Subscription[T]) Poll(dst []Entry[T], limit int) ([]Entry[T], Update, error)

// Wait blocks until Poll may have something, ctx is done, or the
// subscription closes. (Or: a blocking Read(ctx, dst, limit) built on Poll.)
```

Questions only the user can answer:
1. **Remove or deprecate?** Either replace `NextState`/`TryNextState` (and maybe `TryNext`) outright, or add the new step and mark the old ones `// Deprecated:`, removing them later. gotrade is a known caller; check with the user how it is pinned. Pre-1.0 module, but S12-xx decisions were made for gotrade.
2. **Keep `Next` and `All`?** They are the simple-consumer conveniences, and S12-09 was requested for demos and tests. Likely keep both, built on the new step.
3. **Seek and `lastWM`.** Keep "Seek resets, so the next read reports the watermark", or make Seek preserve it? Behaviour should stay identical unless the user says otherwise.
4. **Naming.** `Poll`/`Update`/`Wait`, or `Read`/`ReadResult`; match the README vocabulary.

Recommendation to bring: keep `Next`, `TryNext`, `All` and `Wake`; add the step (`Poll` + `Update`) plus a blocking form that returns the same `Update`; deprecate `NextState`/`TryNextState` rather than delete them in this change; keep the Seek behaviour.

## Constraints (settled; don't re-litigate)

- **S12-02**: derive loops belong to the caller; the library starts no goroutines.
- **P-12 / S8-05**: the state is captured under the same lock as the records; it is zero on error.
- **S12-08**: the watermark update is checked **before** the closed check, so a watermark set just before `Close` is reported before `ErrClosed`. Records returned also record their state's watermark, so an advance is reported once. Without `WatermarkUpdates` nothing changes (P-10: subscriptions are not woken by watermark changes).
- **S8-01**: blocking reads check `ctx.Err()` before reading, with the cursor unchanged.
- **S8-03, S8-04, P-07**: StartAt validation on every read; TryNext reports the same errors as Next; Subscribe on a closed Series drains then returns the close error (S12-05: `ErrClosed` or `*ClosedError`; a subscription's own `Close`/`Seek` return plain `ErrClosed`).
- **S-005**: the cursor and `skipped` move only after a successful copy.
- **S12-09**: `All` ends silently on plain `ErrClosed`, yields every other error once, and a break rewinds.
- **S7-04 / alloc_test.go**: "subscriber drain (TryNext/Next)" must stay at exactly 0 allocations. Keep `Update` a plain value with no pointers that escape.
- Wake-ups now go through `changeSignal` (S13-02, `signal.go`). `sub.sub.wake` is still the coalesced channel; nothing in candidate 4 should need to touch `signal.go`.

## Process the user asked for

1. Read the candidate section plus decisions P-07, P-12, S8-01..S8-05, S12-02, S12-05, S12-08, S12-09, S12-10.
2. Propose the interface (answering the four questions above) and get agreement **before** coding.
3. Keep behaviour identical. Update `README.md` (API table around line 222, the "Derived series" section, the `Wake()` note around line 358), `doc.go`, and both examples in `example_test.go`. Example output must not change.
4. Verify with `scripts/check.sh` (gofmt, vet, plain tests incl. alloc tests, `-race`, `-tags temporaldebug`). Benchmarks to compare before and after: `BenchmarkCompare_SubscriptionNext`, plus the alloc table.
5. One commit, a new decision (**S13-04**) in `docs/DECISIONS.md` (append after S13-03, before the lock audit), and mark candidate 4 done in `docs/ARCHITECTURE_IMPROVEMENTS.md` (table row plus a **Status:** line, as for 1-3). Update the lock-audit row for `Subscribe`, `unsubscribe`, `poll`, `Seek` if method names change.
6. Don't push or open a PR unless asked.

## Tests to lean on

- `subscription_test.go` (12 tests), `subscription_all_test.go` (All), `wait_watermark_test.go` (WatermarkUpdates and the drain-order cases, e.g. "record with advance" around line 219), `close_error_test.go`, and `verification_test.go` (compares two Series' TryNext results step by step).
- `model_sub_test.go` is the subscription oracle.
- New: a test that the step reports `Watermark` exactly once per advance, including after Seek and before close.

## Lessons from candidates 1-3 (measurement)

- The machine is shared and noisy (Spotlight, other sessions); medians swing 2x. Compare interleaved A/B binaries and use **minimums** over many short rounds. The method was: `git archive <base> | tar -x` into a scratch dir, `go test -c` there and in the worktree, alternate `-test.bench ... -test.benchtime 100ms` runs 8-16 times, and compare with `scripts/benchcmp.py`'s `parse`/`mann_whitney_p`. Run Python with `-I -B`, or it drops `__pycache__` into `scripts/`. `-I` ignores `PYTHON*` env vars.
- Go's inline budget is 80. On ~25 ns paths, one extra non-inlined call cost ~1 ns. `go build -gcflags=-m=2` shows costs. `Next` → `NextState` → `poll` is already two calls deep; keep the step inlinable or no deeper than today.
- Field layout of `Series` didn't matter (padding control), but code shape did: small differences of ±0.4 ns persisted. Record honest numbers in the decision.
