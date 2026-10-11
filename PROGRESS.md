# Progress

Plan: `IMPLEMENTATION_PLAN.md`. Spec: `temporal_series_requirements.md`. Decisions: `DECISIONS.md`.

## Status

| Session | State |
|---|---|
| 1 Foundations and ring | done |
| 2 Series core (Seq path) | next |

## Session 1 — Foundations and ring

Done:
- `go.mod` (`github.com/fedorwk/temporal`, `go 1.23`), `scripts/check.sh` (gofmt, vet in both tag sets, `go test -race`, `go test -tags temporaldebug`).
- `temporal/doc.go`: synchronization model stub.
- `temporal/types.go`: Seq/SeqNone/seqMax, Gen, Ref, Timestamp + sentinels, FromTime/Time, SourceEpoch, Flags + masks, InputMeta, Meta, Entry, Input, Cursor, timeKey/keyLess.
- `temporal/status.go`: Status (+String, Err), all sentinel errors, LagError, ConflictError, CallbackPanicError, ReadInfo, TimeCursor (+makeCursor/decode), Outcome, AppendResult.
- `temporal/config.go`: all enums, Config[T], `validate()` → normalized `config[T]`.
- `temporal/debug_on.go` / `debug_off.go`: `debugAssert`, `assertf`.
- `temporal/ring.go`: struct-of-arrays `ring[T]` (32-byte `slotMeta`, debug-only Seq array), `bounds`.
- `temporal/key_index.go`: `Index[T]` stub (S6 completes it).

Tests per requirement:
- §6.1 Timestamp: `TestTimestamp_RoundTrip`, `TestTimestamp_Sentinels`
- §6.2 Ref: `TestRef_IsZero`
- §6.3 Flags: `TestFlags_Masks`
- §6.5 timeKey order: `TestKeyLess`
- §7.1 Status.Err: `TestStatus_Err`, `TestSentinels_Distinct`
- §21 typed errors: `TestTypedErrors_IsAs`
- §8.4 cursor shape: `TestTimeCursor_EncodeDecode`
- §12.1 config validation: `TestConfig_ValidateInvalid`, `TestConfig_ValidateNormalizes` (incl. P-17)
- §12.1/§12.2 ring and bounds: `TestRing_PushRetainEvict`, `TestRing_SlotFormula`, `TestBounds_EmptyAndReset`, `TestRing_ZeroClearsReferences`, `TestSlotMeta_Size` (P-16)

Open questions: none.

Known gaps: none for S1 scope. `Order.valid`, `ConflictPolicy.valid`, `LagPolicy.valid` are unused until S6/S8.
