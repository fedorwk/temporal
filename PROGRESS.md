# Progress

Plan: `IMPLEMENTATION_PLAN.md`. Spec: `temporal_series_requirements.md`. Decisions: `DECISIONS.md`.

## Status

| Session | State |
|---|---|
| 1 Foundations and ring | done |
| 2 Series core (Seq path) | done |
| 3 Event-time ordering capability | done |
| 4 Event-time point queries, status precedence, watermark gating | done |
| 5 Batch time reads, TimeCursor pagination, Entries | next |

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

## Session 2 — Series core on the Seq path

Done:
- `series.go`: `Series[T]`, `New`, `Append` (prepare per P-03 steps 1-5, commit, notify stub), `Clear`, `Close`, `Len`, `Cap`, `Gen`, `TimeIndexMode`, `OldestRef`, `NewestRef`; eviction envelope; late threshold with saturation (P-10); SourceSeq tracker; `callClone` with panic recovery.
- `query.go`: `Get`, `Latest`, `ReadFrom` (P-06), `copyOut` (Clone OnRead, fills `Meta.Ref.Gen`), effective limit.
- `watermark.go`: `SetWatermark` (monotonic, false after Close), `Watermark`.
- `stats.go`: `Stats`, `IndexStats` (time-index and index fields empty for now).
- `invariants.go`: `CheckInvariants` v1 (bounds, slot uniqueness, debug Seq array, envelope, MaxObserved, stored flags).
- `doc.go`: Seq/Gen/Ref semantics.
- Test infrastructure: `testutil_test.go` (`-temporal.iters`, `-temporal.seed`, helpers), `race_on/off_test.go`, `export_test.go` (`testSetNewest`, `testSetGen`).
- `scripts/check.sh` now also runs plain `go test ./...` so allocation tests execute.

Tests per requirement:
- T-002 (Seq path, both modes), T-010 (part): `TestModel_SeqPath` (reference model in `model_test.go`; compares results, errors, statuses, ReadInfo, entries, Stats, accessors; CheckInvariants after every op). Mutation-checked: breaking the late threshold or the envelope makes it fail.
- Q-001, E-001, T-016 (part): `TestGet_StatusMatrix`
- W-001, §9.5: `TestAppend_ResultAndMeta`
- E-009, E-010, P-04: `TestAppend_InvalidInput`
- §5.3, W-005, P-10: `TestAppend_LateDetection`
- W-006, E-005, S2-01: `TestAppend_SourceSeqPolicy`
- E-006: `TestAppend_GapFlagCounted`
- §6.2 ErrSeqExhausted: `TestAppend_SeqExhausted`; ErrGenExhausted: `TestClear_GenExhausted`
- §15.4: `TestClear_Resets`; §15.3: `TestEvictionEnvelope`
- W-008, E-013, P-08: `TestClose_Semantics`
- §19, T-020 (part): `TestWatermark_Monotonic`
- Q-007, Q-011, Q-013, §8.2, P-06: `TestReadFrom`
- W-007: `TestAppend_Concurrent`
- T-014 (part), E-007, W-004: `TestAppend_ClonePanicNoStateChange`
- T-015, C-007: `TestRead_ClonePanicReleasesLock`
- §11.5, T-006 (basic), P-17: `TestClone_Isolation`
- T-026 (part), W-009: `TestAllocs_SeqPath` (Append incl. late and rejected, Get, Latest, ReadFrom, accessors, SetWatermark: 0 allocs)

Open questions: none.

Known gaps:
- `Config.Indexes` is validated for names only; binding and maintenance arrive in S6.
- `TimeIndexBTree` is accepted but has no effect until S3.
- `notify()` is a no-op until S8.

## Session 3 — Event-time ordering capability

Done:
- `go.mod`: `github.com/tidwall/btree v1.8.2`.
- `time_index.go`: `timeIndex` interface (full §13.2 method set), `newTimeIndex(mode)` (nil for None).
- `time_index_btree.go`: `btreeTimeIndex` (NoLocks, two path hints, reusable iterator; debug assertions on Set/Delete preconditions).
- `time_index_none.go`: zero-size `noTimeIndex`.
- Series wiring: `time`, `keyScratch` (cap MaxReadBatch+1), `delKeys`/`insKeys` only when `time != nil`; one `Apply` per committed Append; `Clear` calls `time.Clear()`; `Stats.Min/MaxEventTime`; `CheckInvariants` verifies index length, order, retained Seqs and matching EventTimes via chunked ascend.

Tests per requirement:
- T-021, TI-011: `TestTimeIndex_Empty`, `TestTimeIndex_TiesAndPrimitives` (hand-computed expectations, limits 0/1/exact/over, dst append), `TestTimeIndex_SentinelAdjacent`, `TestTimeIndex_None`, `TestTimeIndex_Differential` (btree vs `sliceTimeIndex` reference after every op: FIFO eviction batches, single Delete+Set, Clear)
- T-025, TI-001, C-002: `TestStructure_Imports` (btree only in its file; no reflect/unsafe; no sync.Map), `TestStructure_NoLockFields`, `TestStructure_BTreeNoLocks`
- W-010, TI-007: `TestAppend_OneTimeIndexCallPerTransaction`
- TI-008: `TestTimeIndexNone_NoScratch`; TI-002: `TestClear_KeepsTimeIndexMode`
- W-009, TI-012: `TestAllocs_AppendByMode` (None 0; BTree measured 0 in steady state), `TestTimeIndex_BTreeAllocs` (all primitives 0)
- T-002/T-010 in BTree mode: `TestModel_SeqPath/mode=1` now checks index invariants after every op (mutation-checked: dropping half of the deletes in Apply fails it).

Open questions: none.

Known gaps: none for S3 scope.

## Session 4 — Event-time point queries, status precedence, watermark gating

Done:
- `query_time.go`: `AsOf`, `Before`, `After`, `AtOrAfter`, `FirstAt`, `LastAt`, `LatestByEventTime`; envelope helpers `expiredLE/LT/GT/GE/EQ`; precedence Disabled → NotReady → data; forward results per P-13 and S4-01.
- `watermark.go`: `gatedAt` (backward point/batch bound), `gatedBelow` (exclusive bound, overflow-safe, for S5), `finalAt` and `complete` (forward reads).
- Model: `byTime` view, `point()` for every point query by linear scan; random configs toggle `GateReadsByWatermark`; watermarks follow the data (incl. MaxTimestamp); the run asserts every status (OK, NotFound, Expired, NotReady; Disabled in None mode) actually occurs.

Tests per requirement:
- T-017, §7.2, E-003, Q-003/Q-004: `TestPointQueries_StatusMatrix` (empty; evicted below retained; evicted above retained; irrelevant evictions; Expired at MinEvicted−1/MinEvicted/MaxEvicted/MaxEvicted+1)
- §7.5, E-015: `TestPointQueries_Precedence` (Disabled beats NotReady on empty and non-empty series; NotReady beats Expired and OK)
- P-13, S4-01: `TestPointQueries_ForwardGate` (EventTime == wm, wm+1, wm == MaxTimestamp; late record behind the watermark; Expired wins with no match)
- T-004, E-004: `TestPointQueries_DuplicatesAndLate`
- T-023, Q-016: `TestPointQueries_NoneNoStateChange` (gate on/off, empty/non-empty, Stats and invariants unchanged)
- T-015: `TestPointQueries_ClonePanicReleasesLock`
- T-026 (part): `TestAllocs_PointQueries` (0 allocs for every point read, including a gated one)
- §16.3 no look-ahead, T-002: `TestModel_SeqPath` (checks AsOf/LastAt never return EventTime > ts; compares every point query with the model in both modes)
- Mutation checks: `expiredGT` boundary, `finalAt` boundary and the MaxTimestamp completeness rule each fail the targeted tests and the model test.

Open questions: none.

Known gaps: none for S4 scope.
