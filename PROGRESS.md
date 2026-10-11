# Progress

Plan: `IMPLEMENTATION_PLAN.md`. Spec: `temporal_series_requirements.md`. Decisions: `DECISIONS.md`.

## Status

| Session | State |
|---|---|
| 1 Foundations and ring | done |
| 2 Series core (Seq path) | done |
| 3 Event-time ordering capability | done |
| 4 Event-time point queries, status precedence, watermark gating | done |
| 5 Batch time reads, TimeCursor pagination, Entries | done |
| 6 Key indexes (single-record Append) | done |
| 7 AppendBatch | done |
| 8 Subscriptions | done |
| 9 Derived series and watermark propagation | done |
| 10 Verification hardening | done |
| 11 Benchmarks, allocation acceptance, docs, DoD audit | done |
| 12 (optional) Trace replay fixtures | needs captured feed data |

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
- W-009, TI-012: `TestAllocs_AppendByMode` (None 0; BTree ~0.06 allocs/op from node churn, see S7 note), `TestTimeIndex_BTreeAllocs` (all primitives 0)
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

## Session 5 — Batch time reads, TimeCursor pagination, Entries

Done:
- `query_batch.go`: `Range`, `RangeValues`, `AtAll`, `Continue`, `ContinueValues`, `PrevN`, `NextN`, `Entries`; two-step reads through `keyScratch`; `ascendPlan` shared by Range/AtAll/Continue; cursor validation per §8.4/P-14 (Disabled → Invalid → Reset → NotReady); `ReadInfo.Evicted` per P-05; NextN gating per P-13/S4-01/S5-04.
- Model: `rangeQ`, `atAll`, `continueQ`, `prevN`, `nextN` (linear scans over `byTime`), expected cursors via `makeCursor`; random runs keep a cursor pool (stale after Clear, foreign, future-generation, zero) and assert that OK, NotFound, Expired, NotReady, Invalid, Reset, truncation and successful continuation all occur.

Tests per requirement:
- Q-005, Q-013, Q-014, T-024: `TestRange_PaginationConcatenates` (every limit 1..9, entries and values)
- T-024, §8.4: `TestContinue_ConcurrentChanges` (late insert behind/ahead/tie, eviction between pages, everything evicted → Expired)
- E-016, T-016, T-024: `TestContinue_CursorValidation` (zero, foreign, future gen → Invalid; old gen → Reset; Invalid before Reset; Reset before NotReady; None → Disabled even with a foreign cursor)
- §19, P-10: `TestRange_WatermarkGate` (to-1 == wm, wm+1, `to == MinTimestamp`, wm == MaxTimestamp)
- E-002, §8.1: `TestBatch_EdgeRules`
- Q-002, Q-014: `TestAtAll_DuplicatesAndContinue`
- Q-006, §16.3: `TestPrevNNextN_Partition` (randomized: PrevN ++ NextN == Range; PrevN last == AsOf; no look-ahead), `TestPrevNNextN_Limits`
- P-13, S4-01, S5-04: `TestNextN_WatermarkGate`
- Q-011, Q-013, §8.2: `TestBatch_LimitAndDst`
- T-023, E-015, Q-016: `TestBatch_NoneNoStateChange` (all batch methods and Entries)
- Q-015, §8.3, C-008, P-15: `TestEntries` (paged result, early break, status up front, calling Append/AsOf inside the loop under a timeout, Clear ends the scan)
- T-015: `TestBatch_ClonePanicReleasesLock`
- T-026 (part): `TestAllocs_BatchReads` (Range, RangeValues, AtAll, Continue, ContinueValues, PrevN, NextN: 0 allocs)
- T-002: `TestModel_SeqPath` covers all batch reads in both modes. Mutation checks: Continue resuming at `last.seq` instead of `last.seq+1`, an off-by-one envelope intersection, and dropping the NextN "pending" rule each fail it.

Open questions: none.

Known gaps: none for S5 scope.

## Session 6 — Key indexes (single-record Append)

Done:
- `key_index.go`: `Extractor`, full `Index[T]` interface (S6-01), `decision`, `bitset`, shared `keyCore` (per-slot keys + indexed bits, per-position scratch, panic-recovering `extract`), `UniqueIndex` (map, all four policies, pending overlay for S7, K-008 eviction, `Get`/`Contains`/`Len`, `checkLocked`), `MultiIndex` (postings FIFO with compaction and free list, `Get`/`Latest`/`Count`/`Keys`, `checkLocked`).
- Series: binding in `New` (S6-02), prepare step 6 with Drop-wins/Reject-fails (`prepareIndexes`, `commitDrop`), commit order evict → ring write → index commit, `Clear` resets indexes, `Stats.Indexes`, `CheckInvariants` runs §14.1 checks per index.
- Model: `model_index_test.go` with per-record (key, ok, indexed) state and linear-scan lookups; random runs register 0-2 indexes (unique with random policy or multi; value-period keys that often equal the evicted row's key; EventTime keys; a panicking extractor) and compare append results, typed errors, Stats.Indexes and every lookup. Coverage guard asserts drops, Replace/KeepFirst, conflicts and extractor panics all occur.

Tests per requirement:
- T-011 (single append), K-007, E-011: `TestUniqueIndex_Policies`, `TestUniqueIndex_MultipleIndexes` (Drop beats Replace/KeepFirst; Reject beats Drop; dropped item modifies no index)
- T-012, E-012: `TestUniqueIndex_KeyOfEvictedRow` (capacity 1 and full ring)
- T-013, K-008: `TestUniqueIndex_EvictionKeepsNewerMapping`
- T-014, E-007: `TestIndex_ExtractorPanicNoStateChange`
- K-005: `TestIndex_ExtractorSeesPreparedEntry`
- Q-010, K-009, §10.4: `TestMultiIndex_Lookups` (orders, limits, compaction under churn, Clear)
- K-003: `TestIndex_Binding`
- K-011, Q-017: `TestIndex_ModeIndependence`
- §12.1: `TestConfig_ValidateInvalid` (unknown policy, nil extractors)
- T-026 (part), W-009: `TestAllocs_KeyIndexes` (lookups 0 allocs; Append with unique+multi index measured 0.0000 allocs/op with the exact counter in steady state)
- T-002, T-010: `TestModel_SeqPath` (indexes in both modes; invariants after every op). Mutation checks: the E-012 `evictBelow` boundary and "Drop wins" each fail targeted tests and the model; removing the K-008 guard is an equivalent mutant (S6-08).

Open questions: none.

Known gaps: none for S6 scope. Batch staging (`beginBatch`/`stage`/`endBatch`) is implemented but first exercised in S7.

## Session 7 — AppendBatch

Done:
- `batch.go`: `AppendBatch` (closed → empty no-op → ErrBatchTooLarge → prepare simulation → commit → one `applyTime` → one notify). `prepareBatch` simulates next Seq, oldest/len (per-item `evictBelow`), MaxObserved and the SourceSeq tracker; indexes stage each non-dropped item's key in the pending overlay (P-02). `commitBatch` applies plans in order; drops consume no Seq. Errors carry the batch position (S7-01).
- Series scratch: `plans`, `cloned` (S7-06).
- Model: `appendBatch` = the same Appends on a deep copy, adopted only if all succeed; failure moves only Rejected and the rejecting index's Conflicts. Random runs use batch sizes 0..Capacity+1 and assert both successful and failed batches occur.
- Test infrastructure: `allocsPerOp` exact allocation counter replaces `testing.AllocsPerRun` in every allocation test (S7-04).

Tests per requirement:
- W-002, P-02: `TestAppendBatch_EquivalentToSequential` (direct oracle: random valid batches vs the same Appends one by one on a twin Series, both modes, Drop/Replace/KeepFirst indexes; results, Stats and contents equal)
- T-011 (batch): `TestAppendBatch_IntraBatchConflicts` (Reject with BatchPos and no leaked overlay, Drop, Replace chain, KeepFirst, several indexes with Drop winning inside a batch)
- T-012 (batch), E-012: `TestAppendBatch_KeysOfRowsEvictedInBatch` (capacities 1, 2, 5; reversed order still conflicts)
- T-014 (batch), E-007: `TestAppendBatch_PanicNoStateChange` (extractor and clone panic at item 2)
- W-003, P-09, W-008: `TestAppendBatch_Limits`
- §5.3, §9.2: `TestAppendBatch_SequentialPolicies` (late and SourceSeq flags see earlier items; LateReject mid-batch; invalid input mid-batch)
- W-010, §9.4: `TestAppendBatch_OneApply`
- T-026, W-009: `TestAllocs_AppendBatch` (None: exactly 0; BTree reported, ~4 allocs per 64-record batch)
- T-002: `TestModel_SeqPath` covers AppendBatch in both modes with indexes. Mutation checks: skipping `stage`, not advancing the simulated oldest, and not advancing the simulated MaxObserved each fail targeted tests and the model.

Open questions: none.

Known gaps:
- W-002's "single notification": tested in S8 by `TestNotify_OncePerTransaction`.
- B-tree node churn costs ~0.06 allocs/record (S7-05); to be quantified in S11.

## Session 8 — Subscriptions

Done:
- `subscription.go`: `StartFrom` (`StartOldest`, `StartNext`, `StartAt`), `SubscribeOptions`, `Subscribe`, `Subscription` (`Next`, `TryNext`, `Seek`, `Cursor`, `Skipped`, `Wake`, `Close`), coalesced wake channels (buffer 1), copy-on-write subscriber list in `atomic.Pointer`, allocation-free non-blocking `notify` after unlock (Append, AppendBatch, Clear, Close), shared `poll` with the P-12 snapshot.
- `Stats.Subscribers`.
- `scripts/check.sh`: test timeouts.

Tests per requirement:
- S-001, S-002, P-07: `TestSubscribe_StartPositions`
- T-007, S-003, S-010: `TestSubscription_ReplayAcrossBatches`
- S-004, S-005, E-008, T-007: `TestSubscription_Lag` (LagFail contents and unchanged cursor, LagSkip and `Skipped()`, exactly one record behind, StartAt an evicted ref)
- S-006, T-016: `TestSubscription_ResetAndSeek` (ErrReset until Seek; every Seek variant incl. clamp, zero ref, old and future generation)
- T-019, E-013, E-014, S-008, S-009: `TestSubscription_UnblockAndClose` (ctx cancel, cancelled ctx with data, Subscription.Close, Series.Close drain then ErrClosed, blocked Next woken by Series.Close, subscribe on closed Series)
- S-007, S-011, T-018, §17.3: `TestSubscription_WakeCoalescing` (50 stalled subscribers never block 20k appends; wake buffer exactly 1; SetWatermark does not wake)
- W-002, §9.4: `TestNotify_OncePerTransaction` (temporaldebug: one per committed batch; none for failed or fully dropped transactions)
- T-018, T-001: `TestSubscription_Stress` (4 producers mixing Append/AppendBatch, 4 consumers; every record exactly once, in order)
- S-013, Q-017: `TestSubscription_ModeIndependence`
- T-015, S-005: `TestSubscription_ClonePanic`
- T-026, W-009: `TestAllocs_Subscriptions` (Append with 8 subscribers, TryNext, Next with data: 0 allocs)
- Exit gate: `go test -race -count=20 -run Subscription` green.
- Mutation checks: the lag boundary (found a gap: added the "one record behind" case), LagSkip accounting, and a blocking wake send (producer test times out) are all caught.

Open questions: none.

Known gaps: none (subscriptions joined the random model and fuzz target in S10).

## Session 9 — Derived series and watermark propagation

Done:
- `derive.go`: `DeriveFunc`, `DeriveOptions`, `WatermarkMode`, `DeriverState`, `DeriverStatus`, `Deriver` (`Status`, `Checkpoint`, `Stop`, `Done`, `Err`), `StartDerived`; worker loop: snapshot read → derive outside locks (panics recovered) → one AppendBatch → advance processed cursor → WatermarkPropagate when the batch reached the source's newest record → lag metrics.
- `subscription.go`: unset `StartFrom` default (S9-01); internal `next` returning the P-12 snapshot.

Tests per requirement:
- D-001, D-003, D-007, D-008: `TestDerived_Pipeline` (all four None/BTree combinations)
- D-004, T-020: `TestDerived_FailuresKeepCursor` (derive error, derive panic, unique Reject on append, ErrBatchTooLarge, LagFail, source Clear; each leaves the processed cursor at the last committed batch and the destination unchanged)
- §18.4, T-020: `TestDerived_WatermarkPropagation` (only when caught up; WatermarkManual untouched)
- D-005, D-006: `TestDerived_DeterministicAndCheckpoint` (two derivers with different batch sizes and modes produce identical destinations; a stateful derivation resumes exactly from a checkpoint)
- D-007: `TestDerived_LagAndSourceClose` (LagRecords and LagTime; source Close → Stopped with ErrClosed)
- §18.2: `TestDerived_ValidationAndCancel`
- Exit gate: `go test -race -count=10 -run Derived` green (also run 30x under race and 50x plain).
- Mutation checks: propagating the watermark when not caught up, and advancing the cursor on a failed append, are both caught.

Open questions:
- Per §17.3, watermark changes do not wake subscribers, so a deriver propagates a new source watermark only when the next source record arrives. On a quiet market (no trades for a while) a derived candle series' watermark therefore stays behind the raw series. Implemented as specified; a possible extension is to let derivers (not general subscribers) also wake on watermark changes.

Known gaps: none for S9 scope.

## Session 10 — Verification hardening

Done:
- `model_sub_test.go`: subscription model (start positions, §17.2 read step, lag/skip, reset, clamp, Seek, Close); random runs now also Subscribe/TryNext/Seek/Close and compare cursors, `Skipped()`, batches, typed lag errors and `Stats.Subscribers`.
- `fuzz_test.go`: `FuzzSeries` (T-003) drives `runSeqModel` from the input bytes via `byteSource`: capacity 1..8, both modes, 0-2 indexes with random policies and extractors (incl. panicking), every operation in the model (Append, AppendBatch, all Seq/point/batch reads, Continue with stale/foreign/future cursors, key lookups, subscriptions, Clear, Close, SetWatermark), CheckInvariants after each op. Seed corpus: six `f.Add` inputs of 0-4096 bytes.
- `TestModel_LongRuns` (T-002): 3000-step overwrite-heavy runs at capacities 1..8 in both modes.
- `verification_test.go`: `TestModeEquivalence` (T-022), `TestRaceStress_NoTornReads` (T-005), `TestClone_DeepOwnershipAllPaths` (T-006).
- `invariants_test.go`: `TestCheckInvariants_DetectsCorruption` (T-010: 17 corruptions of bounds, envelope, trackers, ring metadata, time index, unique and multi indexes are each detected), `TestMultiIndex_InvalidOrder`.
- `MultiIndex.Get` rejects an unknown Order (S10-01).
- Lock-discipline audit recorded in `DECISIONS.md`.

Results:
- `go test -race ./...` green (via `scripts/check.sh`).
- Statement coverage: 95.3% (target ≥ 90%). Uncovered: `noTimeIndex` no-op methods, debug-only helpers, and some invariant error branches.
- Fuzzing: `go test -run='^$' -fuzz=FuzzSeries -fuzztime=10m` passed: 7.2M executions, 335 corpus entries, no failures (Apple M4, 10 workers).

Tests per requirement:
- T-002: `TestModel_SeqPath`, `TestModel_LongRuns`; T-003: `FuzzSeries`; T-005: `TestRaceStress_NoTornReads`; T-006: `TestClone_DeepOwnershipAllPaths` (all 21 read paths under CloneOnRead and CloneOnReadWrite; producer buffers under CloneOnWrite and CloneOnReadWrite for Append and AppendBatch); T-010: model/fuzz after every op plus `TestCheckInvariants_DetectsCorruption`; T-022: `TestModeEquivalence`.

Open questions: none new.

Known gaps: none for S10 scope.

## Session 11 — Benchmarks, allocation acceptance, docs, DoD audit

Done:
- `bench_test.go`: §22.3 comparison (`BenchmarkCompare_*`: Append, AppendBatch 8/64/256/1024, ReadFrom, Subscription.Next, AsOf, Range; records/s, ns/op, B/op, allocs/op), §22.1 subset (`BenchmarkMatrix_*`: disorder and ties, value size and capacity incl. non-power-of-two, key indexes with high/low cardinality, the query set at batch sizes 1-4096 with nil and preallocated dst, paged Entries scan, contended Append), `BenchmarkWriteLatency` (p50/p99 under 1/2/8/32 readers and read sizes 64/4096), `TestMemoryPerRow`, `TestSustainedIngestionHeapBounded` (T-009).
- `alloc_test.go`: `TestAllocAcceptance`, the consolidated §22.2 table (T-026), all exactly zero.
- `example_test.go`: five testable examples mirroring §25 (raw series with dedup and AsOf, statuses, Range+Continue, subscription lag handling, StartDerived).
- `doc.go`: complete package documentation (identity, time and late events, event-time capability, statuses, watermarks, key indexes, writes, ownership and Clone, subscriptions and derivation, synchronization model, allocation).
- `scripts/bench.sh`, `scripts/benchcmp.py`; results in `BENCHMARKS.md` and `bench/`.

Key results (BENCHMARKS.md): Append 25.6 ns (None) vs 95.9 ns (BTree); Q-017 confirmed (ReadFrom p=0.06, Subscription.Next p=0.43); B-tree write path ~0.066 allocs/record; 64 / 99 / 108 / 93 bytes per row for None / BTree / +unique / +multi; heap bounded under sustained ingestion; writer p99 up to ~1-2 ms under tight-loop readers (S11-03).

Open questions:
- Writer tail latency under polling readers (BENCHMARKS.md §4): keep `sync.Mutex` with usage guidance, lower the default MaxReadBatch, or run the §22.4 RWMutex experiment?
- Deriver watermark propagation on quiet markets (S9 open question).

Known gaps:
- Session 12 (trace replay fixtures, T-008) needs captured feed data.

## Definition of Done audit (spec §31)

General:

| §31 item | Evidence |
|---|---|
| No public API exposes memory that a later append can overwrite. | Copy-out on every read path (lock audit in DECISIONS.md); `TestRaceStress_NoTornReads` (T-005); `TestClone_DeepOwnershipAllPaths` (21 read paths). Batch reads append into caller-owned `dst`; `ReadInfo`/`Entry` hold no references into the ring. |
| Every query reports Status or ReadInfo exactly per §7; status matrix and precedence tests pass. | `TestPointQueries_StatusMatrix`, `TestPointQueries_Precedence`, `TestContinue_CursorValidation`, `TestGet_StatusMatrix`, `TestReadFrom`; every status of every query compared with the model in `TestModel_SeqPath`/`FuzzSeries`. |
| `go test -race` passes under dedicated reader/writer/subscriber stress tests. | `scripts/check.sh` (race step); `TestRaceStress_NoTornReads`, `TestSubscription_Stress`, `TestAppend_Concurrent`; `-race -count=20 -run Subscription` and `-race -count=10 -run Derived` gates. |
| Randomized reference-model tests prove ring, time index and key indexes equivalent, incl. all conflict policies and Clear. | `TestModel_SeqPath`, `TestModel_LongRuns`, `FuzzSeries` (model incl. all four policies, multi indexes, Clear, batches, subscriptions; CheckInvariants after every op); `TestTimeIndex_Differential`. |
| AsOf and range queries correct for equal timestamps, late events, SourceEpoch changes, wraparound and generations. | `TestPointQueries_DuplicatesAndLate`, `TestRange_PaginationConcatenates`, `TestAppend_SourceSeqPolicy`, `TestModel_LongRuns` (capacities 1..8, thousands of wraps), `TestGet_StatusMatrix`/`TestContinue_CursorValidation` (generations). |
| Subscribers process every record within retention and report deterministic lag or reset. | `TestSubscription_Stress`, `TestSubscription_ReplayAcrossBatches`, `TestSubscription_Lag`, `TestSubscription_ResetAndSeek`, subscription model in `FuzzSeries`. |
| Failed appends, incl. recovered callback panics, leave no state change. | `TestAppend_ClonePanicNoStateChange`, `TestIndex_ExtractorPanicNoStateChange`, `TestAppendBatch_PanicNoStateChange`, `TestAppend_InvalidInput`, `TestAppendBatch_IntraBatchConflicts/Reject`; model compares Stats after every failed append. Only diagnostic counters move (P-04). |
| Steady-state hot paths meet the §22.2 allocation targets. | `TestAllocAcceptance` (exact counting, every §22.2 item = 0); B-tree write path ~0.06 allocs/record measured and reported (S7-05, BENCHMARKS.md). |
| No serialization, reflection, `[]byte` keys, or T/K to `interface{}` on normal paths. | `TestStructure_Imports` bans `reflect`/`unsafe`; keys stay typed in per-index scratch (S6-01); `fmt.Sprint(k)` only on the conflict error path (P-18); `CallbackPanicError.Value any` only on the panic path. |
| Synchronization model documented in package comments and enforced structurally. | `doc.go` "Synchronization model"; `TestStructure_NoLockFields`, `TestStructure_BTreeNoLocks`; lock-discipline audit in DECISIONS.md. |

Event-time ordering capability:

| §31 item | Evidence |
|---|---|
| Series core has no direct dependency on a concrete B-tree API (T-025a). | `TestStructure_Imports`. |
| btreeTimeIndex and the TimeIndexNone path pass the common suite (T-021..T-023). | `TestTimeIndex_*` (btree vs slice reference), `TestPointQueries_NoneNoStateChange`, `TestBatch_NoneNoStateChange`, `TestTimeIndex_None`. |
| No index implementation has its own lock (T-025b). | `TestStructure_NoLockFields`. |
| One Series has exactly one consistency boundary (C-001). | `Series.mu` is the only lock touching Series state (lock audit); index types have no lock fields (T-025b). |
| ReadFrom and subscriptions work fully without an event-time index (T-022). | `TestModeEquivalence`, `TestSubscription_ModeIndependence`, model runs in None mode. |
| Disabling the event-time index does not change Seq, Gen or retention semantics (T-022). | `TestModeEquivalence` (identical results and Stats except time-only fields), `TestIndex_ModeIndependence`. |
| Public API requires no B-tree types; continuation uses the opaque TimeCursor (TI-013). | `TimeCursor` fields are unexported; the btree import is confined to one file (T-025a). |
| Benchmarks measure event-time indexing cost separately (22.3). | `BenchmarkCompare_*` (None vs BTree), BENCHMARKS.md. |
