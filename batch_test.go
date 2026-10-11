package temporal

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

func tradeInputs(specs ...[3]int) []Input[trade] {
	out := make([]Input[trade], len(specs))
	for i, sp := range specs {
		out[i] = Input[trade]{Value: trade{ID: sp[0], Symbol: fmt.Sprint(sp[1])}, Meta: at(Timestamp(sp[2]))}
	}
	return out
}

// W-002, P-02: random valid batches give exactly the results of the same
// Appends made one by one, in every mode, with indexes under non-failing
// policies.
func TestAppendBatch_EquivalentToSequential(t *testing.T) {
	r, _ := newRand(t)
	for run := range iters(150) {
		capacity := 1 + r.IntN(8)
		mode := allModes[r.IntN(len(allModes))]
		mk := func() (*Series[trade], *UniqueIndex[trade, int], *MultiIndex[trade, string]) {
			policy := []ConflictPolicy{ConflictDrop, ConflictReplace, ConflictKeepFirst}[run%3]
			u := NewUniqueIndex("id", func(e *Entry[trade]) (int, bool) { return e.Value.ID % (capacity + 2), true }, policy)
			mi := NewMultiIndex("sym", bySymbol)
			s := mustNew(t, Config[trade]{
				Capacity:        capacity,
				TimeIndex:       mode,
				SourceSeqPolicy: SourceSeqFlag,
				Indexes:         []Index[trade]{u, mi},
			})
			return s, u, mi
		}
		batched, bu, bm := mk()
		seq, su, sm := mk()
		id := 0
		for range 40 {
			items := make([]Input[trade], r.IntN(capacity+1))
			for i := range items {
				id++
				items[i] = Input[trade]{
					Value: trade{ID: id, Symbol: []string{"A", "B"}[r.IntN(2)]},
					Meta:  InputMeta{EventTime: Timestamp(r.IntN(30)), SourceSeq: uint64(r.IntN(10))},
				}
			}
			got, err := batched.AppendBatch(nil, items)
			if err != nil {
				t.Fatalf("run %d: AppendBatch: %v", run, err)
			}
			var want []AppendResult
			for _, it := range items {
				want = append(want, mustAppend(t, seq, it.Value, it.Meta))
			}
			if !slices.Equal(got, want) {
				t.Fatalf("run %d: batch %+v\nsequential %+v", run, got, want)
			}
			if !statsEqual(batched.Stats(), seq.Stats()) {
				t.Fatalf("run %d: stats differ:\n%+v\n%+v", run, batched.Stats(), seq.Stats())
			}
			ba, _ := batched.ReadFrom(nil, Ref{}, 0)
			sa, _ := seq.ReadFrom(nil, Ref{}, 0)
			if !slices.Equal(ba, sa) || bu.Len() != su.Len() || bm.Keys() != sm.Keys() {
				t.Fatalf("run %d: contents differ", run)
			}
			mustInvariants(t, batched)
		}
	}
}

// T-011 (batch): intra-batch duplicates under each policy.
func TestAppendBatch_IntraBatchConflicts(t *testing.T) {
	t.Run("Reject", func(t *testing.T) {
		ix := NewUniqueIndex("id", byID, ConflictReject)
		s := mustNew(t, Config[trade]{Capacity: 8, Indexes: []Index[trade]{ix}})
		mustAppend(t, s, trade{ID: 9}, at(1))
		before := s.Stats()
		dst := []AppendResult{{Index: "keep"}}
		got, err := s.AppendBatch(dst, tradeInputs([3]int{1, 0, 2}, [3]int{2, 0, 3}, [3]int{1, 0, 4}))
		var ce *ConflictError
		if !errors.As(err, &ce) || ce.BatchPos != 2 || ce.Existing != (Ref{Gen: 1, Seq: 2}) || ce.Key != "1" {
			t.Fatalf("error = %#v", err)
		}
		if len(got) != 1 || got[0].Index != "keep" {
			t.Fatalf("dst changed on error: %+v", got)
		}
		after := s.Stats()
		before.Rejected++
		before.Indexes[0].Conflicts++
		if !statsEqual(before, after) {
			t.Fatalf("state changed: %+v vs %+v", after, before)
		}
		if ix.Contains(1) || ix.Contains(2) {
			t.Fatal("pending batch keys leaked into the index")
		}
		// The overlay is gone: the same keys are free in the next batch.
		if _, err := s.AppendBatch(nil, tradeInputs([3]int{1, 0, 5}, [3]int{2, 0, 6})); err != nil {
			t.Fatal(err)
		}
		mustInvariants(t, s)
	})

	t.Run("Drop", func(t *testing.T) {
		ix := NewUniqueIndex("id", byID, ConflictDrop)
		s := mustNew(t, Config[trade]{Capacity: 8, Indexes: []Index[trade]{ix}})
		got, err := s.AppendBatch(nil, tradeInputs([3]int{1, 0, 1}, [3]int{1, 0, 2}, [3]int{2, 0, 3}, [3]int{1, 0, 4}))
		if err != nil {
			t.Fatal(err)
		}
		want := []AppendResult{
			{Ref: Ref{1, 1}},
			{Outcome: OutcomeDropped, Existing: Ref{1, 1}, Index: "id"},
			{Ref: Ref{1, 2}}, // the drop consumed no Seq
			{Outcome: OutcomeDropped, Existing: Ref{1, 1}, Index: "id"},
		}
		if !slices.Equal(got, want) {
			t.Fatalf("results %+v\nwant %+v", got, want)
		}
		if st := s.Stats(); st.Dropped != 2 || st.Indexes[0].Dropped != 2 || st.Len != 2 {
			t.Fatalf("stats %+v", st)
		}
		mustInvariants(t, s)
	})

	t.Run("Replace chain", func(t *testing.T) {
		ix := NewUniqueIndex("id", byID, ConflictReplace)
		s := mustNew(t, Config[trade]{Capacity: 8, Indexes: []Index[trade]{ix}})
		mustAppend(t, s, trade{ID: 5}, at(1))
		got, err := s.AppendBatch(nil, tradeInputs([3]int{5, 0, 2}, [3]int{5, 0, 3}, [3]int{5, 0, 4}))
		if err != nil {
			t.Fatal(err)
		}
		for i, res := range got {
			if res.Existing != (Ref{Gen: 1, Seq: Seq(i + 1)}) || res.Ref.Seq != Seq(i+2) {
				t.Fatalf("item %d: %+v", i, res)
			}
		}
		if e, _ := ix.Get(5); e.Meta.Ref.Seq != 4 {
			t.Fatalf("final holder %+v", e.Meta.Ref)
		}
		if st := s.Stats(); st.Indexes[0].Replaced != 3 || st.Indexes[0].Keys != 1 {
			t.Fatalf("stats %+v", st)
		}
		mustInvariants(t, s)
	})

	t.Run("KeepFirst", func(t *testing.T) {
		ix := NewUniqueIndex("id", byID, ConflictKeepFirst)
		s := mustNew(t, Config[trade]{Capacity: 8, Indexes: []Index[trade]{ix}})
		got, err := s.AppendBatch(nil, tradeInputs([3]int{5, 0, 2}, [3]int{5, 0, 3}))
		if err != nil {
			t.Fatal(err)
		}
		if got[1].Existing != got[0].Ref || got[1].Outcome != OutcomeAppended {
			t.Fatalf("results %+v", got)
		}
		if e, _ := ix.Get(5); e.Meta.Ref != got[0].Ref {
			t.Fatal("KeepFirst moved the mapping")
		}
		mustInvariants(t, s)
	})

	t.Run("multiple indexes, drop wins inside a batch", func(t *testing.T) {
		ids := NewUniqueIndex("id", byID, ConflictReplace)
		syms := NewUniqueIndex("sym", func(e *Entry[trade]) (string, bool) { return e.Value.Symbol, true }, ConflictDrop)
		s := mustNew(t, Config[trade]{Capacity: 8, Indexes: []Index[trade]{ids, syms}})
		got, err := s.AppendBatch(nil, tradeInputs([3]int{1, 7, 1}, [3]int{1, 7, 2}, [3]int{1, 8, 3}))
		if err != nil {
			t.Fatal(err)
		}
		if got[1].Outcome != OutcomeDropped || got[1].Index != "sym" || got[2].Existing != got[0].Ref || got[2].Index != "id" {
			t.Fatalf("results %+v", got)
		}
		if e, _ := ids.Get(1); e.Meta.Ref != got[2].Ref {
			t.Fatal("the dropped item staged a Replace")
		}
		mustInvariants(t, s)
	})
}

// T-012 (batch), E-012, P-02: a Capacity-sized batch on a full ring may reuse
// keys of rows evicted earlier in the same batch.
func TestAppendBatch_KeysOfRowsEvictedInBatch(t *testing.T) {
	for _, capacity := range []int{1, 2, 5} {
		ix := NewUniqueIndex("id", byID, ConflictReject)
		s := mustNew(t, Config[trade]{Capacity: capacity, Indexes: []Index[trade]{ix}})
		var fill []Input[trade]
		for i := 1; i <= capacity; i++ {
			fill = append(fill, Input[trade]{Value: trade{ID: i}, Meta: at(Timestamp(i))})
		}
		if _, err := s.AppendBatch(nil, fill); err != nil {
			t.Fatal(err)
		}
		// Same keys again: item i evicts the row with ID i+1 before item i+1 needs it.
		got, err := s.AppendBatch(nil, fill)
		if err != nil {
			t.Fatalf("capacity %d: %v", capacity, err)
		}
		for i, res := range got {
			if res.Evicted.Seq != Seq(i+1) {
				t.Fatalf("capacity %d item %d: %+v", capacity, i, res)
			}
		}
		if capacity > 1 {
			// Reversed order conflicts: item 0 wants the last row's key, which is still retained.
			rev := slices.Clone(fill)
			slices.Reverse(rev)
			if _, err := s.AppendBatch(nil, rev); !errors.Is(err, ErrUniqueConflict) {
				t.Fatalf("capacity %d: reversed batch: %v", capacity, err)
			}
		}
		mustInvariants(t, s)
	}
}

// S12-01 (replaces T-014 batch), E-007: a panic at item k propagates with no
// state change; the lock, the CloneOnWrite scratch and the pending-key overlay
// are all released.
func TestAppendBatch_PanicNoStateChange(t *testing.T) {
	boom := func(e *Entry[trade]) (int, bool) {
		if e.Value.ID < 0 {
			panic("extractor")
		}
		return e.Value.ID, true
	}
	clone := func(v trade) trade {
		if v.Symbol == "boom" {
			panic("clone")
		}
		return v
	}
	for _, kind := range []string{"extractor", "clone"} {
		ix := NewUniqueIndex("id", boom, ConflictReplace)
		s := mustNew(t, Config[trade]{
			Capacity: 4, TimeIndex: TimeIndexBTree, Clone: clone, CloneMode: CloneOnWrite,
			Indexes: []Index[trade]{ix},
		})
		mustAppend(t, s, trade{ID: 1}, at(1))
		before := s.Stats()
		items := tradeInputs([3]int{2, 0, 2}, [3]int{3, 0, 3}, [3]int{4, 0, 4})
		if kind == "extractor" {
			items[2].Value.ID = -1
		} else {
			items[2].Value.Symbol = "boom"
		}
		if v := panicValue(func() { s.AppendBatch(nil, items) }); v != kind {
			t.Fatalf("%s: panic value = %v", kind, v)
		}
		if after := s.Stats(); !statsEqual(before, after) {
			t.Fatalf("%s: state changed: %+v vs %+v", kind, after, before)
		}
		for i := range s.cloned {
			if s.cloned[i] != (trade{}) {
				t.Fatalf("%s: clone scratch not cleared", kind)
			}
		}
		mustInvariants(t, s)
		if _, err := s.AppendBatch(nil, tradeInputs([3]int{2, 0, 5})); err != nil { // the lock was released
			t.Fatalf("%s: next batch: %v", kind, err)
		}
	}
}

// S12-01: a batch that panics after an earlier item claimed key K must not
// leave K in the pending overlay; a later single Append of K succeeds even
// under ConflictReject.
func TestAppendBatch_PanicEndsPendingOverlay(t *testing.T) {
	ix := NewUniqueIndex("id", func(e *Entry[trade]) (int, bool) {
		if e.Value.ID < 0 {
			panic("boom")
		}
		return e.Value.ID, true
	}, ConflictReject)
	s := mustNew(t, Config[trade]{Capacity: 8, Indexes: []Index[trade]{ix}})
	items := tradeInputs([3]int{5, 0, 1}, [3]int{-1, 0, 2})
	if v := panicValue(func() { s.AppendBatch(nil, items) }); v != "boom" {
		t.Fatalf("panic value = %v", v)
	}
	if ix.batch || len(ix.pending) != 0 {
		t.Fatal("pending overlay still active after the panic")
	}
	if _, err := s.Append(trade{ID: 5}, at(3)); err != nil {
		t.Fatalf("key 5 should be free: %v", err)
	}
	mustInvariants(t, s)
}

// W-003, P-09, W-008: size limits, empty batches and closed series.
func TestAppendBatch_Limits(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 3})
	dst := []AppendResult{{Index: "x"}}
	got, err := s.AppendBatch(dst, nil)
	if err != nil || len(got) != 1 || s.Stats().Rejected != 0 {
		t.Fatalf("empty batch: %v %+v", err, got)
	}
	four := []Input[int]{{1, at(1)}, {2, at(2)}, {3, at(3)}, {4, at(4)}}
	if _, err := s.AppendBatch(nil, four); !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("oversized batch: %v", err)
	}
	if st := s.Stats(); st.Len != 0 || st.Rejected != 1 {
		t.Fatalf("stats %+v", st)
	}
	got, err = s.AppendBatch(nil, four[:3])
	if err != nil || len(got) != 3 || got[2].Ref.Seq != 3 {
		t.Fatalf("capacity-sized batch: %v %+v", err, got)
	}
	s.Close()
	if _, err := s.AppendBatch(nil, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("empty batch on a closed series: %v (closed check comes first)", err)
	}
	if _, err := s.AppendBatch(nil, four[:1]); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed: %v", err)
	}
}

// §5.3, §9.2, P-02: policies see earlier items of the batch; a mid-batch
// rejection fails the whole batch.
func TestAppendBatch_SequentialPolicies(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 8, SourceSeqPolicy: SourceSeqFlag})
	got, err := s.AppendBatch(nil, []Input[int]{
		{1, InputMeta{EventTime: 100, SourceSeq: 5}},
		{2, InputMeta{EventTime: 50, SourceSeq: 5}},
		{3, InputMeta{EventTime: 200, SourceSeq: 1, SourceEpoch: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Flags != 0 || got[1].Flags != FlagLate|FlagSourceSeqNotIncreasing || got[2].Flags != 0 {
		t.Fatalf("flags %+v", got)
	}

	r := mustNew(t, Config[int]{Capacity: 8, LatePolicy: LateReject})
	mustAppend(t, r, 0, at(10))
	before := r.Stats()
	_, err = r.AppendBatch(nil, []Input[int]{{1, at(20)}, {2, at(30)}, {3, at(25)}})
	if !errors.Is(err, ErrLate) || !strings.Contains(err.Error(), "batch item 2") {
		t.Fatalf("error = %v", err)
	}
	after := r.Stats()
	before.Rejected++
	if !statsEqual(before, after) {
		t.Fatalf("state changed: %+v vs %+v", after, before)
	}
	_, err = r.AppendBatch(nil, []Input[int]{{1, at(20)}, {2, at(MaxTimestamp)}})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("error = %v", err)
	}
}

// W-010, §9.4: exactly one time-index call per batch, none for a failed or
// all-dropped batch.
func TestAppendBatch_OneApply(t *testing.T) {
	ix := NewUniqueIndex("id", byID, ConflictDrop)
	s := mustNew(t, Config[trade]{Capacity: 4, TimeIndex: TimeIndexBTree, LatePolicy: LateReject, Indexes: []Index[trade]{ix}})
	c := &countingIndex{timeIndex: s.time}
	s.time = c
	if _, err := s.AppendBatch(nil, tradeInputs([3]int{1, 0, 1}, [3]int{2, 0, 2}, [3]int{3, 0, 3}, [3]int{4, 0, 4})); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendBatch(nil, tradeInputs([3]int{5, 0, 5}, [3]int{6, 0, 6}, [3]int{7, 0, 7})); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendBatch(nil, tradeInputs([3]int{8, 0, 8}, [3]int{9, 0, 1})); err == nil {
		t.Fatal("expected ErrLate")
	}
	if c.applies != 2 || c.sets != 0 || c.deletes != 0 {
		t.Fatalf("applies %d sets %d deletes %d", c.applies, c.sets, c.deletes)
	}
	mustInvariants(t, s)
}

// T-026, W-009: AppendBatch with TimeIndexNone and no indexes does not
// allocate in steady state.
func TestAllocs_AppendBatch(t *testing.T) {
	skipAllocTest(t)
	for _, mode := range allModes {
		s := mustNew(t, Config[[3]float64]{Capacity: 512, TimeIndex: mode})
		items := make([]Input[[3]float64], 64)
		dst := make([]AppendResult, 0, 64)
		ts := Timestamp(0)
		step := func() {
			for i := range items {
				ts++
				items[i] = Input[[3]float64]{Value: [3]float64{1, 2, 3}, Meta: at(ts - Timestamp(i%4))}
			}
			var err error
			if dst, err = s.AppendBatch(dst[:0], items); err != nil {
				t.Fatal(err)
			}
		}
		for range 100 {
			step()
		}
		n := allocsPerOp(200, step)
		t.Logf("mode %d: AppendBatch(64) %.4f allocs/op", mode, n)
		if mode == TimeIndexNone && n != 0 {
			t.Errorf("TimeIndexNone AppendBatch: %.4f allocs/op, want 0", n)
		}
	}
}
