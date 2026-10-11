package temporal

import (
	"errors"
	"fmt"
	"slices"
	"testing"
)

// trade is a typical deduplicated market-data record.
type trade struct {
	ID     int
	Symbol string
	Px     float64
}

func byID(e *Entry[trade]) (int, bool)        { return e.Value.ID, e.Value.ID != 0 }
func bySymbol(e *Entry[trade]) (string, bool) { return e.Value.Symbol, e.Value.Symbol != "" }

func appendTrade(t *testing.T, s *Series[trade], id int, sym string, ts Timestamp) (AppendResult, error) {
	t.Helper()
	return s.Append(trade{ID: id, Symbol: sym, Px: float64(id)}, at(ts))
}

// T-011, K-007, §10.3, E-011: each unique ConflictPolicy on a single Append.
func TestUniqueIndex_Policies(t *testing.T) {
	t.Run("Reject", func(t *testing.T) {
		ix := NewUniqueIndex("id", byID, ConflictReject)
		s := mustNew(t, Config[trade]{Capacity: 8, Indexes: []Index[trade]{ix}})
		first, _ := appendTrade(t, s, 42, "BTC", 1)
		before := s.Stats()
		_, err := appendTrade(t, s, 42, "ETH", 2)
		var ce *ConflictError
		if !errors.Is(err, ErrUniqueConflict) || !errors.As(err, &ce) ||
			ce.Index != "id" || ce.Key != "42" || ce.Existing != first.Ref || ce.BatchPos != 0 {
			t.Fatalf("error = %#v", err)
		}
		after := s.Stats()
		before.Rejected++
		before.Indexes[0].Conflicts++ // P-04: diagnostic counters only
		if !statsEqual(before, after) {
			t.Fatalf("state changed: %+v vs %+v", after, before)
		}
		if e, st := ix.Get(42); st != StatusOK || e.Value.Symbol != "BTC" {
			t.Fatalf("Get(42) = %+v, %v", e, st)
		}
		mustInvariants(t, s)
	})

	t.Run("Drop", func(t *testing.T) {
		ix := NewUniqueIndex("id", byID, ConflictDrop)
		s := mustNew(t, Config[trade]{Capacity: 8, TimeIndex: TimeIndexBTree, Indexes: []Index[trade]{ix}})
		first, _ := appendTrade(t, s, 42, "BTC", 10)
		res, err := appendTrade(t, s, 42, "ETH", 5) // late, too: flags still reported
		if err != nil {
			t.Fatal(err)
		}
		want := AppendResult{Outcome: OutcomeDropped, Flags: FlagLate, Existing: first.Ref, Index: "id"}
		if res != want {
			t.Fatalf("result = %+v, want %+v", res, want)
		}
		st := s.Stats()
		if st.Len != 1 || st.Dropped != 1 || st.Appended != 1 || st.Late != 0 || st.Indexes[0].Dropped != 1 {
			t.Fatalf("stats = %+v", st)
		}
		if st.MaxObservedEventTime != 10 {
			t.Fatal("a dropped item updated MaxObservedEventTime")
		}
		next, _ := appendTrade(t, s, 7, "BTC", 11)
		if next.Ref.Seq != 2 {
			t.Fatalf("drop consumed a Seq: next Seq %d", next.Ref.Seq)
		}
		mustInvariants(t, s)
	})

	t.Run("Replace", func(t *testing.T) {
		ix := NewUniqueIndex("id", byID, ConflictReplace)
		s := mustNew(t, Config[trade]{Capacity: 8, Indexes: []Index[trade]{ix}})
		first, _ := appendTrade(t, s, 42, "BTC", 1)
		res, _ := appendTrade(t, s, 42, "ETH", 2)
		if res.Outcome != OutcomeAppended || res.Existing != first.Ref || res.Index != "id" {
			t.Fatalf("result = %+v", res)
		}
		if e, _ := ix.Get(42); e.Meta.Ref != res.Ref {
			t.Fatalf("Get(42) = %+v, want the replacement", e)
		}
		if _, st := s.Get(first.Ref); st != StatusOK {
			t.Fatal("the replaced row must stay retained")
		}
		if st := s.Stats(); st.Indexes[0].Replaced != 1 || st.Indexes[0].Keys != 1 || st.Len != 2 {
			t.Fatalf("stats = %+v", st)
		}
		mustInvariants(t, s)
	})

	t.Run("KeepFirst", func(t *testing.T) {
		ix := NewUniqueIndex("id", byID, ConflictKeepFirst)
		s := mustNew(t, Config[trade]{Capacity: 8, TimeIndex: TimeIndexBTree, Indexes: []Index[trade]{ix}})
		first, _ := appendTrade(t, s, 42, "BTC", 1)
		res, _ := appendTrade(t, s, 42, "ETH", 2)
		if res.Outcome != OutcomeAppended || res.Existing != first.Ref || res.Index != "id" {
			t.Fatalf("result = %+v", res)
		}
		if e, _ := ix.Get(42); e.Meta.Ref != first.Ref {
			t.Fatalf("Get(42) = %+v, want the first row", e)
		}
		if e, st := s.AsOf(2); st != StatusOK || e.Meta.Ref != res.Ref {
			t.Fatal("the KeepFirst row must be stored and time-indexed")
		}
		if st := s.Stats(); st.Indexes[0].KeptFirst != 1 {
			t.Fatalf("stats = %+v", st)
		}
		mustInvariants(t, s)
	})

	t.Run("not indexed", func(t *testing.T) {
		ix := NewUniqueIndex("id", byID, ConflictReject)
		s := mustNew(t, Config[trade]{Capacity: 8, Indexes: []Index[trade]{ix}})
		for range 3 {
			if _, err := appendTrade(t, s, 0, "BTC", 1); err != nil { // ID 0: ok=false
				t.Fatal(err)
			}
		}
		if ix.Len() != 0 || s.Len() != 3 {
			t.Fatalf("Len = %d, series %d", ix.Len(), s.Len())
		}
	})
}

// T-011, §10.3: several unique indexes are evaluated in Config order; any
// Reject fails the item, otherwise Drop wins over Replace and KeepFirst, and a
// dropped item modifies no index.
func TestUniqueIndex_MultipleIndexes(t *testing.T) {
	bySym := func(e *Entry[trade]) (string, bool) { return e.Value.Symbol, true }
	cases := []struct {
		name     string
		policies [2]ConflictPolicy
		outcome  Outcome
		err      error
		index    string
	}{
		{"replace then drop", [2]ConflictPolicy{ConflictReplace, ConflictDrop}, OutcomeDropped, nil, "sym"},
		{"drop then keepFirst", [2]ConflictPolicy{ConflictDrop, ConflictKeepFirst}, OutcomeDropped, nil, "id"},
		{"drop then reject", [2]ConflictPolicy{ConflictDrop, ConflictReject}, 0, ErrUniqueConflict, ""},
		{"keepFirst then replace", [2]ConflictPolicy{ConflictKeepFirst, ConflictReplace}, OutcomeAppended, nil, "id"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ids := NewUniqueIndex("id", byID, c.policies[0])
			syms := NewUniqueIndex("sym", bySym, c.policies[1])
			s := mustNew(t, Config[trade]{Capacity: 8, Indexes: []Index[trade]{ids, syms}})
			first, _ := appendTrade(t, s, 1, "BTC", 1)
			before := s.Stats()
			res, err := appendTrade(t, s, 1, "BTC", 2) // conflicts on both
			if !errors.Is(err, c.err) && !(c.err == nil && err == nil) {
				t.Fatalf("error = %v, want %v", err, c.err)
			}
			if err != nil {
				return
			}
			if res.Outcome != c.outcome || res.Index != c.index || res.Existing != first.Ref {
				t.Fatalf("result = %+v", res)
			}
			if c.outcome == OutcomeDropped {
				after := s.Stats()
				if after.Len != before.Len || after.Indexes[0].Keys != 1 || after.Indexes[1].Keys != 1 ||
					after.Indexes[0].Replaced != 0 || after.Indexes[1].Replaced != 0 {
					t.Fatalf("a dropped item modified an index: %+v", after)
				}
				if e, _ := ids.Get(1); e.Meta.Ref != first.Ref {
					t.Fatal("dropped item changed the id mapping")
				}
			}
			mustInvariants(t, s)
		})
	}
}

// T-012, E-012: a key equal to that of the row evicted by the same Append is
// not a conflict, at capacity 1 and on a full ring.
func TestUniqueIndex_KeyOfEvictedRow(t *testing.T) {
	for _, capacity := range []int{1, 3} {
		ix := NewUniqueIndex("id", byID, ConflictReject)
		s := mustNew(t, Config[trade]{Capacity: capacity, Indexes: []Index[trade]{ix}})
		for i := 1; i <= capacity; i++ {
			if _, err := appendTrade(t, s, i, "X", Timestamp(i)); err != nil {
				t.Fatal(err)
			}
		}
		// The ring is full; the next append evicts the row with ID 1.
		res, err := appendTrade(t, s, 1, "X", 100)
		if err != nil {
			t.Fatalf("capacity %d: %v", capacity, err)
		}
		if e, st := ix.Get(1); st != StatusOK || e.Meta.Ref != res.Ref {
			t.Fatalf("capacity %d: Get(1) = %+v, %v", capacity, e, st)
		}
		if capacity > 1 {
			if _, err := appendTrade(t, s, 3, "X", 101); !errors.Is(err, ErrUniqueConflict) {
				t.Fatalf("capacity %d: ID 3 is retained and not evicted by this append: %v", capacity, err)
			}
		}
		mustInvariants(t, s)
	}
}

// T-013, K-008: evicting an older row never removes a newer mapping.
func TestUniqueIndex_EvictionKeepsNewerMapping(t *testing.T) {
	for _, policy := range []ConflictPolicy{ConflictReplace, ConflictKeepFirst} {
		ix := NewUniqueIndex("id", byID, policy)
		s := mustNew(t, Config[trade]{Capacity: 3, Indexes: []Index[trade]{ix}})
		a, _ := appendTrade(t, s, 7, "A", 1)
		b, _ := appendTrade(t, s, 7, "B", 2)
		appendTrade(t, s, 8, "C", 3)
		appendTrade(t, s, 9, "D", 4) // evicts a
		e, st := ix.Get(7)
		switch policy {
		case ConflictReplace:
			if st != StatusOK || e.Meta.Ref != b.Ref {
				t.Fatalf("Replace: Get(7) = %+v, %v; want %+v", e, st, b.Ref)
			}
		case ConflictKeepFirst:
			if st != StatusNotFound {
				t.Fatalf("KeepFirst: Get(7) = %+v, %v; the holder %+v was evicted", e, st, a.Ref)
			}
		}
		appendTrade(t, s, 10, "E", 5) // evicts b
		if _, st := ix.Get(7); st != StatusNotFound {
			t.Fatalf("policy %d: key 7 still mapped after both rows were evicted", policy)
		}
		mustInvariants(t, s)
	}
}

// T-014, E-007, W-004: an extractor panic is recovered with no state change.
func TestIndex_ExtractorPanicNoStateChange(t *testing.T) {
	boom := func(e *Entry[trade]) (int, bool) {
		if e.Value.ID < 0 {
			panic("bad id")
		}
		return e.Value.ID, true
	}
	ids := NewUniqueIndex("id", byID, ConflictReplace)
	syms := NewMultiIndex("sym", bySymbol)
	risky := NewUniqueIndex("risky", boom, ConflictReject)
	s := mustNew(t, Config[trade]{Capacity: 2, TimeIndex: TimeIndexBTree, Indexes: []Index[trade]{ids, syms, risky}})
	appendTrade(t, s, 1, "BTC", 1)
	appendTrade(t, s, 2, "BTC", 2)
	before := s.Stats()

	_, err := s.Append(trade{ID: -1, Symbol: "BTC"}, at(3)) // would evict ID 1
	var pe *CallbackPanicError
	if !errors.Is(err, ErrCallbackPanic) || !errors.As(err, &pe) || pe.Callback != "extractor:risky" || pe.Value != "bad id" {
		t.Fatalf("error = %#v", err)
	}
	after := s.Stats()
	before.Rejected++
	if !statsEqual(before, after) {
		t.Fatalf("state changed: %+v vs %+v", after, before)
	}
	if e, st := ids.Get(1); st != StatusOK || e.Value.ID != 1 {
		t.Fatal("eviction happened despite the failure")
	}
	if syms.Count("BTC") != 2 {
		t.Fatal("multi index changed")
	}
	if s.xe != (Entry[trade]{}) {
		t.Fatal("extractor scratch entry not cleared")
	}
	mustInvariants(t, s)
	if res, err := appendTrade(t, s, 3, "ETH", 3); err != nil || res.Ref.Seq != 3 {
		t.Fatalf("next append: %+v %v", res, err)
	}
}

// K-005: extractors see the tentative Ref, series flags and the OnWrite clone.
func TestIndex_ExtractorSeesPreparedEntry(t *testing.T) {
	var seen []Entry[[]int]
	ix := NewMultiIndex("k", func(e *Entry[[]int]) (int, bool) {
		seen = append(seen, Entry[[]int]{Meta: e.Meta, Value: e.Value})
		return e.Value[0], true
	})
	clone := func(v []int) []int { return append([]int{100}, v[1:]...) }
	s := mustNew(t, Config[[]int]{Capacity: 4, Clone: clone, CloneMode: CloneOnWrite, Indexes: []Index[[]int]{ix}})
	mustAppend(t, s, []int{1, 2}, InputMeta{EventTime: 10, Flags: FlagGap})
	mustAppend(t, s, []int{1, 3}, at(5)) // late
	if len(seen) != 2 || seen[0].Meta.Ref != (Ref{Gen: 1, Seq: 1}) || seen[1].Meta.Ref != (Ref{Gen: 1, Seq: 2}) {
		t.Fatalf("refs seen: %+v", seen)
	}
	if seen[0].Meta.Flags != FlagGap || seen[1].Meta.Flags != FlagLate || seen[0].Meta.EventTime != 10 {
		t.Fatalf("meta seen: %+v", seen)
	}
	if seen[0].Value[0] != 100 || ix.Count(100) != 2 || ix.Count(1) != 0 {
		t.Fatal("extractor did not see the OnWrite clone")
	}
}

// Q-010, K-009, §10.4: multi-index lookups in both orders, limits, eviction
// from the front and posting-list compaction.
func TestMultiIndex_Lookups(t *testing.T) {
	syms := NewMultiIndex("sym", bySymbol)
	s := mustNew(t, Config[trade]{Capacity: 6, MaxReadBatch: 3, Indexes: []Index[trade]{syms}})
	for i, sym := range []string{"A", "B", "A", "A", "C", "A"} {
		appendTrade(t, s, i+1, sym, Timestamp(i))
	}
	asc, info := syms.Get(nil, "A", 0, Ascending)
	if !slices.Equal(seqsOf(asc), []Seq{1, 3, 4}) || !info.Truncated || info.Status != StatusOK || info.LastRef.Seq != 4 {
		t.Fatalf("ascending = %v %+v", seqsOf(asc), info)
	}
	desc, info := syms.Get([]Entry[trade]{{}}, "A", 2, Descending)
	if !slices.Equal(seqsOf(desc[1:]), []Seq{6, 4}) || !info.Truncated || info.N != 2 {
		t.Fatalf("descending = %v %+v", seqsOf(desc), info)
	}
	if _, info := syms.Get(nil, "Z", 0, Ascending); info.Status != StatusNotFound {
		t.Fatalf("missing key: %+v", info)
	}
	if e, st := syms.Latest("A"); st != StatusOK || e.Meta.Ref.Seq != 6 {
		t.Fatalf("Latest = %+v %v", e, st)
	}
	if syms.Count("A") != 4 || syms.Keys() != 3 {
		t.Fatalf("Count %d Keys %d", syms.Count("A"), syms.Keys())
	}
	// Push "A" through many evictions so the posting list compacts repeatedly.
	for i := 7; i < 200; i++ {
		appendTrade(t, s, i, "A", Timestamp(i))
		mustInvariants(t, s)
	}
	if syms.Count("A") != 6 || syms.Keys() != 1 || syms.Count("B") != 0 {
		t.Fatalf("after churn: A %d keys %d", syms.Count("A"), syms.Keys())
	}
	if p := syms.m["A"]; len(p.seqs) > 13 {
		t.Fatalf("posting list not compacted: len %d head %d", len(p.seqs), p.head)
	}
	if err := s.Clear(); err != nil {
		t.Fatal(err)
	}
	if syms.Keys() != 0 || syms.Count("A") != 0 {
		t.Fatal("Clear did not reset the multi index")
	}
	mustInvariants(t, s)
}

// K-003: a handle binds to one Series; failure binds nothing.
func TestIndex_Binding(t *testing.T) {
	ix := NewUniqueIndex("id", byID, ConflictReject)
	if _, st := ix.Get(1); st != StatusInvalid || ix.Contains(1) || ix.Len() != 0 {
		t.Fatal("unbound handle should report StatusInvalid and be empty")
	}
	multi := NewMultiIndex("sym", bySymbol)
	if _, info := multi.Get(nil, "A", 0, Ascending); info.Status != StatusInvalid {
		t.Fatal("unbound multi handle should report StatusInvalid")
	}
	mustNew(t, Config[trade]{Capacity: 2, Indexes: []Index[trade]{ix}})

	fresh := NewMultiIndex("sym", bySymbol)
	_, err := New(Config[trade]{Capacity: 2, Indexes: []Index[trade]{fresh, ix}})
	if !errors.Is(err, ErrIndexBound) {
		t.Fatalf("reusing a handle: %v, want ErrIndexBound", err)
	}
	if fresh.isBound() {
		t.Fatal("a failed New bound some indexes")
	}
	if _, err := New(Config[trade]{Capacity: 2, Indexes: []Index[trade]{fresh}}); err != nil {
		t.Fatalf("fresh handle after a failed New: %v", err)
	}
	same := NewUniqueIndex("x", byID, ConflictReject)
	if _, err := New(Config[trade]{Capacity: 2, Indexes: []Index[trade]{same, same}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("same handle twice: %v, want ErrInvalidConfig", err)
	}
}

// K-011, Q-017, TI-010: key indexes behave identically in every mode.
func TestIndex_ModeIndependence(t *testing.T) {
	type outcome struct {
		res AppendResult
		err string
	}
	run := func(mode TimeIndexMode) ([]outcome, []Stats) {
		ids := NewUniqueIndex("id", byID, ConflictPolicy(0))
		drops := NewUniqueIndex("dedup", func(e *Entry[trade]) (int, bool) { return e.Value.ID % 5, true }, ConflictDrop)
		syms := NewMultiIndex("sym", bySymbol)
		s := mustNew(t, Config[trade]{Capacity: 4, TimeIndex: mode, Indexes: []Index[trade]{syms, drops, ids}})
		var outs []outcome
		var stats []Stats
		for i := range 60 {
			res, err := appendTrade(t, s, i%9, []string{"A", "B", ""}[i%3], Timestamp(i%7))
			outs = append(outs, outcome{res, fmt.Sprint(err)})
			st := s.Stats()
			st.TimeIndex, st.MinEventTime, st.MaxEventTime = 0, 0, 0
			stats = append(stats, st)
		}
		mustInvariants(t, s)
		return outs, stats
	}
	o1, s1 := run(TimeIndexNone)
	o2, s2 := run(TimeIndexBTree)
	if !slices.Equal(o1, o2) {
		t.Fatal("append outcomes differ between modes")
	}
	for i := range s1 {
		if !statsEqual(s1[i], s2[i]) {
			t.Fatalf("step %d stats differ:\n%+v\n%+v", i, s1[i], s2[i])
		}
	}
}

// T-026 (part), W-009: key lookups do not allocate; Append with key indexes
// is reported (per-key map growth is allowed).
func TestAllocs_KeyIndexes(t *testing.T) {
	skipAllocTest(t)
	ids := NewUniqueIndex("id", byID, ConflictDrop)
	syms := NewMultiIndex("sym", bySymbol)
	s := mustNew(t, Config[trade]{Capacity: 1024, Indexes: []Index[trade]{ids, syms}})
	id := 0
	appendOne := func() {
		id++
		if _, err := s.Append(trade{ID: id, Symbol: "BTC"}, at(Timestamp(id))); err != nil {
			t.Fatal(err)
		}
	}
	for range 5000 {
		appendOne()
	}
	dst := make([]Entry[trade], 0, 64)
	fns := map[string]func(){
		"UniqueIndex.Get":      func() { ids.Get(id - 10) },
		"UniqueIndex.Contains": func() { ids.Contains(id - 10) },
		"MultiIndex.Get":       func() { dst, _ = syms.Get(dst[:0], "BTC", 64, Descending) },
		"MultiIndex.Latest":    func() { syms.Latest("BTC") },
		"MultiIndex.Count":     func() { syms.Count("BTC") },
		"Append duplicate":     func() { s.Append(trade{ID: id, Symbol: "BTC"}, at(Timestamp(id))) },
	}
	for name, fn := range fns {
		if n := testing.AllocsPerRun(500, fn); n != 0 {
			t.Errorf("%s: %.2f allocs/op, want 0", name, n)
		}
	}
	t.Logf("Append with unique+multi index, steady state: %.3f allocs/op", testing.AllocsPerRun(5000, appendOne))
}
