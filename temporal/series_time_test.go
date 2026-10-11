package temporal

import "testing"

// countingIndex wraps a timeIndex and counts calls, to check that the write
// path dispatches at most once per transaction (W-010, TI-007).
type countingIndex struct {
	timeIndex
	applies, sets, deletes int
}

func (c *countingIndex) Apply(del, ins []timeKey) { c.applies++; c.timeIndex.Apply(del, ins) }
func (c *countingIndex) Set(k timeKey)            { c.sets++; c.timeIndex.Set(k) }
func (c *countingIndex) Delete(k timeKey)         { c.deletes++; c.timeIndex.Delete(k) }

// W-010: one Apply per committed Append, none for a failed one.
func TestAppend_OneTimeIndexCallPerTransaction(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 2, TimeIndex: TimeIndexBTree, LatePolicy: LateReject})
	c := &countingIndex{timeIndex: s.time}
	s.time = c
	for i := range 5 {
		mustAppend(t, s, i, at(Timestamp(10+i)))
	}
	if _, err := s.Append(9, at(1)); err == nil { // late, rejected
		t.Fatal("expected ErrLate")
	}
	if c.applies != 5 || c.sets != 0 || c.deletes != 0 {
		t.Fatalf("applies %d sets %d deletes %d, want 5 0 0", c.applies, c.sets, c.deletes)
	}
	mustInvariants(t, s)
}

// TI-008, W-010: TimeIndexNone keeps a nil index and no key scratch.
func TestTimeIndexNone_NoScratch(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 4})
	if s.time != nil || s.keyScratch != nil || s.delKeys != nil || s.insKeys != nil {
		t.Fatal("TimeIndexNone allocated time-index state")
	}
	b := mustNew(t, Config[int]{Capacity: 4, TimeIndex: TimeIndexBTree, MaxReadBatch: 7})
	if b.time == nil || cap(b.keyScratch) != 8 {
		t.Fatalf("BTree mode: time %v, keyScratch cap %d", b.time, cap(b.keyScratch))
	}
	if b.TimeIndexMode() != TimeIndexBTree || b.Stats().TimeIndex != TimeIndexBTree {
		t.Fatal("TimeIndexMode not reported")
	}
}

// TI-002: Clear keeps the mode and empties the index.
func TestClear_KeepsTimeIndexMode(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 3, TimeIndex: TimeIndexBTree})
	for i := range 5 {
		mustAppend(t, s, i, at(Timestamp(5-i)))
	}
	if err := s.Clear(); err != nil {
		t.Fatal(err)
	}
	if s.time == nil || s.time.Len() != 0 || s.TimeIndexMode() != TimeIndexBTree {
		t.Fatal("Clear changed the time index mode or left keys")
	}
	mustAppend(t, s, 1, at(1))
	mustInvariants(t, s)
}

// W-009, TI-012: TimeIndexNone Append stays at zero allocations; BTree Append
// is reported (amortized node growth is allowed).
func TestAllocs_AppendByMode(t *testing.T) {
	skipAllocTest(t)
	for _, mode := range allModes {
		s := mustNew(t, Config[int]{Capacity: 1024, TimeIndex: mode})
		ts := Timestamp(0)
		appendOne := func() {
			ts++
			if _, err := s.Append(1, at(ts-Timestamp(ts%7))); err != nil {
				t.Fatal(err)
			}
		}
		for range 4096 {
			appendOne()
		}
		n := testing.AllocsPerRun(10000, appendOne)
		t.Logf("mode %d: Append steady state %.4f allocs/op", mode, n)
		if mode == TimeIndexNone && n != 0 {
			t.Errorf("TimeIndexNone Append: %.4f allocs/op, want 0", n)
		}
	}
}
