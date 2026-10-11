package temporal

import (
	"slices"
	"testing"
	"time"
)

func seqsOf[T any](es []Entry[T]) []Seq {
	out := []Seq{}
	for _, e := range es {
		out = append(out, e.Meta.Ref.Seq)
	}
	return out
}

func timesOf[T any](es []Entry[T]) []Timestamp {
	out := []Timestamp{}
	for _, e := range es {
		out = append(out, e.Meta.EventTime)
	}
	return out
}

// newTimeSeries returns a BTree series holding the given event times in order.
func newTimeSeries(t *testing.T, capacity, maxBatch int, times ...Timestamp) *Series[int] {
	t.Helper()
	s := mustNew(t, Config[int]{Capacity: capacity, TimeIndex: TimeIndexBTree, MaxReadBatch: maxBatch})
	appendTimes(t, s, times...)
	return s
}

// Q-005, Q-013, Q-014, T-024: Range pages concatenate to the full result.
func TestRange_PaginationConcatenates(t *testing.T) {
	times := []Timestamp{50, 10, 30, 10, 20, 40, 30, 30, 60, 5}
	s := newTimeSeries(t, 32, 0, times...)
	full, info := s.Range(nil, 10, 60, 100)
	if info.Status != StatusOK || info.Truncated || !info.Next.IsZero() {
		t.Fatalf("full read info = %+v", info)
	}
	if want := []Timestamp{10, 10, 20, 30, 30, 30, 40, 50}; !slices.Equal(timesOf(full), want) {
		t.Fatalf("full = %v, want %v", timesOf(full), want)
	}
	if want := []Seq{2, 4, 5, 3, 7, 8, 6, 1}; !slices.Equal(seqsOf(full), want) {
		t.Fatalf("full seqs = %v, want %v (ties by Seq)", seqsOf(full), want)
	}

	for limit := 1; limit <= 9; limit++ {
		var pages []Entry[int]
		page, info := s.Range(nil, 10, 60, limit)
		pages = append(pages, page...)
		for info.Truncated {
			if info.Next.IsZero() || info.N != limit || info.LastRef != page[len(page)-1].Meta.Ref {
				t.Fatalf("limit %d: truncated page info %+v", limit, info)
			}
			page, info = s.Continue(page[:0], info.Next, limit)
			pages = append(pages, page...)
		}
		if !info.Next.IsZero() || info.Status != StatusOK {
			t.Fatalf("limit %d: last page info %+v", limit, info)
		}
		if !slices.Equal(pages, full) {
			t.Fatalf("limit %d: pages %v, want %v", limit, seqsOf(pages), seqsOf(full))
		}

		var vals []int
		vpage, vinfo := s.RangeValues(nil, 10, 60, limit)
		vals = append(vals, vpage...)
		for vinfo.Truncated {
			vpage, vinfo = s.ContinueValues(vpage[:0], vinfo.Next, limit)
			vals = append(vals, vpage...)
		}
		want := []int{}
		for _, e := range full {
			want = append(want, e.Value)
		}
		if !slices.Equal(vals, want) {
			t.Fatalf("limit %d: values %v, want %v", limit, vals, want)
		}
	}
}

// T-024, §8.4: late inserts behind the cursor are not returned, late inserts
// ahead of it are; eviction between pages sets Evicted.
func TestContinue_ConcurrentChanges(t *testing.T) {
	t.Run("late inserts", func(t *testing.T) {
		s := newTimeSeries(t, 32, 0, 10, 20, 30, 40)
		page, info := s.Range(nil, 0, 100, 2) // 10, 20
		if !slices.Equal(timesOf(page), []Timestamp{10, 20}) {
			t.Fatalf("first page %v", timesOf(page))
		}
		appendTimes(t, s, 15, 35, 20) // behind, ahead, tie at the cursor time (larger Seq: ahead)
		page, info = s.Continue(nil, info.Next, 10)
		if want := []Timestamp{20, 30, 35, 40}; !slices.Equal(timesOf(page), want) || info.Truncated {
			t.Fatalf("continuation %v (info %+v), want %v", timesOf(page), info, want)
		}
		if page[0].Meta.Ref.Seq != 7 {
			t.Fatalf("tie at the cursor time should be the new record, got seq %d", page[0].Meta.Ref.Seq)
		}
	})

	t.Run("eviction between pages", func(t *testing.T) {
		s := newTimeSeries(t, 4, 0, 10, 20, 30, 40)
		page, info := s.Range(nil, 0, 100, 2)
		if info.Evicted {
			t.Fatal("Evicted before any eviction")
		}
		appendTimes(t, s, 90, 91, 92) // evicts 10, 20, 30
		page, info = s.Continue(page[:0], info.Next, 10)
		if !info.Evicted || !slices.Equal(timesOf(page), []Timestamp{40, 90, 91, 92}) {
			t.Fatalf("continuation %v info %+v", timesOf(page), info)
		}
	})

	t.Run("everything evicted", func(t *testing.T) {
		s := newTimeSeries(t, 2, 0, 10, 20)
		_, info := s.Range(nil, 0, 50, 1)
		appendTimes(t, s, 60, 70) // evicts 10 and 20; 60 and 70 are beyond the bound
		page, info := s.Continue(nil, info.Next, 10)
		if info.Status != StatusExpired || info.N != 0 || len(page) != 0 || !info.Evicted {
			t.Fatalf("info %+v", info)
		}
	})
}

// E-016, T-016, T-024: cursor validation and precedence.
func TestContinue_CursorValidation(t *testing.T) {
	s := newTimeSeries(t, 8, 0, 1, 2, 3, 4)
	other := newTimeSeries(t, 8, 0, 1, 2, 3, 4)
	_, info := s.Range(nil, 0, 10, 2)
	_, foreignInfo := other.Range(nil, 0, 10, 2)
	valid := info.Next

	cases := []struct {
		name string
		c    TimeCursor
		want Status
	}{
		{"zero", TimeCursor{}, StatusInvalid},
		{"foreign", foreignInfo.Next, StatusInvalid},
		{"future generation", TimeCursor{series: s.id, gen: s.gen + 1, state: valid.state}, StatusInvalid},
		{"valid", valid, StatusOK},
	}
	for _, c := range cases {
		page, info := s.Continue(nil, c.c, 0)
		if info.Status != c.want || (c.want != StatusOK && (len(page) != 0 || info.N != 0)) {
			t.Errorf("%s: info %+v, page %v", c.name, info, page)
		}
		if _, vinfo := s.ContinueValues(nil, c.c, 0); vinfo.Status != c.want {
			t.Errorf("%s: ContinueValues status %v", c.name, vinfo.Status)
		}
	}

	if err := s.Clear(); err != nil {
		t.Fatal(err)
	}
	appendTimes(t, s, 1, 2, 3)
	if _, info := s.Continue(nil, valid, 0); info.Status != StatusReset {
		t.Fatalf("old-generation cursor: %v, want Reset", info.Status)
	}
	if _, info := s.Continue(nil, TimeCursor{}, 0); info.Status != StatusInvalid {
		t.Fatalf("zero cursor after Clear: %v (Invalid precedes Reset)", info.Status)
	}

	none := mustNew(t, Config[int]{Capacity: 4})
	if _, info := none.Continue(nil, valid, 0); info.Status != StatusTimeIndexDisabled {
		t.Fatalf("None with a foreign cursor: %v, want TimeIndexDisabled", info.Status)
	}

	gated := mustNew(t, Config[int]{Capacity: 8, TimeIndex: TimeIndexBTree, GateReadsByWatermark: true})
	appendTimes(t, gated, 1, 2, 3, 4)
	gated.SetWatermark(10)
	_, ginfo := gated.Range(nil, 0, 10, 1)
	if ginfo.Next.IsZero() {
		t.Fatal("expected a cursor")
	}
	if err := gated.Clear(); err != nil {
		t.Fatal(err)
	}
	if _, info := gated.Continue(nil, ginfo.Next, 0); info.Status != StatusReset {
		t.Fatalf("Reset must precede NotReady, got %v", info.Status)
	}
}

// §19, P-10: Range and Continue are gated when to-1 > watermark.
func TestRange_WatermarkGate(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 8, TimeIndex: TimeIndexBTree, GateReadsByWatermark: true})
	appendTimes(t, s, 1, 2, 3, 4, 5)
	s.SetWatermark(3)
	cases := []struct {
		from, to Timestamp
		want     Status
	}{
		{0, 4, StatusOK},       // to-1 == wm
		{0, 5, StatusNotReady}, // to-1 == wm+1
		{0, MinTimestamp, StatusNotFound},
		{MinTimestamp, MinTimestamp + 1, StatusNotFound},
		{0, MaxTimestamp, StatusNotReady},
	}
	for _, c := range cases {
		if _, info := s.Range(nil, c.from, c.to, 0); info.Status != c.want {
			t.Errorf("Range(%d, %d) = %v, want %v", c.from, c.to, info.Status, c.want)
		}
	}
	_, info := s.Range(nil, 0, 4, 1)
	s2 := info.Next
	if _, info := s.Continue(nil, s2, 0); info.Status != StatusOK {
		t.Fatalf("Continue below the watermark: %v", info.Status)
	}
	s.SetWatermark(MaxTimestamp)
	if _, info := s.Range(nil, 0, MaxTimestamp, 0); info.Status != StatusOK || info.N != 5 {
		t.Fatalf("Range with watermark MaxTimestamp: %+v", info)
	}
}

// E-002, §8.1: Range with from >= to is empty NotFound; AtAll(MaxTimestamp) is
// NotFound; n <= 0 for PrevN/NextN is NotFound.
func TestBatch_EdgeRules(t *testing.T) {
	s := newTimeSeries(t, 8, 0, 1, 2, 3)
	for _, c := range [][2]Timestamp{{5, 5}, {5, 1}, {MaxTimestamp, MinTimestamp}} {
		got, info := s.Range([]Entry[int]{{Value: 9}}, c[0], c[1], 0)
		if info.Status != StatusNotFound || info.N != 0 || len(got) != 1 || info.Evicted {
			t.Errorf("Range(%d, %d) = %v %+v", c[0], c[1], got, info)
		}
	}
	if _, info := s.AtAll(nil, MaxTimestamp, 0); info.Status != StatusNotFound {
		t.Errorf("AtAll(MaxTimestamp) = %v", info.Status)
	}
	for _, n := range []int{0, -1} {
		if _, info := s.PrevN(nil, 10, n); info.Status != StatusNotFound || info.N != 0 {
			t.Errorf("PrevN n=%d: %+v", n, info)
		}
		if _, info := s.NextN(nil, 0, n); info.Status != StatusNotFound || info.N != 0 {
			t.Errorf("NextN n=%d: %+v", n, info)
		}
	}
	if got, info := s.AtAll(nil, MinTimestamp, 0); info.Status != StatusNotFound || len(got) != 0 {
		t.Errorf("AtAll(MinTimestamp) = %+v", info)
	}
}

// Q-002, Q-014: AtAll returns all records at ts in Seq order and continues.
func TestAtAll_DuplicatesAndContinue(t *testing.T) {
	s := newTimeSeries(t, 16, 0, 7, 5, 7, 7, 9, 7)
	page, info := s.AtAll(nil, 7, 2)
	if !slices.Equal(seqsOf(page), []Seq{1, 3}) || !info.Truncated || info.Next.IsZero() {
		t.Fatalf("first page %v %+v", seqsOf(page), info)
	}
	page, info = s.Continue(page, info.Next, 0)
	if !slices.Equal(seqsOf(page), []Seq{1, 3, 4, 6}) || info.Truncated || !info.Next.IsZero() {
		t.Fatalf("continued %v %+v", seqsOf(page), info)
	}
	// A record at ts+1 stays outside the cursor bound.
	if _, info := s.AtAll(nil, 8, 0); info.Status != StatusNotFound {
		t.Fatalf("AtAll(8) = %v", info.Status)
	}
}

// Q-006, §16.3: PrevN's last entry equals AsOf(ts); PrevN and NextN partition
// the timeline at ts.
func TestPrevNNextN_Partition(t *testing.T) {
	r, _ := newRand(t)
	for range iters(200) {
		s := mustNew(t, Config[int]{Capacity: 1 + r.IntN(30), TimeIndex: TimeIndexBTree})
		for range r.IntN(60) {
			mustAppend(t, s, 0, at(Timestamp(r.IntN(20))))
		}
		all, _ := s.Range(nil, MinTimestamp, MaxTimestamp, 0)
		for ts := Timestamp(-1); ts <= 21; ts++ {
			prev, pinfo := s.PrevN(nil, ts, 1000)
			next, ninfo := s.NextN(nil, ts, 1000)
			if pinfo.Truncated || ninfo.Truncated {
				t.Fatal("unexpected truncation")
			}
			if joined := append(slices.Clone(prev), next...); !slices.Equal(joined, all) {
				t.Fatalf("ts %d: PrevN ++ NextN = %v, Range = %v", ts, seqsOf(joined), seqsOf(all))
			}
			asOf, st := s.AsOf(ts)
			if (st == StatusOK) != (len(prev) > 0) || (st == StatusOK && prev[len(prev)-1] != asOf) {
				t.Fatalf("ts %d: PrevN last %v, AsOf %+v %v", ts, seqsOf(prev), asOf, st)
			}
			for _, e := range prev {
				if e.Meta.EventTime > ts {
					t.Fatalf("PrevN(%d) looked ahead: %d", ts, e.Meta.EventTime)
				}
			}
			for _, e := range next {
				if e.Meta.EventTime <= ts {
					t.Fatalf("NextN(%d) returned %d", ts, e.Meta.EventTime)
				}
			}
		}
	}
}

// Q-006: PrevN returns the n greatest keys in ascending order; NextN the n
// smallest; both truncate at n and at MaxReadBatch.
func TestPrevNNextN_Limits(t *testing.T) {
	s := newTimeSeries(t, 16, 3, 10, 20, 20, 30, 40)
	prev, info := s.PrevN(nil, 30, 2)
	if !slices.Equal(seqsOf(prev), []Seq{3, 4}) || !info.Truncated || info.LastRef.Seq != 4 {
		t.Fatalf("PrevN(30, 2) = %v %+v", seqsOf(prev), info)
	}
	prev, info = s.PrevN(nil, 30, 100) // capped at MaxReadBatch 3
	if !slices.Equal(seqsOf(prev), []Seq{2, 3, 4}) || !info.Truncated || !info.Next.IsZero() {
		t.Fatalf("PrevN(30, 100) = %v %+v", seqsOf(prev), info)
	}
	next, info := s.NextN(nil, 10, 2)
	if !slices.Equal(seqsOf(next), []Seq{2, 3}) || !info.Truncated {
		t.Fatalf("NextN(10, 2) = %v %+v", seqsOf(next), info)
	}
	next, info = s.NextN(nil, 20, 5)
	if !slices.Equal(seqsOf(next), []Seq{4, 5}) || info.Truncated || !info.Next.IsZero() {
		t.Fatalf("NextN(20, 5) = %v %+v", seqsOf(next), info)
	}
}

// P-13, S4-01: NextN on a gated series returns only records at or below the
// watermark.
func TestNextN_WatermarkGate(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 8, TimeIndex: TimeIndexBTree, GateReadsByWatermark: true})
	appendTimes(t, s, 10, 20, 30)
	if _, info := s.NextN(nil, 0, 5); info.Status != StatusNotReady {
		t.Fatalf("before any watermark: %v", info.Status)
	}
	s.SetWatermark(20)
	got, info := s.NextN(nil, 0, 5)
	if !slices.Equal(timesOf(got), []Timestamp{10, 20}) || info.Truncated || info.Status != StatusOK {
		t.Fatalf("NextN(0) at wm 20 = %v %+v", timesOf(got), info)
	}
	got, info = s.NextN(nil, 0, 1)
	if !slices.Equal(timesOf(got), []Timestamp{10}) || !info.Truncated {
		t.Fatalf("NextN(0, 1) = %v %+v (truncated: 20 is also final)", timesOf(got), info)
	}
	if _, info := s.NextN(nil, 20, 5); info.Status != StatusNotReady {
		t.Fatalf("NextN(20) with 30 pending: %v", info.Status)
	}
	s.SetWatermark(29)
	if _, info := s.NextN(nil, 20, 5); info.Status != StatusNotReady {
		t.Fatalf("NextN(20) at wm+1: %v", info.Status)
	}
	s.SetWatermark(MaxTimestamp)
	if got, info := s.NextN(nil, 20, 5); info.Status != StatusOK || !slices.Equal(timesOf(got), []Timestamp{30}) {
		t.Fatalf("NextN(20) complete: %v %+v", timesOf(got), info)
	}
	if _, info := s.NextN(nil, 30, 5); info.Status != StatusNotFound {
		t.Fatalf("NextN(30) complete: %v", info.Status)
	}
}

// Q-011, Q-013, §8.2: limit <= 0 means MaxReadBatch; dst keeps its prefix; nil
// dst works.
func TestBatch_LimitAndDst(t *testing.T) {
	s := newTimeSeries(t, 16, 4, 1, 2, 3, 4, 5, 6)
	for _, limit := range []int{0, -3, 4, 100} {
		got, info := s.Range(nil, 0, 10, limit)
		if info.N != 4 || len(got) != 4 || !info.Truncated {
			t.Errorf("limit %d: N %d truncated %v", limit, info.N, info.Truncated)
		}
	}
	prefix := []Entry[int]{{Value: -1}, {Value: -2}}
	got, info := s.AtAll(prefix, 3, 0)
	if len(got) != 3 || got[0].Value != -1 || got[1].Value != -2 || got[2].Meta.EventTime != 3 || info.N != 1 {
		t.Fatalf("AtAll with prefix = %+v %+v", got, info)
	}
	vals, _ := s.RangeValues([]int{-5}, 5, 7, 0)
	if !slices.Equal(vals, []int{-5, 5, 6}) {
		t.Fatalf("RangeValues with prefix = %v", vals)
	}
}

// T-023, E-015, Q-016: every batch method on TimeIndexNone returns
// TimeIndexDisabled with no data and no state change, gate on or off.
func TestBatch_NoneNoStateChange(t *testing.T) {
	for _, gate := range []bool{false, true} {
		for _, n := range []int{0, 4} {
			s := mustNew(t, Config[int]{Capacity: 3, GateReadsByWatermark: gate})
			appendTimes(t, s, []Timestamp{1, 2, 3, 4}[:n]...)
			before := s.Stats()
			infos := map[string]ReadInfo{}
			var got []Entry[int]
			var vals []int
			got, infos["Range"] = s.Range(got, 0, 10, 0)
			vals, infos["RangeValues"] = s.RangeValues(vals, 0, 10, 0)
			got, infos["AtAll"] = s.AtAll(got, 2, 0)
			got, infos["Continue"] = s.Continue(got, TimeCursor{}, 0)
			vals, infos["ContinueValues"] = s.ContinueValues(vals, makeCursor(99, 1, timeKey{}, 5), 0)
			got, infos["PrevN"] = s.PrevN(got, 10, 3)
			got, infos["NextN"] = s.NextN(got, 0, 3)
			for name, info := range infos {
				if info.Status != StatusTimeIndexDisabled || info.N != 0 || info.Truncated ||
					info.Evicted || !info.Next.IsZero() || !info.LastRef.IsZero() {
					t.Errorf("gate %v len %d: %s info %+v", gate, n, name, info)
				}
			}
			if len(got) != 0 || len(vals) != 0 {
				t.Errorf("data returned on None: %v %v", got, vals)
			}
			seq, st := s.Entries(0, 10)
			if st != StatusTimeIndexDisabled {
				t.Errorf("Entries status %v", st)
			}
			for range seq {
				t.Error("Entries yielded on None")
			}
			if after := s.Stats(); !statsEqual(before, after) {
				t.Errorf("state changed: %+v vs %+v", after, before)
			}
			mustInvariants(t, s)
		}
	}
}

// Q-015, §8.3, P-15: Entries yields the full Range in pages, never under the
// lock, and reports its status up front.
func TestEntries(t *testing.T) {
	s := newTimeSeries(t, 64, 3, 9, 1, 5, 3, 7, 3, 2, 8)
	want, info := s.Range(nil, 2, 9, 0) // MaxReadBatch 3: page manually
	for info.Truncated {
		want, info = s.Continue(want, info.Next, 0)
	}
	if !slices.Equal(timesOf(want), []Timestamp{2, 3, 3, 5, 7, 8}) {
		t.Fatalf("paged Range = %v", timesOf(want))
	}

	seq, st := s.Entries(2, 9)
	if st != StatusOK {
		t.Fatalf("status %v", st)
	}
	var got []Entry[int]
	for e := range seq {
		got = append(got, e)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("Entries = %v, want %v", seqsOf(got), seqsOf(want))
	}

	t.Run("early break", func(t *testing.T) {
		n := 0
		for range seq {
			n++
			if n == 4 {
				break
			}
		}
		if n != 4 {
			t.Fatalf("n = %d", n)
		}
	})

	t.Run("status up front", func(t *testing.T) {
		if _, st := s.Entries(5, 5); st != StatusNotFound {
			t.Errorf("empty interval: %v", st)
		}
		g := mustNew(t, Config[int]{Capacity: 4, TimeIndex: TimeIndexBTree, GateReadsByWatermark: true})
		if _, st := g.Entries(0, 10); st != StatusNotReady {
			t.Errorf("gated: %v", st)
		}
		e := mustNew(t, Config[int]{Capacity: 4, TimeIndex: TimeIndexBTree})
		seq, st := e.Entries(0, 10)
		if st != StatusOK {
			t.Errorf("empty series: %v", st)
		}
		for range seq {
			t.Error("empty series yielded")
		}
	})

	t.Run("calls into the series while iterating", func(t *testing.T) {
		s := newTimeSeries(t, 64, 2, 1, 2, 3, 4, 5)
		done := make(chan []Timestamp)
		go func() {
			seq, _ := s.Entries(0, 100)
			var seen []Timestamp
			for e := range seq {
				seen = append(seen, e.Meta.EventTime)
				// Would deadlock if the lock were held while yielding (§8.3, C-008).
				mustAppend(t, s, 0, at(e.Meta.EventTime+50))
				s.AsOf(e.Meta.EventTime)
				if len(seen) == 8 {
					break
				}
			}
			done <- seen
		}()
		select {
		case seen := <-done:
			// Records appended during the scan ahead of the cursor are visible.
			if want := []Timestamp{1, 2, 3, 4, 5, 51, 52, 53}; !slices.Equal(seen, want) {
				t.Fatalf("seen %v, want %v", seen, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Entries held the lock while yielding")
		}
	})

	t.Run("Clear ends the scan", func(t *testing.T) {
		s := newTimeSeries(t, 64, 2, 1, 2, 3, 4, 5)
		seq, _ := s.Entries(0, 100)
		var seen []Timestamp
		for e := range seq {
			seen = append(seen, e.Meta.EventTime)
			if len(seen) == 2 {
				if err := s.Clear(); err != nil {
					t.Fatal(err)
				}
				appendTimes(t, s, 1, 2, 3)
			}
		}
		if !slices.Equal(seen, []Timestamp{1, 2}) {
			t.Fatalf("seen %v, want [1 2] (scan ends at Reset)", seen)
		}
	})
}

// T-015: a Clone panic in a batch read leaves the mutex free.
func TestBatch_ClonePanicReleasesLock(t *testing.T) {
	clone := func(v int) int {
		if v < 0 {
			panic("clone")
		}
		return v
	}
	s := mustNew(t, Config[int]{Capacity: 64, TimeIndex: TimeIndexBTree, Clone: clone})
	mustAppend(t, s, -1, at(10))
	reads := map[string]func(){
		"Range":       func() { s.Range(nil, 0, 20, 0) },
		"RangeValues": func() { s.RangeValues(nil, 0, 20, 0) },
		"AtAll":       func() { s.AtAll(nil, 10, 0) },
		"PrevN":       func() { s.PrevN(nil, 10, 1) },
		"NextN":       func() { s.NextN(nil, 9, 1) },
		"Entries": func() {
			seq, _ := s.Entries(0, 20)
			for range seq {
			}
		},
	}
	for name, read := range reads {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s did not panic", name)
				}
			}()
			read()
		}()
		if _, err := s.Append(1, at(1)); err != nil {
			t.Fatal(err)
		}
	}
}

// T-026 (part): batch reads with sufficient cap(dst) do not allocate.
func TestAllocs_BatchReads(t *testing.T) {
	skipAllocTest(t)
	s := mustNew(t, Config[[2]int64]{Capacity: 2048, TimeIndex: TimeIndexBTree, MaxReadBatch: 128})
	for i := range 5000 {
		if _, err := s.Append([2]int64{int64(i), 1}, at(Timestamp(i-i%3))); err != nil {
			t.Fatal(err)
		}
	}
	dst := make([]Entry[[2]int64], 0, 128)
	vals := make([][2]int64, 0, 128)
	_, info := s.Range(dst[:0], 3500, 4500, 64)
	cursor := info.Next
	if cursor.IsZero() {
		t.Fatal("expected a cursor")
	}
	fns := map[string]func(){
		"Range":          func() { dst, _ = s.Range(dst[:0], 3500, 4500, 64) },
		"RangeValues":    func() { vals, _ = s.RangeValues(vals[:0], 3500, 4500, 64) },
		"AtAll":          func() { dst, _ = s.AtAll(dst[:0], 4002, 0) },
		"Continue":       func() { dst, _ = s.Continue(dst[:0], cursor, 64) },
		"ContinueValues": func() { vals, _ = s.ContinueValues(vals[:0], cursor, 64) },
		"PrevN":          func() { dst, _ = s.PrevN(dst[:0], 4000, 100) },
		"NextN":          func() { dst, _ = s.NextN(dst[:0], 4000, 100) },
	}
	for name, fn := range fns {
		if n := allocsPerOp(300, fn); n != 0 {
			t.Errorf("%s: %.4f allocs/op, want 0", name, n)
		}
	}
}
