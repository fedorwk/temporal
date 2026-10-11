package temporal

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
)

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

// W-009, TI-012: TimeIndexNone and TimeIndexOrdered Append stay at zero
// allocations; BTree Append is reported (amortized node growth is allowed).
func TestAllocs_AppendByMode(t *testing.T) {
	skipAllocTest(t)
	for _, mode := range allModes {
		s := mustNew(t, withMode(mode, Config[int]{Capacity: 1024}))
		ts := Timestamp(0)
		disorder := Timestamp(7)
		if mode == TimeIndexOrdered {
			disorder = 1
		}
		appendOne := func() {
			ts++
			if _, err := s.Append(1, at(ts-ts%disorder)); err != nil {
				t.Fatal(err)
			}
		}
		for range 4096 {
			appendOne()
		}
		n := allocsPerOp(10000, appendOne)
		t.Logf("mode %d: Append steady state %.4f allocs/op", mode, n)
		if mode != TimeIndexBTree && n != 0 {
			t.Errorf("mode %d Append: %.4f allocs/op, want 0", mode, n)
		}
	}
}

// TimeIndexOrdered: out-of-order records fail with ErrLate and change
// nothing; ties are accepted; a batch is all-or-nothing; Clear starts a new
// timeline.
func TestOrdered_RejectsOutOfOrder(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 4, TimeIndex: TimeIndexOrdered, LatePolicy: LateReject})
	mustAppend(t, s, 1, at(10))
	mustAppend(t, s, 2, at(10)) // tie
	mustAppend(t, s, 3, at(12))
	if _, err := s.Append(4, at(11)); !errors.Is(err, ErrLate) {
		t.Fatalf("out-of-order Append: %v, want ErrLate", err)
	}
	bad := []Input[int]{{Value: 5, Meta: at(12)}, {Value: 6, Meta: at(13)}, {Value: 7, Meta: at(12)}}
	if _, err := s.AppendBatch(nil, bad); !errors.Is(err, ErrLate) {
		t.Fatalf("out-of-order AppendBatch: %v, want ErrLate", err)
	}
	if e, _ := s.Latest(); s.Len() != 3 || e.Value != 3 {
		t.Fatalf("after rejections: len %d, latest %d", s.Len(), e.Value)
	}
	good := []Input[int]{{Value: 5, Meta: at(12)}, {Value: 6, Meta: at(13)}, {Value: 7, Meta: at(13)}}
	if _, err := s.AppendBatch(nil, good); err != nil {
		t.Fatal(err)
	}
	mustInvariants(t, s)

	// Retained: 3@12, 5@12, 6@13, 7@13; evicted: 1@10, 2@10.
	val := func(e Entry[int], st Status) string { return fmt.Sprint(e.Value, st) }
	for _, c := range []struct{ got, want string }{
		{val(s.AsOf(12)), "5 OK"},
		{val(s.FirstAt(12)), "3 OK"},
		{val(s.After(12)), "6 OK"},
		{val(s.Before(13)), "5 OK"},
		{val(s.AsOf(11)), "0 Expired"},
		{val(s.LatestByEventTime()), "7 OK"},
	} {
		if c.got != c.want {
			t.Errorf("got %s, want %s", c.got, c.want)
		}
	}
	all, _ := s.AtAll(nil, 13, 0)
	if len(all) != 2 || all[0].Value != 6 || all[1].Value != 7 {
		t.Errorf("AtAll(13) = %+v", all)
	}

	if err := s.Clear(); err != nil {
		t.Fatal(err)
	}
	mustAppend(t, s, 8, at(1)) // a new generation may start earlier
	if e, st := s.AsOf(1); st != StatusOK || e.Value != 8 {
		t.Fatalf("after Clear: AsOf(1) = %d %v", e.Value, st)
	}
	mustInvariants(t, s)
}

// TimeIndexOrdered answers every event-time query exactly like
// TimeIndexBTree on in-order streams, at capacities large enough for real
// binary searches and many ring wraps (the model tests use capacities <= 8).
func TestOrdered_MatchesBTree(t *testing.T) {
	r, _ := newRand(t)
	for run := range iters(30) {
		capacity := 1 + r.IntN(300)
		if run%3 == 0 {
			capacity = 1 << r.IntN(9)
		}
		cfg := Config[int]{Capacity: capacity, LatePolicy: LateReject, MaxReadBatch: r.IntN(40),
			GateReadsByWatermark: r.IntN(2) == 0}
		cfg.TimeIndex = TimeIndexOrdered
		o := mustNew(t, cfg)
		cfg.TimeIndex = TimeIndexBTree
		b := mustNew(t, cfg)
		var now Timestamp
		v := 0
		next := func() InputMeta {
			now += Timestamp(r.IntN(3)) // ties are common
			return at(now)
		}
		randTS := func() Timestamp {
			switch r.IntN(8) {
			case 0:
				return MinTimestamp + Timestamp(r.IntN(2))
			case 1:
				return MaxTimestamp - Timestamp(r.IntN(2))
			default:
				return now - Timestamp(r.IntN(3*capacity+4)) + 2
			}
		}
		// strip removes the series identity carried by cursors.
		strip := func(info ReadInfo) ReadInfo {
			info.Next = TimeCursor{series: uint64(boolInt(!info.Next.IsZero()))}
			return info
		}
		page := func(s *Series[int], from, to Timestamp, limit int) []any {
			var out []any
			e, info := s.Range(nil, from, to, limit)
			for {
				out = append(out, e, strip(info))
				if info.Next.IsZero() {
					return out
				}
				e, info = s.Continue(nil, info.Next, limit)
			}
		}
		for step := range 400 {
			switch op := r.IntN(20); {
			case op < 12:
				v++
				in := next()
				if op == 0 {
					in = at(now - 1) // rejected by both
				}
				ro, eo := o.Append(v, in)
				rb, eb := b.Append(v, in)
				if ro != rb || !errors.Is(eo, eb) && eo != eb {
					t.Fatalf("run %d step %d: Append %+v %v, BTree %+v %v", run, step, ro, eo, rb, eb)
				}
			case op < 15:
				items := make([]Input[int], 1+r.IntN(capacity))
				for i := range items {
					v++
					items[i] = Input[int]{Value: v, Meta: next()}
				}
				ro, eo := o.AppendBatch(nil, items)
				rb, eb := b.AppendBatch(nil, items)
				if eo != nil || eb != nil || !slices.Equal(ro, rb) {
					t.Fatalf("run %d step %d: AppendBatch %v %v", run, step, eo, eb)
				}
			case op < 16 && r.IntN(5) == 0:
				o.Clear()
				b.Clear()
				now -= Timestamp(r.IntN(100)) // the next generation may start earlier
			case op < 17:
				ts := randTS()
				o.SetWatermark(ts)
				b.SetWatermark(ts)
			}

			ts, n := randTS(), r.IntN(capacity+2)
			from, to := randTS(), randTS()
			type q struct {
				name string
				f    func(s *Series[int]) any
			}
			pt := func(e Entry[int], st Status) any { return []any{e, st} }
			bt := func(e []Entry[int], info ReadInfo) any { return []any{e, strip(info)} }
			for _, c := range []q{
				{"AsOf", func(s *Series[int]) any { return pt(s.AsOf(ts)) }},
				{"Before", func(s *Series[int]) any { return pt(s.Before(ts)) }},
				{"After", func(s *Series[int]) any { return pt(s.After(ts)) }},
				{"AtOrAfter", func(s *Series[int]) any { return pt(s.AtOrAfter(ts)) }},
				{"FirstAt", func(s *Series[int]) any { return pt(s.FirstAt(ts)) }},
				{"LastAt", func(s *Series[int]) any { return pt(s.LastAt(ts)) }},
				{"LatestByEventTime", func(s *Series[int]) any { return pt(s.LatestByEventTime()) }},
				{"AtAll", func(s *Series[int]) any { return bt(s.AtAll(nil, ts, n)) }},
				{"PrevN", func(s *Series[int]) any { return bt(s.PrevN(nil, ts, n)) }},
				{"NextN", func(s *Series[int]) any { return bt(s.NextN(nil, ts, n)) }},
				{"Range+Continue", func(s *Series[int]) any { return page(s, from, to, n) }},
				{"Stats", func(s *Series[int]) any { st := s.Stats(); st.TimeIndex = 0; return st }},
			} {
				if got, want := c.f(o), c.f(b); !reflect.DeepEqual(got, want) {
					t.Fatalf("run %d step %d (cap %d): %s(ts %d, n %d, from %d, to %d)\nordered %+v\nbtree   %+v",
						run, step, capacity, c.name, ts, n, from, to, got, want)
				}
			}
		}
		mustInvariants(t, o)
		mustInvariants(t, b)
	}
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
