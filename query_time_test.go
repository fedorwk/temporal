package temporal

import (
	"testing"
	"time"
)

type pointCase struct {
	q    pointQuery
	ts   Timestamp
	want Status
	seq  Seq // expected Ref.Seq when OK
}

func checkPoints(t *testing.T, s *Series[int], cases []pointCase) {
	t.Helper()
	for _, c := range cases {
		e, st := c.q.run(s, c.ts)
		if st != c.want || (st == StatusOK && e.Meta.Ref.Seq != c.seq) || (st != StatusOK && e != (Entry[int]{})) {
			t.Errorf("%v(%d) = (seq %d, %v), want (seq %d, %v)", c.q, c.ts, e.Meta.Ref.Seq, st, c.seq, c.want)
		}
		if st == StatusOK && e.Meta.Ref.Gen != s.Gen() {
			t.Errorf("%v(%d) returned Gen %d", c.q, c.ts, e.Meta.Ref.Gen)
		}
	}
}

func appendTimes(t *testing.T, s *Series[int], times ...Timestamp) {
	t.Helper()
	for _, ts := range times {
		mustAppend(t, s, int(ts), at(ts))
	}
}

// T-017, §7.2, E-003, Q-003, Q-004: the status matrix of every point query,
// with Expired exact at the envelope boundaries for the ordered lookups and
// conservative for the equality lookups.
func TestPointQueries_StatusMatrix(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		s := mustNew(t, Config[int]{Capacity: 4, TimeIndex: TimeIndexBTree})
		var cases []pointCase
		for q := range numPointQueries {
			for _, ts := range []Timestamp{MinTimestamp, 0, MaxTimestamp} {
				cases = append(cases, pointCase{q: q, ts: ts, want: StatusNotFound})
			}
		}
		checkPoints(t, s, cases)
	})

	t.Run("evicted below retained", func(t *testing.T) {
		// Evicted 30, 20, 40 → envelope [20, 40]. Retained: 100 (s4), 110 (s5), 110 (s6), 120 (s7).
		s := mustNew(t, Config[int]{Capacity: 4, TimeIndex: TimeIndexBTree})
		appendTimes(t, s, 30, 20, 40, 100, 110, 110, 120)
		checkPoints(t, s, []pointCase{
			{qAsOf, MinTimestamp, StatusNotFound, 0},
			{qAsOf, 19, StatusNotFound, 0},
			{qAsOf, 20, StatusExpired, 0}, // ts == MinEvicted
			{qAsOf, 99, StatusExpired, 0},
			{qAsOf, 100, StatusOK, 4},
			{qAsOf, 110, StatusOK, 6}, // latest ingested among ties
			{qAsOf, 115, StatusOK, 6},
			{qAsOf, MaxTimestamp, StatusOK, 7},

			{qBefore, 20, StatusNotFound, 0}, // MinEvicted is not < 20
			{qBefore, 21, StatusExpired, 0},
			{qBefore, 100, StatusExpired, 0},
			{qBefore, 101, StatusOK, 4},
			{qBefore, 111, StatusOK, 6},

			{qAfter, 39, StatusOK, 4},
			{qAfter, 110, StatusOK, 7},
			{qAfter, 120, StatusNotFound, 0},
			{qAfter, MaxTimestamp, StatusNotFound, 0},
			{qAtOrAfter, 110, StatusOK, 5}, // earliest ingested among ties
			{qAtOrAfter, 121, StatusNotFound, 0},
			{qAtOrAfter, MinTimestamp, StatusOK, 4},

			{qFirstAt, 19, StatusNotFound, 0},
			{qFirstAt, 20, StatusExpired, 0},
			{qFirstAt, 25, StatusExpired, 0}, // conservative: never stored, but inside the envelope
			{qFirstAt, 40, StatusExpired, 0},
			{qFirstAt, 41, StatusNotFound, 0},
			{qFirstAt, 105, StatusNotFound, 0},
			{qFirstAt, 110, StatusOK, 5},
			{qLastAt, 110, StatusOK, 6},
			{qLastAt, 40, StatusExpired, 0},
			{qLastAt, 41, StatusNotFound, 0},
			{qLastAt, MaxTimestamp, StatusNotFound, 0},

			{qLatestByEventTime, 0, StatusOK, 7},
		})
	})

	t.Run("evicted above retained", func(t *testing.T) {
		// Evicted 50, 60 → envelope [50, 60]. Retained (late): 10 (s3), 12 (s4).
		s := mustNew(t, Config[int]{Capacity: 2, TimeIndex: TimeIndexBTree})
		appendTimes(t, s, 50, 60, 10, 12)
		checkPoints(t, s, []pointCase{
			{qAfter, 11, StatusOK, 4},
			{qAfter, 12, StatusExpired, 0},
			{qAfter, 59, StatusExpired, 0}, // MaxEvicted > ts
			{qAfter, 60, StatusNotFound, 0},
			{qAfter, 61, StatusNotFound, 0},
			{qAtOrAfter, 12, StatusOK, 4},
			{qAtOrAfter, 13, StatusExpired, 0},
			{qAtOrAfter, 60, StatusExpired, 0}, // MaxEvicted >= ts
			{qAtOrAfter, 61, StatusNotFound, 0},
			{qAsOf, 9, StatusNotFound, 0},
			{qAsOf, 49, StatusOK, 4},
			{qAsOf, 50, StatusOK, 4}, // retained match wins over the envelope
			{qBefore, 11, StatusOK, 3},
			{qLatestByEventTime, 0, StatusOK, 4},
		})
	})

	t.Run("irrelevant evictions", func(t *testing.T) {
		// Evicted 500 → envelope [500, 500]; retained 1000 (s2).
		s := mustNew(t, Config[int]{Capacity: 1, TimeIndex: TimeIndexBTree})
		appendTimes(t, s, 500, 1000)
		checkPoints(t, s, []pointCase{
			{qAsOf, 499, StatusNotFound, 0},
			{qBefore, 500, StatusNotFound, 0},
			{qAfter, 1000, StatusNotFound, 0},
			{qAtOrAfter, 1001, StatusNotFound, 0},
			{qFirstAt, 501, StatusNotFound, 0},
			{qLastAt, 499, StatusNotFound, 0},
		})
	})
}

// §7.5, E-015: status precedence. Disabled beats NotReady and data statuses;
// NotReady beats Expired, NotFound and OK for backward reads.
func TestPointQueries_Precedence(t *testing.T) {
	t.Run("disabled first", func(t *testing.T) {
		for _, gate := range []bool{false, true} {
			s := mustNew(t, Config[int]{Capacity: 2, GateReadsByWatermark: gate})
			var cases []pointCase
			for q := range numPointQueries {
				cases = append(cases, pointCase{q: q, ts: 5, want: StatusTimeIndexDisabled})
			}
			checkPoints(t, s, cases) // empty
			appendTimes(t, s, 1, 2, 3)
			checkPoints(t, s, cases) // with data and evictions
		}
	})
	t.Run("not ready before data", func(t *testing.T) {
		s := mustNew(t, Config[int]{Capacity: 2, TimeIndex: TimeIndexBTree, GateReadsByWatermark: true})
		checkPoints(t, s, []pointCase{
			{qAsOf, 5, StatusNotReady, 0}, // initial watermark is MinTimestamp
			{qAsOf, MinTimestamp, StatusNotFound, 0},
			{qFirstAt, 5, StatusNotReady, 0},
			{qLatestByEventTime, 0, StatusNotFound, 0}, // never gated
		})
		appendTimes(t, s, 10, 20, 30) // evicts 10
		s.SetWatermark(15)
		checkPoints(t, s, []pointCase{
			{qAsOf, 15, StatusExpired, 0},
			{qAsOf, 16, StatusNotReady, 0}, // beats Expired
			{qAsOf, 25, StatusNotReady, 0}, // beats OK
			{qBefore, 15, StatusExpired, 0},
			{qBefore, 16, StatusNotReady, 0},
			{qLastAt, 20, StatusNotReady, 0},
			{qFirstAt, 10, StatusExpired, 0},
			{qLatestByEventTime, 0, StatusOK, 3},
		})
	})
}

// P-13, S4-01: forward reads on a gated series return only final answers:
// a record at or below the watermark. Boundaries: EventTime == wm, wm+1, and
// wm == MaxTimestamp.
func TestPointQueries_ForwardGate(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 8, TimeIndex: TimeIndexBTree, GateReadsByWatermark: true})
	appendTimes(t, s, 10, 20, 30)
	checkPoints(t, s, []pointCase{
		{qAfter, 5, StatusNotReady, 0}, // watermark still MinTimestamp
		{qAtOrAfter, 10, StatusNotReady, 0},
	})
	s.SetWatermark(20)
	checkPoints(t, s, []pointCase{
		{qAfter, 10, StatusOK, 2}, // EventTime == wm is final
		{qAtOrAfter, 20, StatusOK, 2},
		{qAtOrAfter, 11, StatusOK, 2},
		{qAfter, 20, StatusNotReady, 0}, // next record (30) is beyond wm
		{qAfter, 30, StatusNotReady, 0}, // nothing yet; more may arrive
		{qAsOf, 20, StatusOK, 2},
		{qAsOf, 21, StatusNotReady, 0},
	})
	s.SetWatermark(29)
	checkPoints(t, s, []pointCase{
		{qAfter, 20, StatusNotReady, 0}, // EventTime == wm+1
	})
	// A late record behind the watermark becomes the final answer.
	mustAppend(t, s, 25, at(25))
	checkPoints(t, s, []pointCase{{qAfter, 20, StatusOK, 4}})

	s.SetWatermark(MaxTimestamp)
	checkPoints(t, s, []pointCase{
		{qAfter, 20, StatusOK, 4},
		{qAfter, 30, StatusNotFound, 0}, // series complete: absence is final
		{qAtOrAfter, MaxTimestamp, StatusNotFound, 0},
		{qAsOf, MaxTimestamp, StatusOK, 3},
	})

	t.Run("expired wins when nothing matches", func(t *testing.T) {
		s := mustNew(t, Config[int]{Capacity: 1, TimeIndex: TimeIndexBTree, GateReadsByWatermark: true})
		appendTimes(t, s, 50, 10) // evicts 50; retained 10
		s.SetWatermark(5)
		checkPoints(t, s, []pointCase{
			{qAfter, 20, StatusExpired, 0},      // MaxEvicted 50 > 20
			{qAfter, 50, StatusNotReady, 0},     // not expired, nothing yet
			{qAtOrAfter, 10, StatusNotReady, 0}, // match at 10 is beyond wm 5
		})
	})
}

// T-004, E-004: duplicate timestamps, late events and sentinel query times.
func TestPointQueries_DuplicatesAndLate(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 16, TimeIndex: TimeIndexBTree})
	appendTimes(t, s, 100, 100, 200, 100) // s1, s2, s3, s4 (s4 late duplicate)
	checkPoints(t, s, []pointCase{
		{qFirstAt, 100, StatusOK, 1},
		{qLastAt, 100, StatusOK, 4},
		{qAsOf, 100, StatusOK, 4},
		{qAsOf, 199, StatusOK, 4},
		{qAtOrAfter, 100, StatusOK, 1},
		{qAfter, 99, StatusOK, 1},
		{qBefore, 200, StatusOK, 4},
		{qAsOf, MaxTimestamp, StatusOK, 3},
		{qAsOf, MinTimestamp, StatusNotFound, 0},
		{qAfter, MaxTimestamp, StatusNotFound, 0},
		{qAtOrAfter, MaxTimestamp, StatusNotFound, 0},
		{qBefore, MinTimestamp, StatusNotFound, 0},
		{qFirstAt, MinTimestamp, StatusNotFound, 0},
		{qLastAt, MaxTimestamp, StatusNotFound, 0},
		{qLatestByEventTime, 0, StatusOK, 3},
	})
	// A late record earlier than everything is found by AsOf and keeps FlagLate.
	res := mustAppend(t, s, 50, at(50))
	e, st := s.AsOf(60)
	if st != StatusOK || e.Meta.Ref != res.Ref || e.Meta.Flags&FlagLate == 0 {
		t.Fatalf("AsOf(60) = %+v, %v", e, st)
	}
	if e, _ := s.LatestByEventTime(); e.Meta.Ref.Seq != 3 {
		t.Fatalf("LatestByEventTime changed after a late append: %+v", e)
	}
	if e, _ := s.Latest(); e.Meta.Ref != res.Ref {
		t.Fatalf("Latest should be the newest ingested: %+v", e)
	}
}

// T-023, E-015, Q-016: on TimeIndexNone every point method returns
// StatusTimeIndexDisabled with no data and no state change.
func TestPointQueries_NoneNoStateChange(t *testing.T) {
	for _, gate := range []bool{false, true} {
		for _, n := range []int{0, 5} {
			s := mustNew(t, Config[int]{Capacity: 3, GateReadsByWatermark: gate, LateTolerance: time.Nanosecond})
			appendTimes(t, s, []Timestamp{5, 4, 3, 2, 1}[:n]...)
			before := s.Stats()
			for q := range numPointQueries {
				for _, ts := range []Timestamp{MinTimestamp, 3, MaxTimestamp} {
					e, st := q.run(s, ts)
					if st != StatusTimeIndexDisabled || e != (Entry[int]{}) {
						t.Errorf("gate %v len %d: %v(%d) = (%+v, %v)", gate, n, q, ts, e, st)
					}
				}
			}
			if after := s.Stats(); !statsEqual(before, after) {
				t.Errorf("state changed: %+v vs %+v", after, before)
			}
			mustInvariants(t, s)
		}
	}
}

// T-015, C-007: a Clone panic inside an event-time read leaves the mutex free.
func TestPointQueries_ClonePanicReleasesLock(t *testing.T) {
	clone := func(v int) int {
		if v < 0 {
			panic("clone")
		}
		return v
	}
	s := mustNew(t, Config[int]{Capacity: 16, TimeIndex: TimeIndexBTree, Clone: clone})
	mustAppend(t, s, -1, at(10))
	// Each query targets the poisoned record at 10; the follow-up appends at 5
	// never become the answer.
	tsFor := map[pointQuery]Timestamp{qAsOf: 10, qBefore: 11, qAfter: 9, qAtOrAfter: 10, qFirstAt: 10, qLastAt: 10}
	for q := range numPointQueries {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%v did not panic", q)
				}
			}()
			q.run(s, tsFor[q])
		}()
		if _, err := s.Append(1, at(5)); err != nil { // would deadlock if the lock leaked
			t.Fatal(err)
		}
	}
}

// T-026 (part): point reads do not allocate.
func TestAllocs_PointQueries(t *testing.T) {
	skipAllocTest(t)
	s := mustNew(t, Config[[4]float64]{Capacity: 1024, TimeIndex: TimeIndexBTree, GateReadsByWatermark: true})
	for i := range 3000 {
		if _, err := s.Append([4]float64{1, 2, 3, 4}, at(Timestamp(i-i%5))); err != nil {
			t.Fatal(err)
		}
	}
	s.SetWatermark(2900)
	fns := map[string]func(){
		"AsOf":              func() { s.AsOf(2500) },
		"Before":            func() { s.Before(2500) },
		"After":             func() { s.After(2500) },
		"AtOrAfter":         func() { s.AtOrAfter(2500) },
		"FirstAt":           func() { s.FirstAt(2500) },
		"LastAt":            func() { s.LastAt(2500) },
		"LatestByEventTime": func() { s.LatestByEventTime() },
		"gated":             func() { s.AsOf(2950) },
	}
	for name, fn := range fns {
		if n := allocsPerOp(500, fn); n != 0 {
			t.Errorf("%s: %.4f allocs/op, want 0", name, n)
		}
	}
}
