package temporal

import (
	"errors"
	"math"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"
)

// E-001, Q-001, T-016 (part): Get reports NotFound, Expired or Reset for
// every non-retained Ref, and Seq restarts at 1 after Clear.
func TestGet_StatusMatrix(t *testing.T) {
	for _, mode := range allModes {
		s := mustNew(t, withMode(mode, Config[int]{Capacity: 3}))
		for i := 1; i <= 5; i++ {
			mustAppend(t, s, i*10, at(Timestamp(i)))
		}
		// Retained: Seq 3..5 of Gen 1.
		cases := []struct {
			ref  Ref
			want Status
			val  int
		}{
			{Ref{}, StatusNotFound, 0},
			{Ref{Gen: 1, Seq: 0}, StatusNotFound, 0},
			{Ref{Gen: 1, Seq: 1}, StatusExpired, 0},
			{Ref{Gen: 1, Seq: 2}, StatusExpired, 0},
			{Ref{Gen: 1, Seq: 3}, StatusOK, 30},
			{Ref{Gen: 1, Seq: 5}, StatusOK, 50},
			{Ref{Gen: 1, Seq: 6}, StatusNotFound, 0},
			{Ref{Gen: 1, Seq: Seq(math.MaxUint64)}, StatusNotFound, 0},
			{Ref{Gen: 2, Seq: 3}, StatusNotFound, 0},
			{Ref{Gen: 0, Seq: 3}, StatusReset, 0},
		}
		for _, tc := range cases {
			e, st := s.Get(tc.ref)
			if st != tc.want || e.Value != tc.val {
				t.Errorf("mode %d: Get(%+v) = (%d, %v), want (%d, %v)", mode, tc.ref, e.Value, st, tc.val, tc.want)
			}
			if st == StatusOK && e.Meta.Ref != tc.ref {
				t.Errorf("mode %d: Get(%+v) returned Ref %+v", mode, tc.ref, e.Meta.Ref)
			}
		}

		if err := s.Clear(); err != nil {
			t.Fatal(err)
		}
		if _, st := s.Get(Ref{Gen: 1, Seq: 4}); st != StatusReset {
			t.Errorf("mode %d: old-gen Get after Clear = %v, want Reset", mode, st)
		}
		if _, st := s.Get(Ref{Gen: 2, Seq: 1}); st != StatusNotFound {
			t.Errorf("mode %d: unassigned Get after Clear = %v, want NotFound", mode, st)
		}
		res := mustAppend(t, s, 7, at(1))
		if res.Ref != (Ref{Gen: 2, Seq: 1}) {
			t.Errorf("mode %d: first Ref after Clear = %+v, want {2 1}", mode, res.Ref)
		}
	}
}

// W-001, §9.5: AppendResult carries the Ref, applied series flags and the
// evicted Ref; stored Meta merges adapter and series flags.
func TestAppend_ResultAndMeta(t *testing.T) {
	s := mustNew(t, Config[string]{Capacity: 2})
	r1 := mustAppend(t, s, "a", InputMeta{EventTime: 10, ReceiveTime: 11, SourceSeq: 5, SourceEpoch: 2, Flags: FlagGap | FlagReplay})
	if r1 != (AppendResult{Outcome: OutcomeAppended, Ref: Ref{Gen: 1, Seq: 1}}) {
		t.Fatalf("r1 = %+v", r1)
	}
	mustAppend(t, s, "b", at(20))
	r3 := mustAppend(t, s, "c", InputMeta{EventTime: 5, Flags: FlagSnapshot}) // late, evicts Seq 1
	want := AppendResult{Outcome: OutcomeAppended, Ref: Ref{Gen: 1, Seq: 3}, Flags: FlagLate, Evicted: Ref{Gen: 1, Seq: 1}}
	if r3 != want {
		t.Fatalf("r3 = %+v, want %+v", r3, want)
	}
	e, st := s.Get(r3.Ref)
	if st != StatusOK || e.Value != "c" || e.Meta.Flags != FlagSnapshot|FlagLate || e.Meta.EventTime != 5 {
		t.Fatalf("Get = %+v, %v", e, st)
	}

	s2 := mustNew(t, Config[string]{Capacity: 2})
	mustAppend(t, s2, "x", InputMeta{EventTime: 10, ReceiveTime: 11, SourceSeq: 5, SourceEpoch: 2, Flags: FlagGap})
	e, _ = s2.Latest()
	wantMeta := Meta{EventTime: 10, ReceiveTime: 11, SourceSeq: 5, SourceEpoch: 2, Flags: FlagGap, Ref: Ref{Gen: 1, Seq: 1}}
	if e.Meta != wantMeta {
		t.Fatalf("Meta = %+v, want %+v", e.Meta, wantMeta)
	}
}

// E-009, E-010: sentinel EventTimes and series-owned flag bits are rejected
// with ErrInvalidInput and no state change.
func TestAppend_InvalidInput(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 4})
	mustAppend(t, s, 1, at(100))
	before := s.Stats()
	bad := []InputMeta{
		{EventTime: MinTimestamp},
		{EventTime: MaxTimestamp},
		{EventTime: 100, Flags: FlagLate},
		{EventTime: 100, Flags: FlagSourceSeqNotIncreasing},
		{EventTime: 100, Flags: 1 << 31},
		{EventTime: 100, Flags: FlagGap | 1<<20},
	}
	for i, m := range bad {
		if _, err := s.Append(2, m); !errors.Is(err, ErrInvalidInput) {
			t.Errorf("Append(%+v) error = %v, want ErrInvalidInput", m, err)
		}
		after := s.Stats()
		before.Rejected = uint64(i + 1) // P-04: only the diagnostic counter moves
		if !statsEqual(after, before) {
			t.Fatalf("state changed after rejected append: %+v vs %+v", after, before)
		}
	}
	mustInvariants(t, s)
	mustAppend(t, s, 3, at(MinTimestamp+1))
	mustAppend(t, s, 4, at(MaxTimestamp-1))
}

// statsEqual compares Stats values deeply (Indexes is a slice).
func statsEqual(a, b Stats) bool { return reflect.DeepEqual(a, b) }

// §5.3, §9.2, W-005, P-10: lateness uses MaxObservedEventTime - LateTolerance,
// saturating at MinTimestamp; LateReject returns ErrLate.
func TestAppend_LateDetection(t *testing.T) {
	cases := []struct {
		name      string
		tolerance time.Duration
		observed  []Timestamp
		ts        Timestamp
		late      bool
	}{
		{"empty generation is never late", 0, nil, -1000, false},
		{"equal to max is not late", 0, []Timestamp{100}, 100, false},
		{"below max with zero tolerance", 0, []Timestamp{100}, 99, true},
		{"max is a running max", 0, []Timestamp{100, 50}, 60, true},
		{"within tolerance", 5, []Timestamp{100}, 95, false},
		{"just beyond tolerance", 5, []Timestamp{100}, 94, true},
		{"saturated threshold", 10, []Timestamp{MinTimestamp + 3}, MinTimestamp + 1, false},
		{"huge tolerance", math.MaxInt64, []Timestamp{0}, MinTimestamp + 1, false},
		{"huge tolerance boundary", math.MaxInt64 - 1, []Timestamp{0}, MinTimestamp + 1, true},
		{"far below a high max", math.MaxInt64, []Timestamp{MaxTimestamp - 1}, MinTimestamp + 1, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, policy := range []LatePolicy{LateAccept, LateReject} {
				s := mustNew(t, Config[int]{Capacity: 8, LatePolicy: policy, LateTolerance: tc.tolerance})
				for _, ts := range tc.observed {
					if _, err := s.Append(0, at(ts)); err != nil && policy == LateAccept {
						t.Fatal(err)
					}
				}
				res, err := s.Append(1, at(tc.ts))
				switch {
				case !tc.late && (err != nil || res.Flags != 0):
					t.Fatalf("policy %d: got (%+v, %v), want not late", policy, res, err)
				case tc.late && policy == LateAccept && (err != nil || res.Flags != FlagLate):
					t.Fatalf("accept: got (%+v, %v), want FlagLate", res, err)
				case tc.late && policy == LateReject && !errors.Is(err, ErrLate):
					t.Fatalf("reject: got (%+v, %v), want ErrLate", res, err)
				}
				mustInvariants(t, s)
			}
		})
	}

	t.Run("Clear resets the tracker", func(t *testing.T) {
		s := mustNew(t, Config[int]{Capacity: 4})
		mustAppend(t, s, 0, at(100))
		if err := s.Clear(); err != nil {
			t.Fatal(err)
		}
		if res := mustAppend(t, s, 1, at(1)); res.Flags != 0 {
			t.Fatalf("first record after Clear flagged %#x", res.Flags)
		}
		if st := s.Stats(); st.MaxObservedEventTime != 1 {
			t.Fatalf("MaxObservedEventTime = %d", st.MaxObservedEventTime)
		}
	})
}

// §9.2, W-006, E-005: SourceSeq checking per epoch under each policy.
func TestAppend_SourceSeqPolicy(t *testing.T) {
	type step struct {
		seq     uint64
		epoch   SourceEpoch
		flagged bool
	}
	// The tracker keeps the highest SourceSeq of the epoch (DECISIONS S2-01).
	steps := []step{
		{5, 0, false},
		{6, 0, false},
		{6, 0, true},  // duplicate
		{4, 0, true},  // backwards
		{0, 0, false}, // not provided: never checked, does not move the tracker
		{7, 0, false},
		{10, 0, false},
		{8, 0, true},
		{9, 0, true},  // still below the epoch's highest (10)
		{1, 1, false}, // new epoch resets
		{1, 1, true},
		{2, 1, false},
		{1, 0, false}, // switching back is also a change of epoch
	}
	for _, policy := range []SourceSeqPolicy{SourceSeqIgnore, SourceSeqFlag, SourceSeqReject} {
		s := mustNew(t, Config[int]{Capacity: 32, SourceSeqPolicy: policy})
		var flagged uint64
		for i, st := range steps {
			res, err := s.Append(i, InputMeta{EventTime: Timestamp(i + 1), SourceSeq: st.seq, SourceEpoch: st.epoch})
			switch {
			case policy == SourceSeqIgnore || !st.flagged:
				if err != nil || res.Flags != 0 {
					t.Fatalf("policy %d step %d: got (%+v, %v), want clean", policy, i, res, err)
				}
			case policy == SourceSeqFlag:
				flagged++
				if err != nil || res.Flags != FlagSourceSeqNotIncreasing {
					t.Fatalf("flag step %d: got (%+v, %v)", i, res, err)
				}
			default:
				if !errors.Is(err, ErrSourceSeqNotIncreasing) {
					t.Fatalf("reject step %d: got (%+v, %v)", i, res, err)
				}
			}
		}
		if got := s.Stats().SourceSeqNotIncr; got != flagged {
			t.Fatalf("policy %d: SourceSeqNotIncr = %d, want %d", policy, got, flagged)
		}
	}
}

// E-006: FlagGap is stored as given and counted; it does not alter the Series.
func TestAppend_GapFlagCounted(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 2})
	mustAppend(t, s, 1, InputMeta{EventTime: 1, SourceSeq: 1})
	mustAppend(t, s, 2, InputMeta{EventTime: 2, SourceSeq: 50, Flags: FlagGap})
	mustAppend(t, s, 3, InputMeta{EventTime: 3, SourceSeq: 51, Flags: FlagGap | FlagSnapshot})
	if st := s.Stats(); st.GapFlags != 2 || st.Appended != 3 || st.Evicted != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

// §6.2: Append fails with ErrSeqExhausted instead of assigning MaxUint64.
func TestAppend_SeqExhausted(t *testing.T) {
	for _, capacity := range []int{1, 3} {
		s := mustNew(t, Config[int]{Capacity: capacity})
		s.testSetNewest(seqMax - 2)
		res := mustAppend(t, s, 1, at(1))
		if res.Ref.Seq != seqMax-1 {
			t.Fatalf("Seq = %d, want MaxUint64-1", res.Ref.Seq)
		}
		before := s.Stats()
		if _, err := s.Append(2, at(2)); !errors.Is(err, ErrSeqExhausted) {
			t.Fatalf("error = %v, want ErrSeqExhausted", err)
		}
		after := s.Stats()
		before.Rejected++
		if !statsEqual(before, after) {
			t.Fatalf("state changed: %+v vs %+v", after, before)
		}
		mustInvariants(t, s)
		if e, st := s.Latest(); st != StatusOK || e.Value != 1 {
			t.Fatalf("Latest = %+v, %v", e, st)
		}
		// Clear restarts the sequence.
		if err := s.Clear(); err != nil {
			t.Fatal(err)
		}
		if res := mustAppend(t, s, 3, at(3)); res.Ref.Seq != 1 {
			t.Fatalf("Seq after Clear = %d", res.Ref.Seq)
		}
	}
}

// §6.2: Clear fails with ErrGenExhausted instead of wrapping Gen.
func TestClear_GenExhausted(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 2})
	s.testSetGen(math.MaxUint32 - 1)
	if err := s.Clear(); err != nil {
		t.Fatalf("Clear to MaxUint32: %v", err)
	}
	mustAppend(t, s, 1, at(1))
	if err := s.Clear(); !errors.Is(err, ErrGenExhausted) {
		t.Fatalf("error = %v, want ErrGenExhausted", err)
	}
	if s.Gen() != math.MaxUint32 || s.Len() != 1 {
		t.Fatalf("state changed: gen %d len %d", s.Gen(), s.Len())
	}
}

// §15.4: Clear resets data, envelope, trackers and watermark; Gen increments.
func TestClear_Resets(t *testing.T) {
	s := mustNew(t, Config[*int]{Capacity: 2, SourceSeqPolicy: SourceSeqFlag})
	for i := range 4 {
		x := i
		mustAppend(t, s, &x, InputMeta{EventTime: Timestamp(10 + i), SourceSeq: 9})
	}
	s.SetWatermark(11)
	if err := s.Clear(); err != nil {
		t.Fatal(err)
	}
	st := s.Stats()
	if st.Gen != 2 || st.Len != 0 || st.HasEvicted || st.MinEvicted != 0 || st.MaxEvicted != 0 ||
		st.MaxObservedEventTime != MinTimestamp || st.Watermark != MinTimestamp || st.Clears != 1 ||
		!st.Oldest.IsZero() || !st.Newest.IsZero() {
		t.Fatalf("stats after Clear = %+v", st)
	}
	if st.Appended != 4 || st.Evicted != 2 {
		t.Fatalf("lifetime counters reset: %+v", st)
	}
	for i := range s.ring.values {
		if s.ring.values[i] != nil {
			t.Fatalf("slot %d still references a value", i)
		}
	}
	// SourceSeq tracker reset: the same SourceSeq is not flagged.
	x := 0
	if res := mustAppend(t, s, &x, InputMeta{EventTime: 1, SourceSeq: 9}); res.Flags != 0 {
		t.Fatalf("flags after Clear = %#x", res.Flags)
	}
	mustInvariants(t, s)
}

// §15.3: the eviction envelope tracks min and max evicted EventTime.
func TestEvictionEnvelope(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 2})
	for _, ts := range []Timestamp{50, 10, 70, 30, 20} {
		mustAppend(t, s, 0, at(ts))
	}
	// Evicted: 50, 10, 70.
	st := s.Stats()
	if !st.HasEvicted || st.MinEvicted != 10 || st.MaxEvicted != 70 || st.MaxObservedEventTime != 70 {
		t.Fatalf("stats = %+v", st)
	}
}

// W-008, E-013, P-08: after Close, writes fail, reads keep working, Close
// stays nil and SetWatermark returns false.
func TestClose_Semantics(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 3})
	mustAppend(t, s, 1, at(1))
	mustAppend(t, s, 2, at(2))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	before := s.Stats()
	if _, err := s.Append(3, at(3)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Append after Close = %v", err)
	}
	if err := s.Clear(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Clear after Close = %v", err)
	}
	if s.SetWatermark(10) {
		t.Fatal("SetWatermark after Close returned true")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
	if after := s.Stats(); !statsEqual(before, after) {
		t.Fatalf("state changed after Close: %+v vs %+v", after, before)
	}
	if e, st := s.Get(Ref{Gen: 1, Seq: 1}); st != StatusOK || e.Value != 1 {
		t.Fatalf("Get after Close = %+v, %v", e, st)
	}
	if e, st := s.Latest(); st != StatusOK || e.Value != 2 {
		t.Fatalf("Latest after Close = %+v, %v", e, st)
	}
	if got, info := s.ReadFrom(nil, Ref{}, 0); info.N != 2 || len(got) != 2 {
		t.Fatalf("ReadFrom after Close = %+v", info)
	}
}

// §19, T-020 (part): the watermark starts at MinTimestamp and is monotonic.
func TestWatermark_Monotonic(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 1})
	if s.Watermark() != MinTimestamp {
		t.Fatalf("initial watermark = %d", s.Watermark())
	}
	steps := []struct {
		ts   Timestamp
		ok   bool
		want Timestamp
	}{
		{MinTimestamp, false, MinTimestamp},
		{10, true, 10},
		{10, false, 10},
		{9, false, 10},
		{11, true, 11},
		{MaxTimestamp, true, MaxTimestamp},
		{MaxTimestamp, false, MaxTimestamp},
	}
	for _, st := range steps {
		if got := s.SetWatermark(st.ts); got != st.ok || s.Watermark() != st.want {
			t.Fatalf("SetWatermark(%d) = %v, watermark %d; want %v, %d", st.ts, got, s.Watermark(), st.ok, st.want)
		}
	}
}

// Q-007, Q-011, Q-013, §8.2, P-06: ReadFrom statuses, limits and append
// semantics.
func TestReadFrom(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 4, MaxReadBatch: 3})
	if got, info := s.ReadFrom(nil, Ref{}, 0); got != nil || info != (ReadInfo{Status: StatusNotFound}) {
		t.Fatalf("empty: %v %+v", got, info)
	}
	for i := 1; i <= 6; i++ {
		mustAppend(t, s, i, at(Timestamp(i)))
	}
	oldest := Ref{Gen: 1, Seq: 3}
	values := func(es []Entry[int]) []int {
		out := []int{}
		for _, e := range es {
			out = append(out, e.Value)
		}
		return out
	}
	cases := []struct {
		name  string
		from  Ref
		limit int
		vals  []int
		info  ReadInfo
	}{
		{"zero ref starts at oldest, capped by MaxReadBatch", Ref{}, 0, []int{3, 4, 5},
			ReadInfo{Status: StatusOK, N: 3, Truncated: true, LastRef: Ref{1, 5}, Oldest: oldest}},
		{"limit above MaxReadBatch is capped", Ref{1, 3}, 10, []int{3, 4, 5},
			ReadInfo{Status: StatusOK, N: 3, Truncated: true, LastRef: Ref{1, 5}, Oldest: oldest}},
		{"limit 1", Ref{1, 4}, 1, []int{4},
			ReadInfo{Status: StatusOK, N: 1, Truncated: true, LastRef: Ref{1, 4}, Oldest: oldest}},
		{"exact tail", Ref{1, 5}, 2, []int{5, 6},
			ReadInfo{Status: StatusOK, N: 2, LastRef: Ref{1, 6}, Oldest: oldest}},
		{"newest only", Ref{1, 6}, -1, []int{6},
			ReadInfo{Status: StatusOK, N: 1, LastRef: Ref{1, 6}, Oldest: oldest}},
		{"future", Ref{1, 7}, 0, []int{}, ReadInfo{Status: StatusNotFound, Oldest: oldest}},
		{"expired", Ref{1, 2}, 0, []int{}, ReadInfo{Status: StatusExpired, Evicted: true, Oldest: oldest}},
		{"earlier generation", Ref{0, 4}, 0, []int{}, ReadInfo{Status: StatusReset, Oldest: oldest}},
		{"later generation", Ref{2, 4}, 0, []int{}, ReadInfo{Status: StatusNotFound, Oldest: oldest}},
		{"zero seq ignores gen", Ref{Gen: 9}, 1, []int{3},
			ReadInfo{Status: StatusOK, N: 1, Truncated: true, LastRef: Ref{1, 3}, Oldest: oldest}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dst := []Entry[int]{{Value: 99}}
			got, info := s.ReadFrom(dst, tc.from, tc.limit)
			if info != tc.info {
				t.Fatalf("info = %+v, want %+v", info, tc.info)
			}
			if got[0].Value != 99 || !slices.Equal(values(got[1:]), tc.vals) {
				t.Fatalf("entries = %v, want [99 %v]", values(got), tc.vals)
			}
		})
	}

	t.Run("replay with LastRef", func(t *testing.T) {
		var all []int
		from := Ref{}
		for {
			got, info := s.ReadFrom(nil, from, 2)
			all = append(all, values(got)...)
			if !info.Truncated {
				break
			}
			from = Ref{Gen: info.LastRef.Gen, Seq: info.LastRef.Seq + 1}
		}
		if !slices.Equal(all, []int{3, 4, 5, 6}) {
			t.Fatalf("replay = %v", all)
		}
	})
}

// W-007: concurrent Appends receive unique, contiguous Seqs.
func TestAppend_Concurrent(t *testing.T) {
	const writers = 8
	per := iters(2000)
	s := mustNew(t, Config[int]{Capacity: writers * per})
	refs := make([][]Ref, writers)
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range per {
				res, err := s.Append(w*per+i, at(Timestamp(i+1)))
				if err != nil {
					t.Error(err)
					return
				}
				refs[w] = append(refs[w], res.Ref)
			}
		}()
	}
	wg.Wait()

	seen := make([]bool, writers*per+1)
	for w := range writers {
		for i, r := range refs[w] {
			if r.Gen != 1 || r.Seq == 0 || int(r.Seq) > writers*per || seen[r.Seq] {
				t.Fatalf("bad or duplicate ref %+v", r)
			}
			seen[r.Seq] = true
			if i > 0 && r.Seq <= refs[w][i-1].Seq {
				t.Fatalf("writer %d refs not increasing", w)
			}
			e, st := s.Get(r)
			if st != StatusOK || e.Value != w*per+i {
				t.Fatalf("Get(%+v) = %+v, %v", r, e, st)
			}
		}
	}
	if s.Len() != writers*per {
		t.Fatalf("Len = %d", s.Len())
	}
	mustInvariants(t, s)
}

// S12-01 (replaces T-014 part), E-007, W-004: a Clone panic on Append
// propagates to the caller, leaves no state change at all (counters
// included), and leaves the Series usable.
func TestAppend_ClonePanicNoStateChange(t *testing.T) {
	clone := func(v []int) []int {
		if len(v) > 0 && v[0] < 0 {
			panic("negative")
		}
		return slices.Clone(v)
	}
	s := mustNew(t, Config[[]int]{Capacity: 2, Clone: clone, CloneMode: CloneOnWrite})
	mustAppend(t, s, []int{1}, at(1))
	mustAppend(t, s, []int{2}, at(2))
	before := s.Stats()

	if v := panicValue(func() { s.Append([]int{-1}, at(3)) }); v != "negative" {
		t.Fatalf("panic value = %v, want the Clone panic", v)
	}
	if after := s.Stats(); !statsEqual(before, after) {
		t.Fatalf("state changed: %+v vs %+v", s.Stats(), before)
	}
	mustInvariants(t, s)
	if res := mustAppend(t, s, []int{3}, at(3)); res.Ref.Seq != 3 { // the lock was released
		t.Fatalf("next Seq = %d, want 3", res.Ref.Seq)
	}
}

// T-015, C-007: a Clone panic on a read propagates, and the mutex is free
// afterwards.
func TestRead_ClonePanicReleasesLock(t *testing.T) {
	clone := func(v int) int {
		if v < 0 {
			panic("read clone")
		}
		return v
	}
	s := mustNew(t, Config[int]{Capacity: 4, Clone: clone, CloneMode: CloneOnRead})
	bad := mustAppend(t, s, -1, at(1))
	mustPanic := func(name string, f func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatalf("%s did not panic", name)
			}
		}()
		f()
	}
	mustPanic("Get", func() { s.Get(bad.Ref) })
	mustPanic("Latest", func() { s.Latest() })
	mustPanic("ReadFrom", func() { s.ReadFrom(nil, Ref{}, 0) })

	done := make(chan struct{})
	go func() {
		defer close(done)
		good := mustAppend(t, s, 5, at(2))
		if e, st := s.Get(good.Ref); st != StatusOK || e.Value != 5 {
			t.Errorf("Get after panic = %+v, %v", e, st)
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("mutex still held after a read-path panic")
	}
}

// §11.5, T-006 (basic): Clone isolates producer and consumer buffers.
func TestClone_Isolation(t *testing.T) {
	clone := func(v []int) []int { return slices.Clone(v) }

	t.Run("OnWrite", func(t *testing.T) {
		s := mustNew(t, Config[[]int]{Capacity: 2, Clone: clone, CloneMode: CloneOnWrite})
		buf := []int{1, 2}
		res := mustAppend(t, s, buf, at(1))
		buf[0] = 100 // producer reuses its buffer
		if e, _ := s.Get(res.Ref); e.Value[0] != 1 {
			t.Fatalf("stored value changed with producer buffer: %v", e.Value)
		}
	})
	t.Run("OnRead", func(t *testing.T) {
		s := mustNew(t, Config[[]int]{Capacity: 2, Clone: clone, CloneMode: CloneOnRead})
		res := mustAppend(t, s, []int{1, 2}, at(1))
		e, _ := s.Get(res.Ref)
		e.Value[0] = 100 // consumer mutates its copy
		if e2, _ := s.Get(res.Ref); e2.Value[0] != 1 {
			t.Fatalf("stored value changed with consumer copy: %v", e2.Value)
		}
	})
	t.Run("nil Clone ignores mode", func(t *testing.T) {
		s := mustNew(t, Config[[]int]{Capacity: 2, CloneMode: CloneOnReadWrite})
		buf := []int{1}
		res := mustAppend(t, s, buf, at(1))
		buf[0] = 7
		if e, _ := s.Get(res.Ref); e.Value[0] != 7 {
			t.Fatal("value was cloned without a Clone func")
		}
	})
}

// T-026 (part), W-009: Append with TimeIndexNone and no indexes, and Seq reads
// with sufficient cap(dst), do not allocate.
func TestAllocs_SeqPath(t *testing.T) {
	skipAllocTest(t)
	type tick struct {
		price, qty float64
		side       int8
	}
	s := mustNew(t, Config[tick]{Capacity: 64, SourceSeqPolicy: SourceSeqFlag, LateTolerance: time.Millisecond})
	ts := Timestamp(1)
	src := uint64(1)
	appendOne := func() {
		ts++
		src++
		if _, err := s.Append(tick{price: 1, qty: 2}, InputMeta{EventTime: ts, SourceSeq: src, Flags: FlagGap}); err != nil {
			t.Fatal(err)
		}
	}
	for range 200 {
		appendOne()
	}
	dst := make([]Entry[tick], 0, 64)
	checks := []struct {
		name string
		fn   func()
	}{
		{"Append", appendOne},
		{"Append late", func() {
			if _, err := s.Append(tick{}, at(1)); err != nil {
				t.Fatal(err)
			}
		}},
		{"Append rejected", func() {
			if _, err := s.Append(tick{}, at(MinTimestamp)); err == nil {
				t.Fatal("expected error")
			}
		}},
		{"Get", func() { s.Get(s.NewestRef()) }},
		{"Latest", func() { s.Latest() }},
		{"ReadFrom", func() { dst, _ = s.ReadFrom(dst[:0], Ref{}, 64) }},
		{"Stats-free accessors", func() { _ = s.Len() + s.Cap(); _ = s.OldestRef() }},
		{"SetWatermark", func() { s.SetWatermark(ts) }},
	}
	for _, c := range checks {
		if n := allocsPerOp(200, c.fn); n != 0 {
			t.Errorf("%s: %.4f allocs/op, want 0", c.name, n)
		}
	}
}
