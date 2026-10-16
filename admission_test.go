package temporal

import (
	"errors"
	"testing"
)

// The admission rules (P-03 steps 1-4 and the state they advance), tested
// without a Series.

func newAdmission(capacity int, tol Timestamp, late LatePolicy, src SourceSeqPolicy) admission {
	a := admission{rules: admissionRules{capacity: capacity, lateTolerance: tol, latePolicy: late, sourceSeqPolicy: src}}
	a.reset()
	return a
}

// advance admits m as the write path does: push, then observe.
func advance(a *admission, m *InputMeta) (seq, evicted Seq) {
	seq, evicted = a.push()
	a.observe(m)
	return seq, evicted
}

// admit checks m and admits it, failing the test if check rejects it.
func admit(tb testing.TB, a *admission, m InputMeta) {
	tb.Helper()
	if _, err := a.check(&m); err != nil {
		tb.Fatalf("check %+v: %v", m, err)
	}
	advance(a, &m)
}

func TestAdmission_BoundsAndEviction(t *testing.T) {
	a := newAdmission(3, 0, LateAccept, SourceSeqIgnore)
	for i := 1; i <= 5; i++ {
		wantBelow := a.b.oldest
		if i > 3 {
			wantBelow++ // full: admitting evicts the oldest
		}
		before := a
		m := at(Timestamp(i))
		if _, err := a.check(&m); err != nil || a != before {
			t.Fatalf("item %d: check %v, changed state %v", i, err, a != before)
		}
		if got := a.evictBelow(); got != wantBelow {
			t.Fatalf("item %d: evictBelow %d, want %d", i, got, wantBelow)
		}
		if a.nextSeq() != Seq(i) {
			t.Fatalf("item %d: next seq %d", i, a.nextSeq())
		}
		seq, evicted := advance(&a, &m)
		if seq != Seq(i) {
			t.Fatalf("item %d: seq %d", i, seq)
		}
		if want := Seq(max(i-3, 0)); evicted != want {
			t.Fatalf("item %d: evicted %d, want %d", i, evicted, want)
		}
	}
	if a.b.oldest != 3 || a.b.newest != 5 || a.maxObserved != 5 {
		t.Fatalf("state %+v", a)
	}
	a.reset()
	if a.b.nextSeq() != 1 || a.observed() || a.rules.capacity != 3 {
		t.Fatalf("reset: %+v", a)
	}
}

func TestAdmission_Check(t *testing.T) {
	type step struct {
		m     InputMeta
		flags Flags
		err   error
		apply bool // advance after a successful check
	}
	cases := []struct {
		name  string
		a     admission
		steps []step
	}{
		{"sentinels and flag bits", newAdmission(4, 0, LateAccept, SourceSeqIgnore), []step{
			{m: InputMeta{EventTime: MinTimestamp}, err: errInvalidEventTime},
			{m: InputMeta{EventTime: MaxTimestamp}, err: errInvalidEventTime},
			{m: InputMeta{EventTime: 1, Flags: FlagLate}, err: errSeriesFlags},
		}},
		{"late accept with tolerance", newAdmission(4, 5, LateAccept, SourceSeqIgnore), []step{
			{m: at(100), apply: true},
			{m: at(95), apply: true}, // within tolerance
			{m: at(94), flags: FlagLate, apply: true},
			{m: at(200), apply: true},
			{m: at(100), flags: FlagLate},
		}},
		{"late reject", newAdmission(4, 0, LateReject, SourceSeqIgnore), []step{
			{m: at(10), apply: true},
			{m: at(10), apply: true}, // ties are not late
			{m: at(9), err: ErrLate},
		}},
		{"nothing observed is never late", newAdmission(4, 0, LateReject, SourceSeqIgnore), []step{
			{m: at(MinTimestamp + 1), apply: true},
		}},
		{"late threshold saturates", newAdmission(4, MaxTimestamp, LateReject, SourceSeqIgnore), []step{
			{m: at(MinTimestamp + 1), apply: true},
			{m: at(MinTimestamp + 1)},
		}},
		{"source seq highest per epoch", newAdmission(8, 0, LateAccept, SourceSeqFlag), []step{
			{m: InputMeta{EventTime: 1, SourceSeq: 10}, apply: true},
			{m: InputMeta{EventTime: 2, SourceSeq: 8}, flags: FlagSourceSeqNotIncreasing, apply: true},
			{m: InputMeta{EventTime: 3, SourceSeq: 9}, flags: FlagSourceSeqNotIncreasing, apply: true}, // S2-01
			{m: InputMeta{EventTime: 4, SourceSeq: 0}, apply: true},                                    // not provided
			{m: InputMeta{EventTime: 5, SourceSeq: 1, SourceEpoch: 2}, apply: true},                    // new epoch
			{m: InputMeta{EventTime: 6, SourceSeq: 1, SourceEpoch: 2}, flags: FlagSourceSeqNotIncreasing},
		}},
		{"source seq first in epoch 0", newAdmission(8, 0, LateAccept, SourceSeqReject), []step{
			{m: InputMeta{EventTime: 1, SourceSeq: 1}, apply: true},
			{m: InputMeta{EventTime: 2, SourceSeq: 1}, err: ErrSourceSeqNotIncreasing},
		}},
		{"source seq ignored", newAdmission(8, 0, LateAccept, SourceSeqIgnore), []step{
			{m: InputMeta{EventTime: 1, SourceSeq: 3}, apply: true},
			{m: InputMeta{EventTime: 2, SourceSeq: 1}},
		}},
		{"unapplied check moves nothing", newAdmission(8, 0, LateAccept, SourceSeqFlag), []step{
			{m: InputMeta{EventTime: 10, SourceSeq: 5}},             // checked but dropped
			{m: InputMeta{EventTime: 9, SourceSeq: 4}, apply: true}, // neither late nor behind
			{m: InputMeta{EventTime: 8, SourceSeq: 4}, flags: FlagLate | FlagSourceSeqNotIncreasing},
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			a := c.a
			for i, st := range c.steps {
				before := a
				flags, err := a.check(&st.m)
				if !errors.Is(err, st.err) || flags != st.flags {
					t.Fatalf("step %d: flags %v err %v, want %v %v", i, flags, err, st.flags, st.err)
				}
				if a != before {
					t.Fatalf("step %d: check changed the state", i)
				}
				if st.apply {
					advance(&a, &st.m)
				}
			}
		})
	}
}

func TestAdmission_SeqExhausted(t *testing.T) {
	a := newAdmission(4, 0, LateAccept, SourceSeqIgnore)
	a.b.newest = seqMax - 2 // as testSetNewest does
	admit(t, &a, at(1))
	m := at(1)
	if _, err := a.check(&m); !errors.Is(err, ErrSeqExhausted) {
		t.Fatalf("got %v, want ErrSeqExhausted", err)
	}
}

// A copy advances independently of the original, which is what AppendBatch's
// prepare phase relies on (P-02).
func TestAdmission_CopyIsIndependent(t *testing.T) {
	a := newAdmission(2, 0, LateReject, SourceSeqReject)
	admit(t, &a, InputMeta{EventTime: 5, SourceSeq: 1})
	before := a
	sim := a
	for i := range 3 {
		admit(t, &sim, InputMeta{EventTime: Timestamp(10 + i), SourceSeq: uint64(2 + i)})
	}
	if a != before {
		t.Fatalf("advancing a copy changed the original: %+v", a)
	}
	if sim.b.oldest != 3 || sim.maxObserved != 12 || sim.srcSeq.last != 4 {
		t.Fatalf("copy: %+v", sim)
	}
}
