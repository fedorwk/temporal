package temporal

import (
	"strings"
	"testing"
)

// T-010, §14.1, §23.3: CheckInvariants detects corruption of every structure
// it rebuilds, not only that a healthy Series passes.
func TestCheckInvariants_DetectsCorruption(t *testing.T) {
	type fixture struct {
		s  *Series[int]
		u  *UniqueIndex[int, int]
		mi *MultiIndex[int, int]
	}
	build := func(t *testing.T) fixture {
		u := NewUniqueIndex("u", func(e *Entry[int]) (int, bool) { return e.Value, true }, ConflictReject)
		mi := NewMultiIndex("m", func(e *Entry[int]) (int, bool) { return e.Value % 2, true })
		s := mustNew(t, Config[int]{Capacity: 4, TimeIndex: TimeIndexBTree, Indexes: []Index[int]{u, mi}})
		for i := 1; i <= 6; i++ { // retained: Seq 3..6, values 3..6; envelope [1, 2]
			mustAppend(t, s, i, at(Timestamp(i)))
		}
		mustInvariants(t, s)
		return fixture{s, u, mi}
	}
	cases := []struct {
		name    string
		corrupt func(f fixture)
		want    string
	}{
		{"bounds beyond capacity", func(f fixture) { f.s.b.oldest = 1 }, "capacity"},
		{"bounds inverted", func(f fixture) { f.s.b.oldest = 9 }, "oldest"},
		{"reserved newest", func(f fixture) { f.s.b.newest = seqMax }, "reserved"},
		{"envelope inverted", func(f fixture) { f.s.env.min = 5 }, "envelope"},
		{"envelope without flag", func(f fixture) { f.s.env = envelope{min: 1} }, "envelope"},
		{"max observed too low", func(f fixture) { f.s.maxObserved = 4 }, "observed"},
		{"stored sentinel", func(f fixture) { f.s.ring.metas[f.s.ring.slot(4)].eventTime = MaxTimestamp }, "sentinel"},
		{"reserved flag bit", func(f fixture) { f.s.ring.metas[f.s.ring.slot(4)].flags |= 1 << 20 }, "flag"},
		{"time index missing key", func(f fixture) { f.s.time.Delete(timeKey{eventTime: 4, seq: 4}) }, "time index"},
		{"time index stale EventTime", func(f fixture) { f.s.ring.metas[f.s.ring.slot(5)].eventTime = 2 }, "time index"},
		{"time index extra key", func(f fixture) { f.s.time.Set(timeKey{eventTime: 1, seq: 1}) }, "time index"},
		{"unique mapping to wrong seq", func(f fixture) { f.u.m[4] = 5 }, "unique"},
		{"unique extra mapping", func(f fixture) { f.u.m[99] = 4 }, "unique"},
		{"unique indexed bit on empty slot", func(f fixture) {
			f.s.Clear()
			f.u.indexed.set(0)
		}, "non-retained"},
		{"multi indexed bit on empty slot", func(f fixture) {
			f.s.Clear()
			f.mi.indexed.set(2)
		}, "non-retained"},
		{"multi wrong postings", func(f fixture) { f.mi.m[0].seqs[f.mi.m[0].head] = 3 }, "multi"},
		{"multi extra key", func(f fixture) { f.mi.m[7] = &postings{seqs: []Seq{5}} }, "multi"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := build(t)
			c.corrupt(f)
			err := f.s.CheckInvariants()
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("CheckInvariants = %v, want an error mentioning %q", err, c.want)
			}
		})
	}
}

// Q-010: an unknown Order is a malformed request.
func TestMultiIndex_InvalidOrder(t *testing.T) {
	mi := NewMultiIndex("m", func(e *Entry[int]) (int, bool) { return 0, true })
	s := mustNew(t, Config[int]{Capacity: 2, Indexes: []Index[int]{mi}})
	mustAppend(t, s, 1, at(1))
	if got, info := mi.Get(nil, 0, 0, Descending+1); info.Status != StatusInvalid || len(got) != 0 {
		t.Fatalf("Get with unknown Order = %v %+v", got, info)
	}
}
