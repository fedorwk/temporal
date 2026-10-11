package temporal

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
)

// timeIndexFactories lists every implementation the common suite runs against
// (T-021, TI-011): each production implementation plus the slice reference.
var timeIndexFactories = []struct {
	name string
	make func() timeIndex
}{
	{"btree", func() timeIndex { return newBTreeTimeIndex() }},
	{"slice", func() timeIndex { return &sliceTimeIndex{} }},
}

type pointResult struct {
	k  timeKey
	ok bool
}

func pr(k timeKey, ok bool) pointResult { return pointResult{k, ok} }

type scanResult struct {
	keys []timeKey
	more bool
}

func ascend(x timeIndex, start timeKey, to Timestamp, limit int) scanResult {
	keys, more := x.Ascend(nil, start, to, limit)
	return scanResult{keys, more}
}

func descend(x timeIndex, start timeKey, limit int) scanResult {
	keys, more := x.Descend(nil, start, limit)
	return scanResult{keys, more}
}

func (a scanResult) equal(b scanResult) bool {
	return a.more == b.more && slices.Equal(a.keys, b.keys)
}

// T-021: an empty index reports absence everywhere.
func TestTimeIndex_Empty(t *testing.T) {
	for _, f := range timeIndexFactories {
		x := f.make()
		for _, ts := range []Timestamp{MinTimestamp, -1, 0, 1, MaxTimestamp} {
			for name, got := range map[string]pointResult{
				"AsOf": pr(x.AsOf(ts)), "Before": pr(x.Before(ts)),
				"After": pr(x.After(ts)), "AtOrAfter": pr(x.AtOrAfter(ts)),
			} {
				if got.ok {
					t.Errorf("%s: %s(%d) on empty index = %v", f.name, name, ts, got.k)
				}
			}
		}
		if _, ok := x.Min(); ok {
			t.Errorf("%s: Min on empty index", f.name)
		}
		if _, ok := x.Max(); ok {
			t.Errorf("%s: Max on empty index", f.name)
		}
		if r := ascend(x, timeKey{MinTimestamp, 0}, MaxTimestamp, 10); len(r.keys) != 0 || r.more {
			t.Errorf("%s: Ascend on empty index = %+v", f.name, r)
		}
		if r := descend(x, timeKey{MaxTimestamp, seqMax}, 10); len(r.keys) != 0 || r.more {
			t.Errorf("%s: Descend on empty index = %+v", f.name, r)
		}
		if x.Len() != 0 {
			t.Errorf("%s: Len = %d", f.name, x.Len())
		}
	}
}

// T-021, §6.5, E-004: hand-computed expectations for ties and every primitive.
func TestTimeIndex_TiesAndPrimitives(t *testing.T) {
	keys := []timeKey{{10, 1}, {10, 2}, {10, 3}, {5, 4}, {20, 5}}
	for _, f := range timeIndexFactories {
		x := f.make()
		for _, k := range keys {
			x.Set(k)
		}
		points := []struct {
			name string
			got  pointResult
			want pointResult
		}{
			{"AsOf(10)", pr(x.AsOf(10)), pr(timeKey{10, 3}, true)},
			{"AsOf(11)", pr(x.AsOf(11)), pr(timeKey{10, 3}, true)},
			{"AsOf(9)", pr(x.AsOf(9)), pr(timeKey{5, 4}, true)},
			{"AsOf(4)", pr(x.AsOf(4)), pr(timeKey{}, false)},
			{"AsOf(Max)", pr(x.AsOf(MaxTimestamp)), pr(timeKey{20, 5}, true)},
			{"Before(10)", pr(x.Before(10)), pr(timeKey{5, 4}, true)},
			{"Before(5)", pr(x.Before(5)), pr(timeKey{}, false)},
			{"Before(21)", pr(x.Before(21)), pr(timeKey{20, 5}, true)},
			{"After(10)", pr(x.After(10)), pr(timeKey{20, 5}, true)},
			{"After(9)", pr(x.After(9)), pr(timeKey{10, 1}, true)},
			{"After(20)", pr(x.After(20)), pr(timeKey{}, false)},
			{"After(Min)", pr(x.After(MinTimestamp)), pr(timeKey{5, 4}, true)},
			{"AtOrAfter(10)", pr(x.AtOrAfter(10)), pr(timeKey{10, 1}, true)},
			{"AtOrAfter(6)", pr(x.AtOrAfter(6)), pr(timeKey{10, 1}, true)},
			{"AtOrAfter(21)", pr(x.AtOrAfter(21)), pr(timeKey{}, false)},
			{"Min", pr(x.Min()), pr(timeKey{5, 4}, true)},
			{"Max", pr(x.Max()), pr(timeKey{20, 5}, true)},
		}
		for _, p := range points {
			if p.got != p.want {
				t.Errorf("%s: %s = %+v, want %+v", f.name, p.name, p.got, p.want)
			}
		}

		scans := []struct {
			name string
			got  scanResult
			want scanResult
		}{
			{"AtAll(10) limit 2", ascend(x, timeKey{10, 0}, 11, 2), scanResult{[]timeKey{{10, 1}, {10, 2}}, true}},
			{"AtAll(10) exact", ascend(x, timeKey{10, 0}, 11, 3), scanResult{[]timeKey{{10, 1}, {10, 2}, {10, 3}}, false}},
			{"AtAll(10) over", ascend(x, timeKey{10, 0}, 11, 9), scanResult{[]timeKey{{10, 1}, {10, 2}, {10, 3}}, false}},
			{"limit 0 with matches", ascend(x, timeKey{10, 0}, 11, 0), scanResult{nil, true}},
			{"limit 0 without matches", ascend(x, timeKey{11, 0}, 20, 0), scanResult{nil, false}},
			{"limit 1", ascend(x, timeKey{MinTimestamp, 0}, MaxTimestamp, 1), scanResult{[]timeKey{{5, 4}}, true}},
			{"continue after tie", ascend(x, timeKey{10, 2}, MaxTimestamp, 9), scanResult{[]timeKey{{10, 2}, {10, 3}, {20, 5}}, false}},
			{"NextN(10)", ascend(x, timeKey{10, seqMax}, MaxTimestamp, 9), scanResult{[]timeKey{{20, 5}}, false}},
			{"empty range", ascend(x, timeKey{11, 0}, 20, 9), scanResult{nil, false}},
			{"PrevN(10) 2", descend(x, timeKey{10, seqMax}, 2), scanResult{[]timeKey{{10, 3}, {10, 2}}, true}},
			{"Descend from tie", descend(x, timeKey{10, 2}, 9), scanResult{[]timeKey{{10, 2}, {10, 1}, {5, 4}}, false}},
			{"Descend below all", descend(x, timeKey{5, 3}, 9), scanResult{nil, false}},
			{"Descend limit 0", descend(x, timeKey{MaxTimestamp, seqMax}, 0), scanResult{nil, true}},
			{"Descend all", descend(x, timeKey{MaxTimestamp, seqMax}, 5),
				scanResult{[]timeKey{{20, 5}, {10, 3}, {10, 2}, {10, 1}, {5, 4}}, false}},
		}
		for _, sc := range scans {
			if !sc.got.equal(sc.want) {
				t.Errorf("%s: %s = %+v, want %+v", f.name, sc.name, sc.got, sc.want)
			}
		}

		// dst append semantics: the prefix is preserved.
		dst := []timeKey{{-1, 99}}
		dst, _ = x.Ascend(dst, timeKey{20, 0}, MaxTimestamp, 5)
		if !slices.Equal(dst, []timeKey{{-1, 99}, {20, 5}}) {
			t.Errorf("%s: Ascend did not append: %v", f.name, dst)
		}
		dst, _ = x.Descend(dst[:1], timeKey{5, seqMax}, 5)
		if !slices.Equal(dst, []timeKey{{-1, 99}, {5, 4}}) {
			t.Errorf("%s: Descend did not append: %v", f.name, dst)
		}

		x.Apply([]timeKey{{10, 2}, {5, 4}}, []timeKey{{7, 6}, {10, 7}})
		want := []timeKey{{7, 6}, {10, 1}, {10, 3}, {10, 7}, {20, 5}}
		if r := ascend(x, timeKey{MinTimestamp, 0}, MaxTimestamp, 99); !slices.Equal(r.keys, want) || x.Len() != 5 {
			t.Errorf("%s: after Apply = %v (len %d), want %v", f.name, r.keys, x.Len(), want)
		}
		x.Delete(timeKey{20, 5})
		if got := pr(x.Max()); got != pr(timeKey{10, 7}, true) {
			t.Errorf("%s: Max after Delete = %+v", f.name, got)
		}
		x.Clear()
		if x.Len() != 0 {
			t.Errorf("%s: Len after Clear = %d", f.name, x.Len())
		}
		x.Set(timeKey{1, 1}) // usable after Clear
		if got := pr(x.Min()); got != pr(timeKey{1, 1}, true) {
			t.Errorf("%s: Min after Clear+Set = %+v", f.name, got)
		}
	}
}

// T-021: timestamps adjacent to the sentinels.
func TestTimeIndex_SentinelAdjacent(t *testing.T) {
	lo, hi := MinTimestamp+1, MaxTimestamp-1
	for _, f := range timeIndexFactories {
		x := f.make()
		x.Set(timeKey{hi, 1})
		x.Set(timeKey{lo, 2})
		x.Set(timeKey{0, 3})
		checks := []struct {
			name      string
			got, want pointResult
		}{
			{"AsOf(lo)", pr(x.AsOf(lo)), pr(timeKey{lo, 2}, true)},
			{"Before(lo)", pr(x.Before(lo)), pr(timeKey{}, false)},
			{"AtOrAfter(hi)", pr(x.AtOrAfter(hi)), pr(timeKey{hi, 1}, true)},
			{"After(hi)", pr(x.After(hi)), pr(timeKey{}, false)},
			{"After(Min)", pr(x.After(MinTimestamp)), pr(timeKey{lo, 2}, true)},
			{"Before(Max)", pr(x.Before(MaxTimestamp)), pr(timeKey{hi, 1}, true)},
			{"AsOf(Min)", pr(x.AsOf(MinTimestamp)), pr(timeKey{}, false)},
			{"AtOrAfter(Max)", pr(x.AtOrAfter(MaxTimestamp)), pr(timeKey{}, false)},
		}
		for _, c := range checks {
			if c.got != c.want {
				t.Errorf("%s: %s = %+v, want %+v", f.name, c.name, c.got, c.want)
			}
		}
		all := ascend(x, timeKey{MinTimestamp, 0}, MaxTimestamp, 10)
		if !slices.Equal(all.keys, []timeKey{{lo, 2}, {0, 3}, {hi, 1}}) || all.more {
			t.Errorf("%s: full ascend = %+v", f.name, all)
		}
		if r := ascend(x, timeKey{hi, 0}, hi, 10); len(r.keys) != 0 {
			t.Errorf("%s: bound is not exclusive: %+v", f.name, r)
		}
	}
}

// T-021: the noTimeIndex placeholder reports absence and ignores mutations.
func TestTimeIndex_None(t *testing.T) {
	var x timeIndex = noTimeIndex{}
	x.Set(timeKey{1, 1})
	x.Apply(nil, []timeKey{{2, 2}})
	if x.Len() != 0 {
		t.Fatal("noTimeIndex stored a key")
	}
	if _, ok := x.AsOf(5); ok {
		t.Fatal("noTimeIndex AsOf found a key")
	}
	if keys, more := x.Ascend(nil, timeKey{}, MaxTimestamp, 5); len(keys) != 0 || more {
		t.Fatal("noTimeIndex Ascend returned keys")
	}
	if newTimeIndex(TimeIndexNone) != nil {
		t.Fatal("TimeIndexNone must be a nil timeIndex (TI-008)")
	}
}

// indexOps drives every implementation through the same operation stream.
type indexOps struct {
	r       *rand.Rand
	nextSeq Seq
	live    []timeKey // insertion (Seq) order, like ring retention
	span    int
}

func (o *indexOps) newKey() timeKey {
	o.nextSeq++
	return timeKey{eventTime: Timestamp(o.r.IntN(o.span) - o.span/2), seq: o.nextSeq}
}

// T-021, TI-011: randomized differential. Every implementation receives the
// same mutations (FIFO eviction windows, late inserts, batches, Clear) and must
// agree on every primitive after every operation.
func TestTimeIndex_Differential(t *testing.T) {
	r, _ := newRand(t)
	for run := range iters(40) {
		impls := make([]timeIndex, len(timeIndexFactories))
		for i, f := range timeIndexFactories {
			impls[i] = f.make()
		}
		o := &indexOps{r: r, span: 1 + r.IntN(60)}
		window := 1 + r.IntN(40)

		for step := range 300 {
			var del, ins []timeKey
			switch op := r.IntN(100); {
			case op < 70: // append-with-eviction batch, as the Series commits it
				for range 1 + r.IntN(min(4, window)) { // W-003: a batch never evicts its own items
					if len(o.live) == window {
						del = append(del, o.live[0])
						o.live = o.live[1:]
					}
					k := o.newKey()
					ins = append(ins, k)
					o.live = append(o.live, k)
				}
				for _, x := range impls {
					x.Apply(del, ins)
				}
			case op < 85 && len(o.live) > 0: // single Delete of the oldest, then Set
				old := o.live[0]
				o.live = o.live[1:]
				k := o.newKey()
				o.live = append(o.live, k)
				for _, x := range impls {
					x.Delete(old)
					x.Set(k)
				}
			case op < 88:
				o.live = nil
				for _, x := range impls {
					x.Clear()
				}
			}
			compareIndexes(t, r, impls, o, fmt.Sprintf("run %d step %d", run, step))
		}
	}
}

func compareIndexes(t *testing.T, r *rand.Rand, impls []timeIndex, o *indexOps, where string) {
	t.Helper()
	ref := impls[len(impls)-1] // the slice reference
	randTS := func() Timestamp {
		switch r.IntN(10) {
		case 0:
			return MinTimestamp
		case 1:
			return MaxTimestamp
		default:
			return Timestamp(r.IntN(o.span+4) - o.span/2 - 2)
		}
	}
	randKey := func() timeKey {
		seqs := []Seq{0, 1, o.nextSeq / 2, o.nextSeq, o.nextSeq + 1, seqMax}
		return timeKey{eventTime: randTS(), seq: seqs[r.IntN(len(seqs))]}
	}
	for i, x := range impls[:len(impls)-1] {
		name := timeIndexFactories[i].name
		if x.Len() != ref.Len() || x.Len() != len(o.live) {
			t.Fatalf("%s %s: Len %d, reference %d, live %d", where, name, x.Len(), ref.Len(), len(o.live))
		}
		if pr(x.Min()) != pr(ref.Min()) || pr(x.Max()) != pr(ref.Max()) {
			t.Fatalf("%s %s: Min/Max differ", where, name)
		}
		for range 4 {
			ts := randTS()
			if pr(x.AsOf(ts)) != pr(ref.AsOf(ts)) || pr(x.Before(ts)) != pr(ref.Before(ts)) ||
				pr(x.After(ts)) != pr(ref.After(ts)) || pr(x.AtOrAfter(ts)) != pr(ref.AtOrAfter(ts)) {
				t.Fatalf("%s %s: point primitive differs at ts %d", where, name, ts)
			}
			start, to, limit := randKey(), randTS(), r.IntN(8)
			if a, b := ascend(x, start, to, limit), ascend(ref, start, to, limit); !a.equal(b) {
				t.Fatalf("%s %s: Ascend(%v, %d, %d) = %+v, reference %+v", where, name, start, to, limit, a, b)
			}
			if a, b := descend(x, start, limit), descend(ref, start, limit); !a.equal(b) {
				t.Fatalf("%s %s: Descend(%v, %d) = %+v, reference %+v", where, name, start, limit, a, b)
			}
		}
	}
}

// TI-012, §22.2 (early check for S4/S5): steady-state B-tree lookups and
// scans into a reused buffer do not allocate; Apply allocates only on node
// growth.
func TestTimeIndex_BTreeAllocs(t *testing.T) {
	skipAllocTest(t)
	x := newBTreeTimeIndex()
	const window = 4096
	var seq Seq
	r := rand.New(rand.NewPCG(1, 2))
	live := make([]timeKey, window) // circular buffer of retained keys
	n := 0
	del, ins := make([]timeKey, 0, 1), make([]timeKey, 0, 1)
	step := func() {
		del, ins = del[:0], ins[:0]
		seq++
		slot := int(seq-1) % window
		if n == window {
			del = append(del, live[slot])
		} else {
			n++
		}
		k := timeKey{eventTime: Timestamp(seq) - Timestamp(r.IntN(50)), seq: seq}
		live[slot] = k
		ins = append(ins, k)
		x.Apply(del, ins)
	}
	for range 3 * window {
		step()
	}
	buf := make([]timeKey, 0, 256)
	mid := Timestamp(seq - window/2)
	checks := []struct {
		name string
		fn   func()
	}{
		{"AsOf", func() { x.AsOf(mid) }},
		{"Before", func() { x.Before(mid) }},
		{"After", func() { x.After(mid) }},
		{"AtOrAfter", func() { x.AtOrAfter(mid) }},
		{"Min/Max", func() { x.Min(); x.Max() }},
		{"Ascend", func() { buf, _ = x.Ascend(buf[:0], timeKey{mid, 0}, MaxTimestamp, 200) }},
		{"Descend", func() { buf, _ = x.Descend(buf[:0], timeKey{mid, seqMax}, 200) }},
	}
	for _, c := range checks {
		if n := testing.AllocsPerRun(500, c.fn); n != 0 {
			t.Errorf("%s: %.2f allocs/op, want 0", c.name, n)
		}
	}
	t.Logf("B-tree Apply (1 delete + 1 insert) steady state: %.3f allocs/op", testing.AllocsPerRun(5000, step))
}
