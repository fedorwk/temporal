package temporal

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

// orderedHarness runs an orderedTimeIndex outside a Series. It owns a ring
// and stores each inserted key's EventTime in it before the index sees the
// key, as Series.commit does before applyTime.
type orderedHarness struct {
	*orderedTimeIndex
	ring *metaRing
}

func newOrderedHarness(capacity int) *orderedHarness {
	r := newMetaRing(capacity)
	h := &orderedHarness{ring: &r}
	h.orderedTimeIndex = newOrderedTimeIndex(h.ring)
	return h
}

func (h *orderedHarness) store(k timeKey) {
	i := h.ring.slot(k.seq)
	h.ring.metas[i].eventTime = k.eventTime
	if debugAssert {
		h.ring.seqs[i] = k.seq
	}
}

func (h *orderedHarness) Set(k timeKey) {
	h.store(k)
	h.orderedTimeIndex.Set(k)
}

func (h *orderedHarness) Apply(del, ins []timeKey) {
	for _, k := range ins {
		h.store(k)
	}
	h.orderedTimeIndex.Apply(del, ins)
}

// exhaustiveCompare checks every primitive of x against ref for every
// timestamp in [lo, hi], the sentinels, and start keys around each.
func exhaustiveCompare(t *testing.T, x, ref timeIndex, lo, hi Timestamp, maxSeq Seq) {
	t.Helper()
	if x.Len() != ref.Len() || pr(x.Min()) != pr(ref.Min()) || pr(x.Max()) != pr(ref.Max()) {
		t.Fatalf("Len/Min/Max: %d %v %v, reference %d %v %v",
			x.Len(), pr(x.Min()), pr(x.Max()), ref.Len(), pr(ref.Min()), pr(ref.Max()))
	}
	stamps := []Timestamp{MinTimestamp, MinTimestamp + 1, MaxTimestamp - 1, MaxTimestamp}
	for ts := lo; ts <= hi; ts++ {
		stamps = append(stamps, ts)
	}
	for _, ts := range stamps {
		for _, q := range []struct {
			name      string
			got, want pointResult
		}{
			{"AsOf", pr(x.AsOf(ts)), pr(ref.AsOf(ts))},
			{"Before", pr(x.Before(ts)), pr(ref.Before(ts))},
			{"After", pr(x.After(ts)), pr(ref.After(ts))},
			{"AtOrAfter", pr(x.AtOrAfter(ts)), pr(ref.AtOrAfter(ts))},
		} {
			if q.got != q.want {
				t.Fatalf("%s(%d) = %+v, reference %+v", q.name, ts, q.got, q.want)
			}
		}
		for _, seq := range []Seq{0, 1, maxSeq / 2, maxSeq, maxSeq + 1, seqMax} {
			start := timeKey{eventTime: ts, seq: seq}
			for _, limit := range []int{0, 1, 2, 100} {
				for _, to := range []Timestamp{ts, ts + 1, hi, MaxTimestamp} {
					if a, b := ascend(x, start, to, limit), ascend(ref, start, to, limit); !a.equal(b) {
						t.Fatalf("Ascend(%v, %d, %d) = %+v, reference %+v", start, to, limit, a, b)
					}
				}
				if a, b := descend(x, start, limit), descend(ref, start, limit); !a.equal(b) {
					t.Fatalf("Descend(%v, %d) = %+v, reference %+v", start, limit, a, b)
				}
			}
		}
	}
}

// T-021 for TimeIndexOrdered: an empty index, ties, FIFO Apply, Delete,
// Clear and sentinel-adjacent timestamps, checked exhaustively against the
// slice reference.
func TestOrderedTimeIndex_Primitives(t *testing.T) {
	h, ref := newOrderedHarness(8), &sliceTimeIndex{}
	exhaustiveCompare(t, h, ref, -2, 3, 1)

	keys := []timeKey{{5, 1}, {10, 2}, {10, 3}, {10, 4}, {20, 5}}
	for _, k := range keys {
		h.Set(k)
		ref.Set(k)
	}
	exhaustiveCompare(t, h, ref, 3, 22, 6)
	if got := pr(h.AsOf(10)); got != pr(timeKey{10, 4}, true) {
		t.Fatalf("AsOf(10) = %+v, want the last of the ties", got)
	}

	// A batch that evicts the two oldest and appends three, with a tie at the
	// boundary between old and new keys.
	del, ins := []timeKey{{5, 1}, {10, 2}}, []timeKey{{20, 6}, {21, 7}, {21, 8}}
	h.Apply(del, ins)
	ref.Apply(del, ins)
	exhaustiveCompare(t, h, ref, 8, 23, 9)

	h.Delete(timeKey{10, 3})
	ref.Delete(timeKey{10, 3})
	exhaustiveCompare(t, h, ref, 8, 23, 9)

	h.Clear()
	ref.Clear()
	exhaustiveCompare(t, h, ref, -2, 3, 1)

	// After Clear, the next generation may start at any time and any Seq.
	lo, hi := MinTimestamp+1, MaxTimestamp-1
	for i, ts := range []Timestamp{lo, lo, 0, hi} {
		k := timeKey{eventTime: ts, seq: Seq(i + 1)}
		h.Set(k)
		ref.Set(k)
	}
	exhaustiveCompare(t, h, ref, -2, 2, 5)
}

// T-021, TI-011: randomized differential against the slice reference with
// in-order streams: FIFO eviction windows equal to the ring capacity (so new
// keys overwrite evicted slots), batches, ties, single Delete/Set and Clear.
func TestOrderedTimeIndex_Differential(t *testing.T) {
	r, _ := newRand(t)
	for run := range iters(60) {
		window := 1 + r.IntN(40)
		if run%4 == 0 {
			window = 1 << r.IntN(6) // power-of-two capacities use the mask path
		}
		h, ref := newOrderedHarness(window), &sliceTimeIndex{}
		impls := []timeIndex{h, ref}
		o := &indexOps{r: r, span: 60, ordered: true, now: Timestamp(r.IntN(60) - 30)}

		for step := range 300 {
			var del, ins []timeKey
			switch op := r.IntN(100); {
			case op < 70:
				for range 1 + r.IntN(window) { // a batch never exceeds the capacity
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
			case op < 85 && len(o.live) > 0:
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
				o.now = Timestamp(r.IntN(60) - 30) // a new generation may start lower
				for _, x := range impls {
					x.Clear()
				}
			}
			compareIndexes(t, r, []string{"ordered"}, impls, o, fmt.Sprintf("run %d step %d (window %d)", run, step, window))
		}
	}
}

// Debug builds catch a caller that breaks the ordering contract.
func TestOrderedTimeIndex_DebugAsserts(t *testing.T) {
	if !debugAssert {
		t.Skip("assertions exist only in temporaldebug builds")
	}
	cases := map[string]func(h *orderedHarness){
		"EventTime decreases": func(h *orderedHarness) { h.Set(timeKey{10, 1}); h.Set(timeKey{9, 2}) },
		"Seq gap":             func(h *orderedHarness) { h.Set(timeKey{10, 1}); h.Set(timeKey{10, 3}) },
		"delete not oldest":   func(h *orderedHarness) { h.Set(timeKey{1, 1}); h.Set(timeKey{2, 2}); h.Delete(timeKey{2, 2}) },
		"delete from empty":   func(h *orderedHarness) { h.Delete(timeKey{1, 1}) },
	}
	for name, f := range cases {
		if panicValue(func() { f(newOrderedHarness(4)) }) == nil {
			t.Errorf("%s: no assertion", name)
		}
	}
}

// TI-012: lookups, scans and Apply on TimeIndexOrdered never allocate.
func TestOrderedTimeIndex_Allocs(t *testing.T) {
	skipAllocTest(t)
	const window = 4096
	h := newOrderedHarness(window)
	r := rand.New(rand.NewPCG(1, 2))
	var seq Seq
	var now Timestamp
	del, ins := make([]timeKey, 0, 1), make([]timeKey, 0, 1)
	oldest := Seq(1)
	step := func() {
		del, ins = del[:0], ins[:0]
		seq++
		if h.Len() == window {
			del = append(del, timeKey{seq: oldest})
			oldest++
		}
		now += Timestamp(r.IntN(3))
		ins = append(ins, timeKey{eventTime: now, seq: seq})
		h.Apply(del, ins)
	}
	for range 3 * window {
		step()
	}
	buf := make([]timeKey, 0, 256)
	mid, _ := h.AsOf(now - window/2)
	checks := []struct {
		name string
		fn   func()
	}{
		{"Apply", step},
		{"AsOf", func() { h.AsOf(mid.eventTime) }},
		{"Before", func() { h.Before(mid.eventTime) }},
		{"After", func() { h.After(mid.eventTime) }},
		{"AtOrAfter", func() { h.AtOrAfter(mid.eventTime) }},
		{"Min/Max", func() { h.Min(); h.Max() }},
		{"Ascend", func() { buf, _ = h.Ascend(buf[:0], timeKey{mid.eventTime, 0}, MaxTimestamp, 200) }},
		{"Descend", func() { buf, _ = h.Descend(buf[:0], timeKey{mid.eventTime, seqMax}, 200) }},
	}
	for _, c := range checks {
		if n := allocsPerOp(500, c.fn); n != 0 {
			t.Errorf("%s: %.4f allocs/op, want 0", c.name, n)
		}
	}
}
