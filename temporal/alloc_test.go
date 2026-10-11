package temporal

import (
	"context"
	"testing"
)

// T-026, §22.2: the consolidated allocation acceptance table. Every listed
// operation must perform exactly zero heap allocations in steady state
// (counted exactly, see allocsPerOp). Per-feature allocation tests live next
// to their features; this table is the single place that mirrors §22.2.
func TestAllocAcceptance(t *testing.T) {
	skipAllocTest(t)
	u := NewUniqueIndex("id", func(e *Entry[tick]) (uint64, bool) { return e.Value.ID, true }, ConflictReject)
	mi := NewMultiIndex("side", func(e *Entry[tick]) (uint8, bool) { return e.Value.Side, true })
	ts := mustNew(t, Config[tick]{Capacity: 4096, TimeIndex: TimeIndexBTree, MaxReadBatch: 256, Indexes: []Index[tick]{u, mi}})
	times := eventTimes(8192, 0.01, 0.01, 21)
	for i := range 8192 {
		ts.Append(tick{ID: uint64(i + 1), Side: uint8(i % 2)}, at(times[i]))
	}
	mid := times[6000]
	newest := ts.NewestRef()
	dst := make([]Entry[tick], 0, 256)
	vals := make([]tick, 0, 256)
	_, info := ts.Range(nil, times[5000]-1000, MaxTimestamp-1, 64)
	cursor := info.Next

	none := mustNew(t, Config[tick]{Capacity: 4096})
	sub := mustSubscribe(t, none, SubscribeOptions{Start: StartOldest(), OnLag: LagSkip})
	for range 3 {
		mustSubscribe(t, none, SubscribeOptions{}) // notification fan-out
	}
	var seq Timestamp
	batch := make([]Input[tick], 64)
	results := make([]AppendResult, 0, 64)
	for range 100 {
		for i := range batch {
			seq++
			batch[i] = Input[tick]{Value: tick{ID: uint64(seq)}, Meta: at(seq)}
		}
		results, _ = none.AppendBatch(results[:0], batch)
	}
	ctx := context.Background()

	cases := []struct {
		item string // §22.2 wording
		fn   func()
	}{
		{"Get", func() { ts.Get(Ref{Gen: newest.Gen, Seq: newest.Seq - 7}) }},
		{"Latest", func() { ts.Latest() }},
		{"AsOf", func() { ts.AsOf(mid) }},
		{"Before", func() { ts.Before(mid) }},
		{"After", func() { ts.After(mid) }},
		{"FirstAt", func() { ts.FirstAt(mid) }},
		{"LastAt", func() { ts.LastAt(mid) }},
		{"UniqueIndex.Get", func() { u.Get(7000) }},
		{"batch: ReadFrom", func() { dst, _ = ts.ReadFrom(dst[:0], Ref{}, 256) }},
		{"batch: Range", func() { dst, _ = ts.Range(dst[:0], mid, MaxTimestamp-1, 256) }},
		{"batch: RangeValues", func() { vals, _ = ts.RangeValues(vals[:0], mid, MaxTimestamp-1, 256) }},
		{"batch: AtAll", func() { dst, _ = ts.AtAll(dst[:0], mid, 0) }},
		{"batch: Continue", func() { dst, _ = ts.Continue(dst[:0], cursor, 64) }},
		{"batch: ContinueValues", func() { vals, _ = ts.ContinueValues(vals[:0], cursor, 64) }},
		{"batch: PrevN", func() { dst, _ = ts.PrevN(dst[:0], mid, 256) }},
		{"batch: NextN", func() { dst, _ = ts.NextN(dst[:0], mid, 256) }},
		{"batch: MultiIndex.Get", func() { dst, _ = mi.Get(dst[:0], 1, 256, Ascending) }},
		{"TimeIndexNone Append (with subscribers: notification)", func() {
			seq++
			none.Append(tick{ID: uint64(seq)}, at(seq))
		}},
		{"TimeIndexNone AppendBatch (with subscribers: notification)", func() {
			for i := range batch {
				seq++
				batch[i] = Input[tick]{Value: tick{ID: uint64(seq)}, Meta: at(seq)}
			}
			results, _ = none.AppendBatch(results[:0], batch)
		}},
		{"subscriber drain (TryNext/Next)", func() {
			dst, _ = sub.TryNext(dst[:0], 1)
			dst, _ = sub.Next(ctx, dst[:0], 1)
		}},
	}
	for _, c := range cases {
		if n := allocsPerOp(300, c.fn); n != 0 {
			t.Errorf("§22.2 %s: %.4f allocs/op, want 0", c.item, n)
		}
	}
}
