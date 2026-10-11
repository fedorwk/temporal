package temporal

import (
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// T-022, TI-010, Q-017: the same stream through a TimeIndexNone and a
// TimeIndexBTree (or, under LateReject, TimeIndexOrdered) series yields
// identical AppendResults, Refs, statuses and results for every Seq read, key
// lookup and subscription batch, and identical Stats except the
// time-index-only fields.
func TestModeEquivalence(t *testing.T) {
	r, _ := newRand(t)
	type pair struct {
		s    *Series[int]
		u    *UniqueIndex[int, int]
		mi   *MultiIndex[int, int]
		subs []*Subscription[int]
	}
	for run := range iters(60) {
		capacity := 1 + r.IntN(8)
		policy := ConflictPolicy(r.IntN(4))
		late := LatePolicy(r.IntN(2))
		mk := func(mode TimeIndexMode) *pair {
			u := NewUniqueIndex("u", func(e *Entry[int]) (int, bool) { return e.Value % (capacity + 1), e.Value%5 != 0 }, policy)
			mi := NewMultiIndex("m", func(e *Entry[int]) (int, bool) { return int(e.Meta.EventTime % 3), true })
			s := mustNew(t, Config[int]{
				Capacity: capacity, TimeIndex: mode, LatePolicy: late, SourceSeqPolicy: SourceSeqFlag,
				Indexes: []Index[int]{u, mi},
			})
			return &pair{s: s, u: u, mi: mi}
		}
		other := TimeIndexBTree
		if late == LateReject && r.IntN(2) == 0 {
			other = TimeIndexOrdered
		}
		a, b := mk(TimeIndexNone), mk(other)
		g := &inputGen{r: r, now: 100}
		check := func(step int, what string, x, y any) {
			t.Helper()
			if !reflect.DeepEqual(x, y) {
				t.Fatalf("run %d step %d %s: None %+v, mode %d %+v", run, step, what, x, other, y)
			}
		}
		for step := range 300 {
			switch op := r.IntN(20); {
			case op < 7:
				v, in := g.nextValue(), g.meta()
				ra, ea := a.s.Append(v, in)
				rb, eb := b.s.Append(v, in)
				check(step, "Append", []any{ra, fmt.Sprint(ea)}, []any{rb, fmt.Sprint(eb)})
			case op < 9:
				items := make([]Input[int], r.IntN(capacity+2))
				for i := range items {
					items[i] = Input[int]{Value: g.nextValue(), Meta: g.meta()}
				}
				ra, ea := a.s.AppendBatch(nil, items)
				rb, eb := b.s.AppendBatch(nil, items)
				check(step, "AppendBatch", []any{ra, fmt.Sprint(ea)}, []any{rb, fmt.Sprint(eb)})
			case op < 11:
				ref := Ref{Gen: a.s.Gen(), Seq: Seq(r.IntN(int(a.s.NewestRef().Seq) + 3))}
				ea, sa := a.s.Get(ref)
				eb, sb := b.s.Get(ref)
				check(step, "Get", []any{ea, sa}, []any{eb, sb})
				la, lsa := a.s.Latest()
				lb, lsb := b.s.Latest()
				check(step, "Latest", []any{la, lsa}, []any{lb, lsb})
			case op < 13:
				from, limit := Ref{Gen: a.s.Gen(), Seq: Seq(r.IntN(20))}, r.IntN(5)
				xa, ia := a.s.ReadFrom(nil, from, limit)
				xb, ib := b.s.ReadFrom(nil, from, limit)
				check(step, "ReadFrom", []any{xa, ia}, []any{xb, ib})
			case op < 15:
				k := r.IntN(capacity + 2)
				ua, usa := a.u.Get(k)
				ub, usb := b.u.Get(k)
				check(step, "UniqueIndex.Get", []any{ua, usa, a.u.Len()}, []any{ub, usb, b.u.Len()})
				o := Order(r.IntN(2))
				ma, mia := a.mi.Get(nil, k%3, 0, o)
				mb, mib := b.mi.Get(nil, k%3, 0, o)
				check(step, "MultiIndex.Get", []any{ma, mia}, []any{mb, mib})
			case op < 17:
				if len(a.subs) == 0 || r.IntN(4) == 0 {
					start, lag := randomStartRef(r, a.s), LagPolicy(r.IntN(2))
					sa, _ := a.s.Subscribe(SubscribeOptions{Start: start, OnLag: lag})
					sb, _ := b.s.Subscribe(SubscribeOptions{Start: start, OnLag: lag})
					a.subs, b.subs = append(a.subs, sa), append(b.subs, sb)
					continue
				}
				i, limit := r.IntN(len(a.subs)), r.IntN(4)
				xa, ea := a.subs[i].TryNext(nil, limit)
				xb, eb := b.subs[i].TryNext(nil, limit)
				check(step, "TryNext", []any{xa, fmt.Sprint(ea), a.subs[i].Cursor(), a.subs[i].Skipped()},
					[]any{xb, fmt.Sprint(eb), b.subs[i].Cursor(), b.subs[i].Skipped()})
				if ea != nil {
					a.subs[i].Seek(StartOldest())
					b.subs[i].Seek(StartOldest())
				}
			case op < 18:
				check(step, "Clear", fmt.Sprint(a.s.Clear()), fmt.Sprint(b.s.Clear()))
			default:
				ts := g.now + Timestamp(r.IntN(10)-5)
				check(step, "SetWatermark", a.s.SetWatermark(ts), b.s.SetWatermark(ts))
			}
			sa, sb := a.s.Stats(), b.s.Stats()
			sa.TimeIndex, sa.MinEventTime, sa.MaxEventTime = 0, 0, 0
			sb.TimeIndex, sb.MinEventTime, sb.MaxEventTime = 0, 0, 0
			check(step, "Stats", sa, sb)
		}
		mustInvariants(t, a.s)
		mustInvariants(t, b.s)
	}
}

func randomStartRef(r *rand.Rand, s *Series[int]) StartFrom {
	switch r.IntN(3) {
	case 0:
		return StartOldest()
	case 1:
		return StartNext()
	default:
		return StartAt(Ref{Gen: s.Gen(), Seq: Seq(r.IntN(int(s.NewestRef().Seq) + 3))})
	}
}

// checked is a value whose fields must stay mutually consistent; a torn copy
// or a value paired with another record's metadata breaks the checksum.
type checked struct {
	a, b, c, d uint64
	sum        uint64
}

func newChecked(ts Timestamp, salt uint64) checked {
	v := checked{a: uint64(ts), b: salt, c: salt * 31, d: ^salt}
	v.sum = v.a ^ v.b ^ v.c ^ v.d ^ 0x9e3779b97f4a7c15
	return v
}

func (v checked) ok(e Meta) bool {
	return v.sum == v.a^v.b^v.c^v.d^0x9e3779b97f4a7c15 && v.a == uint64(e.EventTime) && v.c == v.b*31
}

// T-005, T-001, C-003, §6.4: readers copying while writers overwrite never see
// a torn value or a value paired with another record's metadata.
func TestRaceStress_NoTornReads(t *testing.T) {
	for _, mode := range allModes {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			s := mustNew(t, withMode(mode, Config[checked]{Capacity: 64, MaxReadBatch: 32}))
			ops := iters(20000)
			var stop atomic.Bool
			var bad atomic.Value
			report := func(where string, e Entry[checked]) {
				if !e.Value.ok(e.Meta) {
					bad.CompareAndSwap(nil, fmt.Sprintf("%s: torn or mismatched %+v / %+v", where, e.Value, e.Meta))
				}
			}
			var writers, readers sync.WaitGroup
			for w := range 2 {
				writers.Add(1)
				go func() {
					defer writers.Done()
					r := rand.New(rand.NewPCG(uint64(w), 1))
					for i := range ops {
						ts := Timestamp(i*2 + w + r.IntN(5))
						if i%5 == 0 {
							items := make([]Input[checked], 4)
							for j := range items {
								items[j] = Input[checked]{Value: newChecked(ts+Timestamp(j), r.Uint64()), Meta: at(ts + Timestamp(j))}
							}
							s.AppendBatch(nil, items)
							continue
						}
						s.Append(newChecked(ts, r.Uint64()), at(ts))
					}
				}()
			}
			for rd := range 4 {
				readers.Add(1)
				go func() {
					defer readers.Done()
					buf := make([]Entry[checked], 0, 32)
					for i := 0; !stop.Load(); i++ {
						if e, st := s.Latest(); st == StatusOK {
							report("Latest", e)
						}
						if e, st := s.Get(s.OldestRef()); st == StatusOK {
							report("Get", e)
						}
						buf, _ = s.ReadFrom(buf[:0], Ref{}, 0)
						for _, e := range buf {
							report("ReadFrom", e)
						}
						if mode != TimeIndexNone {
							ts := Timestamp(i % (ops * 2))
							if e, st := s.AsOf(ts); st == StatusOK {
								report("AsOf", e)
							}
							var info ReadInfo
							buf, info = s.Range(buf[:0], ts-50, ts+50, 8)
							for {
								for _, e := range buf {
									report("Range/Continue", e)
								}
								if info.Next.IsZero() {
									break
								}
								buf, info = s.Continue(buf[:0], info.Next, 8)
							}
						}
						_ = rd
					}
				}()
			}
			for range 2 {
				sub := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest(), OnLag: LagSkip})
				readers.Add(1)
				go func() {
					defer readers.Done()
					defer sub.Close()
					buf := make([]Entry[checked], 0, 32)
					for !stop.Load() {
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
						var err error
						buf, err = sub.Next(ctx, buf[:0], 0)
						cancel()
						if err == nil {
							for _, e := range buf {
								report("Subscription", e)
							}
						}
					}
				}()
			}
			writers.Wait()
			stop.Store(true)
			readers.Wait()
			if msg := bad.Load(); msg != nil {
				t.Fatal(msg)
			}
			mustInvariants(t, s)
		})
	}
}

// deep is a reference-heavy value for Clone tests.
type deep struct {
	xs []int
	m  map[string]int
}

func deepClone(v deep) deep { return deep{xs: slices.Clone(v.xs), m: maps.Clone(v.m)} }

func newDeep(i int) deep { return deep{xs: []int{i, i}, m: map[string]int{"k": i}} }

func (v deep) mutate()           { v.xs[0] = -999; v.m["k"] = -999 }
func (v deep) intact(i int) bool { return v.xs[0] == i && v.m["k"] == i }

// T-006, §11.5: with CloneOnRead every read path returns values the caller
// may mutate freely; with CloneOnWrite the producer may reuse its buffers.
func TestClone_DeepOwnershipAllPaths(t *testing.T) {
	for _, mode := range []CloneMode{CloneOnRead, CloneOnWrite, CloneOnReadWrite} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			u := NewUniqueIndex("u", func(e *Entry[deep]) (int, bool) { return e.Value.xs[1], true }, ConflictReject)
			mi := NewMultiIndex("m", func(e *Entry[deep]) (int, bool) { return e.Value.xs[1] % 2, true })
			s := mustNew(t, Config[deep]{
				Capacity: 16, TimeIndex: TimeIndexBTree, MaxReadBatch: 2,
				Clone: deepClone, CloneMode: mode, Indexes: []Index[deep]{u, mi},
			})
			sub := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest()})

			// Producer buffers, mutated after Append/AppendBatch.
			var produced []deep
			for i := 1; i <= 4; i++ {
				v := newDeep(i)
				mustAppend(t, s, v, at(Timestamp(i)))
				produced = append(produced, v)
			}
			batch := []Input[deep]{{newDeep(5), at(5)}, {newDeep(6), at(6)}}
			if _, err := s.AppendBatch(nil, batch); err != nil {
				t.Fatal(err)
			}
			produced = append(produced, batch[0].Value, batch[1].Value)
			if mode != CloneOnRead {
				for _, v := range produced {
					v.mutate()
				}
			}
			if mode == CloneOnWrite {
				// Without read cloning, readers share storage; check write isolation only.
				all, _ := s.ReadFrom(nil, Ref{}, 0)
				for len(all) < 6 {
					more, _ := s.ReadFrom(nil, Ref{Gen: 1, Seq: Seq(len(all) + 1)}, 0)
					all = append(all, more...)
				}
				for _, e := range all {
					if !e.Value.intact(int(e.Meta.EventTime)) {
						t.Fatalf("producer mutation reached storage: %+v", e)
					}
				}
				return
			}

			// Every read path: mutate what it returns, then verify storage.
			reads := map[string]func() []Entry[deep]{
				"Get":    func() []Entry[deep] { e, _ := s.Get(Ref{1, 2}); return []Entry[deep]{e} },
				"Latest": func() []Entry[deep] { e, _ := s.Latest(); return []Entry[deep]{e} },
				"ReadFrom": func() []Entry[deep] {
					x, _ := s.ReadFrom(nil, Ref{}, 0)
					return x
				},
				"AsOf":              func() []Entry[deep] { e, _ := s.AsOf(3); return []Entry[deep]{e} },
				"Before":            func() []Entry[deep] { e, _ := s.Before(3); return []Entry[deep]{e} },
				"After":             func() []Entry[deep] { e, _ := s.After(3); return []Entry[deep]{e} },
				"AtOrAfter":         func() []Entry[deep] { e, _ := s.AtOrAfter(3); return []Entry[deep]{e} },
				"FirstAt":           func() []Entry[deep] { e, _ := s.FirstAt(3); return []Entry[deep]{e} },
				"LastAt":            func() []Entry[deep] { e, _ := s.LastAt(3); return []Entry[deep]{e} },
				"LatestByEventTime": func() []Entry[deep] { e, _ := s.LatestByEventTime(); return []Entry[deep]{e} },
				"AtAll":             func() []Entry[deep] { x, _ := s.AtAll(nil, 4, 0); return x },
				"Range":             func() []Entry[deep] { x, _ := s.Range(nil, 0, 10, 0); return x },
				"Continue": func() []Entry[deep] {
					_, info := s.Range(nil, 0, 10, 1)
					x, _ := s.Continue(nil, info.Next, 0)
					return x
				},
				"PrevN": func() []Entry[deep] { x, _ := s.PrevN(nil, 5, 2); return x },
				"NextN": func() []Entry[deep] { x, _ := s.NextN(nil, 1, 2); return x },
				"Entries": func() []Entry[deep] {
					seq, _ := s.Entries(0, 10)
					var x []Entry[deep]
					for e := range seq {
						x = append(x, e)
					}
					return x
				},
				"UniqueIndex.Get":   func() []Entry[deep] { e, _ := u.Get(3); return []Entry[deep]{e} },
				"MultiIndex.Get":    func() []Entry[deep] { x, _ := mi.Get(nil, 1, 0, Descending); return x },
				"MultiIndex.Latest": func() []Entry[deep] { e, _ := mi.Latest(0); return []Entry[deep]{e} },
				"Subscription": func() []Entry[deep] {
					sub.Seek(StartOldest())
					x, _ := sub.TryNext(nil, 0)
					return x
				},
			}
			for name, read := range reads {
				got := read()
				if len(got) == 0 {
					t.Fatalf("%s returned nothing", name)
				}
				for _, e := range got {
					if !e.Value.intact(int(e.Meta.EventTime)) {
						t.Fatalf("%s returned a corrupted value %+v", name, e)
					}
					e.Value.mutate()
				}
				again, _ := s.ReadFrom(nil, Ref{}, 0)
				for len(again) < 6 {
					more, _ := s.ReadFrom(nil, Ref{Gen: 1, Seq: Seq(len(again) + 1)}, 0)
					again = append(again, more...)
				}
				for _, e := range again {
					if !e.Value.intact(int(e.Meta.EventTime)) {
						t.Fatalf("mutating %s's result changed storage: %+v", name, e)
					}
				}
			}
			values, _ := s.RangeValues(nil, 0, 10, 0)
			values[0].mutate()
			cv, _ := s.ContinueValues(nil, func() TimeCursor { _, i := s.Range(nil, 0, 10, 1); return i.Next }(), 0)
			cv[0].mutate()
			if e, _ := s.Get(Ref{1, 1}); !e.Value.intact(1) {
				t.Fatal("RangeValues/ContinueValues results share storage")
			}
		})
	}
}
