package temporal

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
)

// modelIndex is the reference model of one key index. The model keeps no maps:
// each retained record carries (key, ok, indexed) per index, and lookups are
// linear scans over the retained records (§23.2).
type modelIndex struct {
	name   string
	unique bool
	policy ConflictPolicy
	fn     func(e *Entry[int]) (int, bool)

	conflicts, dropped, replaced, keptFirst uint64
}

// islot is one record's state in one index.
type islot struct {
	key     int
	ok      bool
	indexed bool
}

// call runs the extractor, reporting a panic like the Series does.
func (ix *modelIndex) call(e *Entry[int]) (k int, ok bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &CallbackPanicError{Callback: "extractor:" + ix.name, Value: r}
		}
	}()
	k, ok = ix.fn(e)
	return k, ok, nil
}

type modelDecision struct {
	kind   decisionKind
	holder int // position in m.recs (Replace, KeepFirst, Drop)
	slot   islot
}

// holderOf returns the retained record (by position) indexed under k in index
// i, skipping positions below from (rows this transaction evicts).
func (m *model) holderOf(i, k, from int) (int, bool) {
	for j := from; j < len(m.recs); j++ {
		if sl := m.slots[j][i]; sl.indexed && sl.key == k {
			return j, true
		}
	}
	return 0, false
}

// decideIndexes evaluates every index for the tentative entry e in Config
// order (§10.3, P-03): any error fails, otherwise Drop wins.
func (m *model) decideIndexes(e *Entry[int], evicting bool) ([]modelDecision, error) {
	from := 0
	if evicting {
		from = 1
	}
	ds := make([]modelDecision, len(m.idx))
	for i, ix := range m.idx {
		k, ok, err := ix.call(e)
		if err != nil {
			return nil, err
		}
		d := modelDecision{kind: decNotIndexed, slot: islot{key: k, ok: ok}}
		if ok {
			d.kind = decInsert
			if h, taken := m.holderOf(i, k, from); ix.unique && taken {
				d.holder = h
				switch ix.policy {
				case ConflictDrop:
					d.kind = decDrop
				case ConflictReplace:
					d.kind = decReplace
				case ConflictKeepFirst:
					d.kind = decKeepFirst
				default:
					ix.conflicts++
					return nil, &ConflictError{
						Index:    ix.name,
						Key:      fmt.Sprint(k),
						Existing: m.recs[h].Meta.Ref,
					}
				}
			}
		}
		ds[i] = d
	}
	return ds, nil
}

// errMatches compares a Series error with the model's expected error,
// including the fields of typed errors.
func errMatches(got, want error) bool {
	var wc *ConflictError
	var wp *CallbackPanicError
	switch {
	case want == nil:
		return got == nil
	case errors.As(want, &wc):
		var gc *ConflictError
		return errors.As(got, &gc) && gc.Index == wc.Index && gc.Key == wc.Key &&
			gc.Existing == wc.Existing && gc.BatchPos == wc.BatchPos
	case errors.As(want, &wp):
		var gp *CallbackPanicError
		return errors.As(got, &gp) && gp.Callback == wp.Callback && gp.Value == wp.Value
	default:
		return errors.Is(got, want)
	}
}

func (m *model) uniqueGet(i, k int) (Entry[int], Status) {
	if j, ok := m.holderOf(i, k, 0); ok {
		return m.recs[j], StatusOK
	}
	return Entry[int]{}, StatusNotFound
}

func (m *model) multiAll(i, k int) []Entry[int] {
	var out []Entry[int]
	for j := range m.recs {
		if sl := m.slots[j][i]; sl.indexed && sl.key == k {
			out = append(out, m.recs[j])
		}
	}
	return out
}

func (m *model) multiGet(i, k, limit int, o Order) ([]Entry[int], ReadInfo) {
	all := m.multiAll(i, k)
	if o == Descending {
		slices.Reverse(all)
	}
	info := ReadInfo{Status: StatusNotFound, Oldest: m.oldestRef()}
	lim := m.limit(limit)
	if len(all) > lim {
		all, info.Truncated = all[:lim], true
	}
	if len(all) > 0 {
		info.Status, info.N, info.LastRef = StatusOK, len(all), all[len(all)-1].Meta.Ref
	}
	return all, info
}

func (m *model) indexKeys(i int) int {
	keys := map[int]bool{}
	for j := range m.recs {
		if sl := m.slots[j][i]; sl.indexed {
			keys[sl.key] = true
		}
	}
	return len(keys)
}

func (m *model) indexStats() []IndexStats {
	if len(m.idx) == 0 {
		return nil
	}
	out := make([]IndexStats, len(m.idx))
	for i, ix := range m.idx {
		out[i] = IndexStats{Name: ix.name, Unique: ix.unique, Keys: m.indexKeys(i)}
		if ix.unique {
			out[i].Conflicts, out[i].Dropped, out[i].Replaced, out[i].KeptFirst =
				ix.conflicts, ix.dropped, ix.replaced, ix.keptFirst
		}
	}
	return out
}

// indexHandles are the typed handles registered with the Series under test,
// in Config order; exactly one of unique/multi is set per position.
type indexHandle struct {
	unique *UniqueIndex[int, int]
	multi  *MultiIndex[int, int]
}

// randomIndexes adds 0-2 indexes with random policies and extractors to cfg
// and the model. Extractors key on the value (a counter, so keys repeat with
// period n, often equal to the row being evicted) or on EventTime; one variant
// panics on some values.
func randomIndexes(r *rand.Rand, cfg *Config[int], m *model) []indexHandle {
	var handles []indexHandle
	for i := range r.IntN(3) {
		name := fmt.Sprintf("ix%d", i)
		n := 1 + r.IntN(cfg.Capacity+3)
		var fn func(e *Entry[int]) (int, bool)
		switch r.IntN(4) {
		case 0:
			fn = func(e *Entry[int]) (int, bool) { return int(e.Meta.EventTime % 5), true }
		case 1:
			fn = func(e *Entry[int]) (int, bool) {
				if e.Value%41 == 17 {
					panic(fmt.Sprintf("extractor %d", e.Value))
				}
				return e.Value % n, true
			}
		default:
			fn = func(e *Entry[int]) (int, bool) { return e.Value % n, e.Value%7 != 3 }
		}
		mi := &modelIndex{name: name, fn: fn}
		if r.IntN(3) == 0 {
			h := NewMultiIndex(name, Extractor[int, int](fn))
			cfg.Indexes = append(cfg.Indexes, h)
			handles = append(handles, indexHandle{multi: h})
		} else {
			mi.unique = true
			mi.policy = ConflictPolicy(r.IntN(4))
			h := NewUniqueIndex(name, Extractor[int, int](fn), mi.policy)
			cfg.Indexes = append(cfg.Indexes, h)
			handles = append(handles, indexHandle{unique: h})
		}
		m.idx = append(m.idx, mi)
	}
	return handles
}

// runIndexLookup compares one random key lookup with the model.
func runIndexLookup(r *rand.Rand, m *model, handles []indexHandle) (string, string) {
	if len(handles) == 0 {
		return "", ""
	}
	i := r.IntN(len(handles))
	h := handles[i]
	k := r.IntN(12)
	if h.unique != nil {
		got, gotSt := h.unique.Get(k)
		want, wantSt := m.uniqueGet(i, k)
		if got != want || gotSt != wantSt {
			return "UniqueIndex.Get", fmt.Sprintf("key %d: got (%+v, %v), want (%+v, %v)", k, got, gotSt, want, wantSt)
		}
		if h.unique.Contains(k) != (wantSt == StatusOK) || h.unique.Len() != m.indexKeys(i) {
			return "UniqueIndex.Contains/Len", fmt.Sprintf("key %d", k)
		}
		return "", ""
	}
	limit, o := r.IntN(6)-1, Order(r.IntN(2))
	got, gotInfo := h.multi.Get([]Entry[int]{{Value: -3}}, k, limit, o)
	want, wantInfo := m.multiGet(i, k, limit, o)
	if gotInfo != wantInfo || got[0].Value != -3 || len(got) != 1+len(want) ||
		(len(want) > 0 && !slices.Equal(got[1:], want)) {
		return "MultiIndex.Get", fmt.Sprintf("key %d limit %d order %d: got %+v %+v, want %+v %+v",
			k, limit, o, got, gotInfo, want, wantInfo)
	}
	all := m.multiAll(i, k)
	latest, st := h.multi.Latest(k)
	if (st == StatusOK) != (len(all) > 0) || (len(all) > 0 && latest != all[len(all)-1]) {
		return "MultiIndex.Latest", fmt.Sprintf("key %d: %+v %v", k, latest, st)
	}
	if h.multi.Count(k) != len(all) || h.multi.Keys() != m.indexKeys(i) {
		return "MultiIndex.Count/Keys", fmt.Sprintf("key %d: count %d want %d", k, h.multi.Count(k), len(all))
	}
	return "", ""
}
