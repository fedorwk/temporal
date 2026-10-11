package temporal

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// model is a deliberately naive reference implementation of the Series
// semantics in the spec: a plain slice of retained entries in Seq order plus
// the trackers, written without the ring, bounds or saturating arithmetic.
type model struct {
	name      string
	mode      TimeIndexMode
	capacity  int
	maxBatch  int
	late      LatePolicy
	tolerance int64
	srcPolicy SourceSeqPolicy
	gate      bool
	sid       uint64 // identity of the Series under test, for expected cursors

	gen     Gen
	recs    []Entry[int] // retained, ascending Seq
	slots   [][]islot    // per retained record, per index
	idx     []*modelIndex
	nextSeq Seq

	evicted          bool
	minEvict, maxEvi Timestamp
	observed         bool
	maxObserved      Timestamp
	trkValid         bool
	trkEpoch         SourceEpoch
	trkLast          uint64
	watermark        Timestamp
	closed           bool

	appended, evictedN, dropped, rejected uint64
	lateN, srcN, gaps, clears             uint64
}

func newModel(cfg Config[int]) *model {
	mb := cfg.MaxReadBatch
	if mb == 0 {
		mb = 4096
	}
	return &model{
		name:      cfg.Name,
		mode:      cfg.TimeIndex,
		capacity:  cfg.Capacity,
		maxBatch:  mb,
		late:      cfg.LatePolicy,
		tolerance: int64(cfg.LateTolerance),
		srcPolicy: cfg.SourceSeqPolicy,
		gate:      cfg.GateReadsByWatermark,
		gen:       1,
		nextSeq:   1,
		watermark: MinTimestamp,
	}
}

// isLate implements §5.3 without saturation: EventTime is late iff
// maxObserved - EventTime > tolerance, computed as an unsigned distance.
func (m *model) isLate(ts Timestamp) bool {
	if !m.observed || ts >= m.maxObserved {
		return false
	}
	return uint64(m.maxObserved)-uint64(ts) > uint64(m.tolerance)
}

func (m *model) append(v int, in InputMeta) (AppendResult, error) {
	if m.closed {
		return AppendResult{}, ErrClosed
	}
	if in.EventTime == MinTimestamp || in.EventTime == MaxTimestamp || in.Flags&SeriesFlagsMask != 0 {
		m.rejected++
		return AppendResult{}, ErrInvalidInput
	}
	var flags Flags
	if m.isLate(in.EventTime) {
		if m.late == LateReject {
			m.rejected++
			return AppendResult{}, ErrLate
		}
		flags |= FlagLate
	}
	checkSrc := m.srcPolicy != SourceSeqIgnore && in.SourceSeq != 0
	if checkSrc && m.trkValid && m.trkEpoch == in.SourceEpoch && in.SourceSeq <= m.trkLast {
		if m.srcPolicy == SourceSeqReject {
			m.rejected++
			return AppendResult{}, ErrSourceSeqNotIncreasing
		}
		flags |= FlagSourceSeqNotIncreasing
	}
	if m.nextSeq == math.MaxUint64 {
		m.rejected++
		return AppendResult{}, ErrSeqExhausted
	}

	all := in.Flags | flags
	entry := Entry[int]{Value: v, Meta: Meta{
		EventTime:   in.EventTime,
		ReceiveTime: in.ReceiveTime,
		SourceSeq:   in.SourceSeq,
		SourceEpoch: in.SourceEpoch,
		Flags:       all,
		Ref:         Ref{Gen: m.gen, Seq: m.nextSeq},
	}}
	evicting := len(m.recs) == m.capacity
	ds, err := m.decideIndexes(&entry, evicting)
	if err != nil {
		if _, panicked := err.(modelPanic); !panicked { // a panic returns no error, so no counter moves
			m.rejected++
		}
		return AppendResult{}, err
	}
	res := AppendResult{Ref: entry.Meta.Ref, Flags: flags}
	for i, d := range ds {
		if d.kind == decDrop {
			// Drop wins: nothing changes except the drop counters (P-03).
			m.dropped++
			for j, d2 := range ds {
				if d2.kind == decDrop {
					m.idx[j].dropped++
				}
			}
			return AppendResult{Outcome: OutcomeDropped, Flags: flags,
				Existing: m.recs[d.holder].Meta.Ref, Index: m.idx[i].name}, nil
		}
	}
	for i, d := range ds {
		if d.kind == decReplace || d.kind == decKeepFirst {
			res.Existing, res.Index = m.recs[d.holder].Meta.Ref, m.idx[i].name
			break
		}
	}
	// Apply index decisions against positions before the eviction shifts them.
	row := make([]islot, len(ds))
	for i, d := range ds {
		row[i] = d.slot
		switch d.kind {
		case decInsert:
			row[i].indexed = true
		case decReplace:
			m.slots[d.holder][i].indexed = false
			row[i].indexed = true
			m.idx[i].replaced++
		case decKeepFirst:
			m.idx[i].keptFirst++
		}
	}
	if evicting {
		old := m.recs[0]
		m.recs = m.recs[1:]
		m.slots = m.slots[1:]
		res.Evicted = old.Meta.Ref
		if !m.evicted {
			m.evicted, m.minEvict, m.maxEvi = true, old.Meta.EventTime, old.Meta.EventTime
		}
		m.minEvict = min(m.minEvict, old.Meta.EventTime)
		m.maxEvi = max(m.maxEvi, old.Meta.EventTime)
		m.evictedN++
	}
	m.recs = append(m.recs, entry)
	m.slots = append(m.slots, row)
	m.nextSeq++
	if !m.observed || in.EventTime > m.maxObserved {
		m.observed, m.maxObserved = true, in.EventTime
	}
	if checkSrc {
		if !m.trkValid || m.trkEpoch != in.SourceEpoch {
			m.trkValid, m.trkEpoch, m.trkLast = true, in.SourceEpoch, in.SourceSeq
		} else {
			m.trkLast = max(m.trkLast, in.SourceSeq)
		}
	}
	m.appended++
	if all&FlagLate != 0 {
		m.lateN++
	}
	if all&FlagSourceSeqNotIncreasing != 0 {
		m.srcN++
	}
	if all&FlagGap != 0 {
		m.gaps++
	}
	return res, nil
}

// clone deep-copies the model state.
func (m *model) clone() *model {
	c := *m
	c.recs = slices.Clone(m.recs)
	c.slots = make([][]islot, len(m.slots))
	for i := range m.slots {
		c.slots[i] = slices.Clone(m.slots[i])
	}
	c.idx = make([]*modelIndex, len(m.idx))
	for i, ix := range m.idx {
		cp := *ix
		c.idx[i] = &cp
	}
	return &c
}

// appendBatch follows W-002, W-003, P-02 and P-09: the same Appends in order
// on a copy, adopted only if every item succeeds. A failure moves only the
// diagnostic counters (P-04): Rejected once, and Conflicts of the rejecting
// index.
func (m *model) appendBatch(items []Input[int]) ([]AppendResult, error) {
	switch {
	case m.closed:
		return nil, ErrClosed
	case len(items) == 0:
		return nil, nil
	case len(items) > m.capacity:
		m.rejected++
		return nil, ErrBatchTooLarge
	}
	c := m.clone()
	var out []AppendResult
	for i, it := range items {
		res, err := c.append(it.Value, it.Meta)
		if _, panicked := err.(modelPanic); panicked {
			return nil, err // propagates with no state or counter change
		}
		if err != nil {
			m.rejected++
			var ce *ConflictError
			if errors.As(err, &ce) {
				ce.BatchPos = i
				for _, ix := range m.idx {
					if ix.name == ce.Index {
						ix.conflicts++
					}
				}
			}
			return nil, err
		}
		out = append(out, res)
	}
	*m = *c
	return out, nil
}

func (m *model) find(seq Seq) (Entry[int], bool) {
	for _, e := range m.recs {
		if e.Meta.Ref.Seq == seq {
			return e, true
		}
	}
	return Entry[int]{}, false
}

// get follows the Get row of §7.2.
func (m *model) get(ref Ref) (Entry[int], Status) {
	switch {
	case ref.Seq == 0 || ref.Gen > m.gen:
		return Entry[int]{}, StatusNotFound
	case ref.Gen < m.gen:
		return Entry[int]{}, StatusReset
	case ref.Seq >= m.nextSeq:
		return Entry[int]{}, StatusNotFound
	}
	if e, ok := m.find(ref.Seq); ok {
		return e, StatusOK
	}
	return Entry[int]{}, StatusExpired
}

func (m *model) latest() (Entry[int], Status) {
	if len(m.recs) == 0 {
		return Entry[int]{}, StatusNotFound
	}
	return m.recs[len(m.recs)-1], StatusOK
}

func (m *model) oldestRef() Ref {
	if len(m.recs) == 0 {
		return Ref{}
	}
	return m.recs[0].Meta.Ref
}

func (m *model) newestRef() Ref {
	if len(m.recs) == 0 {
		return Ref{}
	}
	return m.recs[len(m.recs)-1].Meta.Ref
}

// readFrom follows Q-007, §8.1, §8.2 and P-06.
func (m *model) readFrom(from Ref, limit int) ([]Entry[int], ReadInfo) {
	info := ReadInfo{Oldest: m.oldestRef()}
	start := from.Seq
	if from.Seq == 0 {
		if len(m.recs) == 0 {
			info.Status = StatusNotFound
			return nil, info
		}
		start = m.recs[0].Meta.Ref.Seq
	} else if from.Gen < m.gen {
		info.Status = StatusReset
		return nil, info
	} else if from.Gen > m.gen {
		info.Status = StatusNotFound
		return nil, info
	}
	if start >= m.nextSeq {
		info.Status = StatusNotFound
		return nil, info
	}
	if len(m.recs) == 0 || start < m.recs[0].Meta.Ref.Seq {
		info.Status, info.Evicted = StatusExpired, true
		return nil, info
	}
	lim := limit
	if lim <= 0 || lim > m.maxBatch {
		lim = m.maxBatch
	}
	var out []Entry[int]
	for _, e := range m.recs {
		if e.Meta.Ref.Seq < start {
			continue
		}
		if len(out) == lim {
			info.Truncated = true
			break
		}
		out = append(out, e)
	}
	info.Status, info.N, info.LastRef = StatusOK, len(out), out[len(out)-1].Meta.Ref
	return out, info
}

// byTime returns the retained entries sorted by (EventTime, Seq).
func (m *model) byTime() []Entry[int] {
	out := slices.Clone(m.recs)
	slices.SortFunc(out, func(a, b Entry[int]) int {
		if c := cmp.Compare(a.Meta.EventTime, b.Meta.EventTime); c != 0 {
			return c
		}
		return cmp.Compare(a.Meta.Ref.Seq, b.Meta.Ref.Seq)
	})
	return out
}

// pointQuery names an event-time point read for the model and the Series.
type pointQuery int

const (
	qAsOf pointQuery = iota
	qBefore
	qAfter
	qAtOrAfter
	qFirstAt
	qLastAt
	qLatestByEventTime
	numPointQueries
)

func (q pointQuery) run(s *Series[int], ts Timestamp) (Entry[int], Status) {
	switch q {
	case qAsOf:
		return s.AsOf(ts)
	case qBefore:
		return s.Before(ts)
	case qAfter:
		return s.After(ts)
	case qAtOrAfter:
		return s.AtOrAfter(ts)
	case qFirstAt:
		return s.FirstAt(ts)
	case qLastAt:
		return s.LastAt(ts)
	default:
		return s.LatestByEventTime()
	}
}

func (q pointQuery) String() string {
	return [...]string{"AsOf", "Before", "After", "AtOrAfter", "FirstAt", "LastAt", "LatestByEventTime"}[q]
}

// point follows §7.2, §7.5, §19 and P-13 by linear scan over byTime.
func (m *model) point(q pointQuery, ts Timestamp) (Entry[int], Status) {
	var none Entry[int]
	if m.mode == TimeIndexNone {
		return none, StatusTimeIndexDisabled
	}
	sorted := m.byTime()
	et := func(e Entry[int]) Timestamp { return e.Meta.EventTime }
	lastWhere := func(pred func(Entry[int]) bool) (Entry[int], bool) {
		for i := len(sorted) - 1; i >= 0; i-- {
			if pred(sorted[i]) {
				return sorted[i], true
			}
		}
		return none, false
	}
	firstWhere := func(pred func(Entry[int]) bool) (Entry[int], bool) {
		for _, e := range sorted {
			if pred(e) {
				return e, true
			}
		}
		return none, false
	}
	miss := func(expired bool) Status {
		if expired {
			return StatusExpired
		}
		return StatusNotFound
	}
	backward := func(pred func(Entry[int]) bool, last bool, expired bool) (Entry[int], Status) {
		if m.gate && ts > m.watermark {
			return none, StatusNotReady
		}
		var e Entry[int]
		var ok bool
		if last {
			e, ok = lastWhere(pred)
		} else {
			e, ok = firstWhere(pred)
		}
		if ok {
			return e, StatusOK
		}
		return none, miss(expired)
	}
	forward := func(pred func(Entry[int]) bool, expired bool) (Entry[int], Status) {
		e, ok := firstWhere(pred)
		switch {
		case ok && (!m.gate || et(e) <= m.watermark):
			return e, StatusOK
		case ok:
			return none, StatusNotReady
		case expired:
			return none, StatusExpired
		case m.gate && m.watermark != MaxTimestamp:
			return none, StatusNotReady
		default:
			return none, StatusNotFound
		}
	}
	ev := m.evicted
	switch q {
	case qAsOf:
		return backward(func(e Entry[int]) bool { return et(e) <= ts }, true, ev && m.minEvict <= ts)
	case qBefore:
		return backward(func(e Entry[int]) bool { return et(e) < ts }, true, ev && m.minEvict < ts)
	case qAfter:
		return forward(func(e Entry[int]) bool { return et(e) > ts }, ev && m.maxEvi > ts)
	case qAtOrAfter:
		return forward(func(e Entry[int]) bool { return et(e) >= ts }, ev && m.maxEvi >= ts)
	case qFirstAt:
		return backward(func(e Entry[int]) bool { return et(e) == ts }, false, ev && m.minEvict <= ts && ts <= m.maxEvi)
	case qLastAt:
		return backward(func(e Entry[int]) bool { return et(e) == ts }, true, ev && m.minEvict <= ts && ts <= m.maxEvi)
	default:
		if len(sorted) == 0 {
			return none, StatusNotFound
		}
		return sorted[len(sorted)-1], StatusOK
	}
}

// gatedBelow is the Range/Continue gate: to-1 > watermark, written without
// computing to-1.
func (m *model) gatedBelow(to Timestamp) bool {
	return m.gate && m.watermark != MaxTimestamp && to > m.watermark+1
}

func (m *model) oldestInfo() ReadInfo { return ReadInfo{Oldest: m.oldestRef()} }

// ascendFrom returns the retained entries with key > after (or >= start when
// inclusive) and EventTime < bound, as the model's ascending scan.
func (m *model) scan(pred func(Entry[int]) bool) []Entry[int] {
	var out []Entry[int]
	for _, e := range m.byTime() {
		if pred(e) {
			out = append(out, e)
		}
	}
	return out
}

func (m *model) limit(l int) int {
	if l <= 0 || l > m.maxBatch {
		return m.maxBatch
	}
	return l
}

// page cuts matches to the limit and fills a continuable ReadInfo.
func (m *model) page(matches []Entry[int], lim int, bound Timestamp, evicted bool) ([]Entry[int], ReadInfo) {
	info := m.oldestInfo()
	info.Evicted = evicted
	if len(matches) > lim {
		matches, info.Truncated = matches[:lim], true
	}
	info.N = len(matches)
	if len(matches) == 0 {
		info.Status = StatusNotFound
		if evicted {
			info.Status = StatusExpired
		}
		return nil, info
	}
	last := matches[len(matches)-1]
	info.Status, info.LastRef = StatusOK, last.Meta.Ref
	if info.Truncated {
		info.Next = makeCursor(m.sid, m.gen, timeKey{last.Meta.EventTime, last.Meta.Ref.Seq}, bound)
	}
	return matches, info
}

func (m *model) envIntersects(from, to Timestamp) bool {
	return m.evicted && m.minEvict < to && m.maxEvi >= from
}

// rangeQ follows Q-005, E-002, §7.4 and §19.
func (m *model) rangeQ(from, to Timestamp, limit int) ([]Entry[int], ReadInfo) {
	info := m.oldestInfo()
	switch {
	case m.mode == TimeIndexNone:
		info.Status = StatusTimeIndexDisabled
		return nil, info
	case m.gatedBelow(to):
		info.Status = StatusNotReady
		return nil, info
	case from >= to:
		info.Status = StatusNotFound
		return nil, info
	}
	matches := m.scan(func(e Entry[int]) bool { return from <= e.Meta.EventTime && e.Meta.EventTime < to })
	return m.page(matches, m.limit(limit), to, m.envIntersects(from, to))
}

// atAll follows Q-002, §8.1 and P-05.
func (m *model) atAll(ts Timestamp, limit int) ([]Entry[int], ReadInfo) {
	info := m.oldestInfo()
	switch {
	case m.mode == TimeIndexNone:
		info.Status = StatusTimeIndexDisabled
		return nil, info
	case m.gate && ts > m.watermark:
		info.Status = StatusNotReady
		return nil, info
	case ts == MaxTimestamp:
		info.Status = StatusNotFound
		return nil, info
	}
	matches := m.scan(func(e Entry[int]) bool { return e.Meta.EventTime == ts })
	return m.page(matches, m.limit(limit), ts+1, m.evicted && m.minEvict <= ts && ts <= m.maxEvi)
}

// continueQ follows §8.4, E-016 and P-14.
func (m *model) continueQ(c TimeCursor, limit int) ([]Entry[int], ReadInfo) {
	info := m.oldestInfo()
	switch {
	case m.mode == TimeIndexNone:
		info.Status = StatusTimeIndexDisabled
		return nil, info
	case c == (TimeCursor{}) || c.series != m.sid || c.gen > m.gen:
		info.Status = StatusInvalid
		return nil, info
	case c.gen < m.gen:
		info.Status = StatusReset
		return nil, info
	}
	last, bound := c.decode()
	if m.gatedBelow(bound) {
		info.Status = StatusNotReady
		return nil, info
	}
	matches := m.scan(func(e Entry[int]) bool {
		k := timeKey{e.Meta.EventTime, e.Meta.Ref.Seq}
		return keyLess(last, k) && e.Meta.EventTime < bound
	})
	return m.page(matches, m.limit(limit), bound, m.envIntersects(last.eventTime, bound))
}

// prevN follows Q-006, §8.1 and P-05.
func (m *model) prevN(ts Timestamp, n int) ([]Entry[int], ReadInfo) {
	info := m.oldestInfo()
	switch {
	case m.mode == TimeIndexNone:
		info.Status = StatusTimeIndexDisabled
		return nil, info
	case m.gate && ts > m.watermark:
		info.Status = StatusNotReady
		return nil, info
	case n <= 0:
		info.Status = StatusNotFound
		return nil, info
	}
	matches := m.scan(func(e Entry[int]) bool { return e.Meta.EventTime <= ts })
	lim := min(n, m.maxBatch)
	if len(matches) > lim {
		matches, info.Truncated = matches[len(matches)-lim:], true
	}
	info.N, info.Evicted = len(matches), m.evicted && m.minEvict <= ts
	if len(matches) == 0 {
		info.Status = StatusNotFound
		if info.Evicted {
			info.Status = StatusExpired
		}
		return nil, info
	}
	info.Status, info.LastRef = StatusOK, matches[len(matches)-1].Meta.Ref
	return matches, info
}

// nextN follows Q-006, §8.1, P-05, P-13 and S4-01.
func (m *model) nextN(ts Timestamp, n int) ([]Entry[int], ReadInfo) {
	info := m.oldestInfo()
	switch {
	case m.mode == TimeIndexNone:
		info.Status = StatusTimeIndexDisabled
		return nil, info
	case n <= 0:
		info.Status = StatusNotFound
		return nil, info
	}
	gated := m.gate && m.watermark != MaxTimestamp
	all := m.scan(func(e Entry[int]) bool { return e.Meta.EventTime > ts })
	var matches []Entry[int]
	for _, e := range all {
		if !gated || e.Meta.EventTime <= m.watermark {
			matches = append(matches, e)
		}
	}
	lim := min(n, m.maxBatch)
	if len(matches) > lim {
		matches, info.Truncated = matches[:lim], true
	}
	info.N, info.Evicted = len(matches), m.evicted && m.maxEvi > ts
	switch {
	case len(matches) > 0:
		info.Status, info.LastRef = StatusOK, matches[len(matches)-1].Meta.Ref
		return matches, info
	case gated && (len(all) > 0 || !info.Evicted):
		info.Status = StatusNotReady
	case info.Evicted:
		info.Status = StatusExpired
	default:
		info.Status = StatusNotFound
	}
	return nil, info
}

// randomQueryTime picks query timestamps near the data: retained EventTimes
// (for exact matches), neighbours, the recent range and the sentinels.
func randomQueryTime(r *rand.Rand, m *model, g *inputGen) Timestamp {
	switch n := r.IntN(10); {
	case n == 0:
		return MinTimestamp
	case n == 1:
		return MaxTimestamp
	case n < 6 && len(m.recs) > 0:
		return m.recs[r.IntN(len(m.recs))].Meta.EventTime + Timestamp(r.IntN(3)-1)
	case m.evicted && n < 8:
		return []Timestamp{m.minEvict - 1, m.minEvict, m.maxEvi, m.maxEvi + 1}[r.IntN(4)]
	default:
		return g.now + Timestamp(r.IntN(70)-60)
	}
}

func (m *model) clear() error {
	if m.closed {
		return ErrClosed
	}
	m.gen++
	m.recs = nil
	m.slots = nil
	m.nextSeq = 1
	m.evicted, m.minEvict, m.maxEvi = false, 0, 0
	m.observed, m.maxObserved = false, 0
	m.trkValid, m.trkEpoch, m.trkLast = false, 0, 0
	m.watermark = MinTimestamp
	m.clears++
	return nil
}

func (m *model) setWatermark(ts Timestamp) bool {
	if m.closed || ts <= m.watermark {
		return false
	}
	m.watermark = ts
	return true
}

func (m *model) stats() Stats {
	st := Stats{
		Name:                 m.name,
		Gen:                  m.gen,
		TimeIndex:            m.mode,
		Capacity:             m.capacity,
		Len:                  len(m.recs),
		Oldest:               m.oldestRef(),
		Newest:               m.newestRef(),
		Appended:             m.appended,
		Evicted:              m.evictedN,
		Dropped:              m.dropped,
		Rejected:             m.rejected,
		Late:                 m.lateN,
		SourceSeqNotIncr:     m.srcN,
		GapFlags:             m.gaps,
		Clears:               m.clears,
		MaxObservedEventTime: MinTimestamp,
		HasEvicted:           m.evicted,
		MinEvicted:           m.minEvict,
		MaxEvicted:           m.maxEvi,
		Watermark:            m.watermark,
		Indexes:              m.indexStats(),
	}
	if m.observed {
		st.MaxObservedEventTime = m.maxObserved
	}
	if m.mode != TimeIndexNone && len(m.recs) > 0 {
		st.MinEventTime, st.MaxEventTime = MaxTimestamp, MinTimestamp
		for _, e := range m.recs {
			st.MinEventTime = min(st.MinEventTime, e.Meta.EventTime)
			st.MaxEventTime = max(st.MaxEventTime, e.Meta.EventTime)
		}
	}
	return st
}

// satAdd adds d to ts, clamping to the sentinels.
func satAdd(ts Timestamp, d int64) Timestamp {
	switch {
	case d > 0 && ts > MaxTimestamp-Timestamp(d):
		return MaxTimestamp
	case d < 0 && ts < MinTimestamp-Timestamp(d):
		return MinTimestamp
	}
	return ts + Timestamp(d)
}

// runBatchOp runs one random event-time batch read against the Series and the
// model. It returns a non-empty name and a description on mismatch.
func runBatchOp(
	t *testing.T,
	r *rand.Rand,
	s *Series[int],
	m *model,
	g *inputGen,
	cursors *[]TimeCursor,
	seen map[string]int,
) (string, string) {
	t.Helper()
	limit := r.IntN(8) - 1
	prefix := r.IntN(2)
	dst := make([]Entry[int], prefix, prefix+r.IntN(4))
	vdst := make([]int, prefix, prefix+r.IntN(4))
	for i := range prefix {
		dst[i] = Entry[int]{Value: -7}
		vdst[i] = -7
	}

	var name string
	var got []Entry[int]
	var gotVals []int
	var gotInfo, wantInfo ReadInfo
	var want []Entry[int]
	values := false

	switch kind := r.IntN(7); kind {
	case 0, 1:
		from := randomQueryTime(r, m, g)
		to := satAdd(from, int64(r.IntN(50)-5))
		if r.IntN(8) == 0 {
			to = randomQueryTime(r, m, g)
		}
		want, wantInfo = m.rangeQ(from, to, limit)
		if kind == 0 {
			name = fmt.Sprintf("Range(%d, %d, %d)", from, to, limit)
			got, gotInfo = s.Range(dst, from, to, limit)
		} else {
			name = fmt.Sprintf("RangeValues(%d, %d, %d)", from, to, limit)
			gotVals, gotInfo = s.RangeValues(vdst, from, to, limit)
			values = true
		}
	case 2:
		ts := randomQueryTime(r, m, g)
		name = fmt.Sprintf("AtAll(%d, %d)", ts, limit)
		want, wantInfo = m.atAll(ts, limit)
		got, gotInfo = s.AtAll(dst, ts, limit)
	case 3, 4:
		var c TimeCursor
		switch n := r.IntN(10); {
		case n < 7 && len(*cursors) > 0:
			c = (*cursors)[r.IntN(len(*cursors))]
		case n < 8:
			c = makeCursor(m.sid+1_000_000, m.gen, timeKey{g.now, 1}, MaxTimestamp-1) // foreign
		case n < 9:
			c = makeCursor(m.sid, m.gen+1, timeKey{g.now, 1}, MaxTimestamp-1) // future generation
		}
		want, wantInfo = m.continueQ(c, limit)
		if kind == 3 {
			name = fmt.Sprintf("Continue(%+v, %d)", c, limit)
			got, gotInfo = s.Continue(dst, c, limit)
		} else {
			name = fmt.Sprintf("ContinueValues(%+v, %d)", c, limit)
			gotVals, gotInfo = s.ContinueValues(vdst, c, limit)
			values = true
		}
	case 5:
		ts, n := randomQueryTime(r, m, g), r.IntN(8)-1
		name = fmt.Sprintf("PrevN(%d, %d)", ts, n)
		want, wantInfo = m.prevN(ts, n)
		got, gotInfo = s.PrevN(dst, ts, n)
		if gotInfo.N > 0 {
			if e, _ := s.AsOf(ts); e != got[len(got)-1] && !(m.gate && ts > m.watermark) {
				return name, fmt.Sprintf("last entry %+v != AsOf %+v (Q-006)", got[len(got)-1], e)
			}
			for _, e := range got[prefix:] {
				if e.Meta.EventTime > ts {
					return name, "looked ahead (§16.3)"
				}
			}
		}
	default:
		ts, n := randomQueryTime(r, m, g), r.IntN(8)-1
		name = fmt.Sprintf("NextN(%d, %d)", ts, n)
		want, wantInfo = m.nextN(ts, n)
		got, gotInfo = s.NextN(dst, ts, n)
	}

	if gotInfo != wantInfo {
		return name, fmt.Sprintf("info %+v, want %+v", gotInfo, wantInfo)
	}
	seen[gotInfo.Status.String()]++
	if gotInfo.Truncated {
		seen["truncated"]++
	}
	if strings.HasPrefix(name, "Continue") && gotInfo.Status == StatusOK {
		seen["continued"]++
	}
	if !gotInfo.Next.IsZero() {
		*cursors = append(*cursors, gotInfo.Next)
	}
	if values {
		if len(gotVals) != prefix+len(want) || (prefix > 0 && gotVals[0] != -7) {
			return name, fmt.Sprintf("values %v, want prefix %d + %d entries", gotVals, prefix, len(want))
		}
		for i, e := range want {
			if gotVals[prefix+i] != e.Value {
				return name, fmt.Sprintf("values %v, want %+v", gotVals[prefix:], want)
			}
		}
		return "", ""
	}
	if len(got) != prefix+len(want) || (prefix > 0 && got[0].Value != -7) {
		return name, fmt.Sprintf("entries %+v, want prefix %d + %+v", got, prefix, want)
	}
	if len(want) > 0 && !slices.Equal(got[prefix:], want) {
		return name, fmt.Sprintf("entries %+v, want %+v", got[prefix:], want)
	}
	return "", ""
}

// gen produces random but feed-like inputs: mostly increasing event times
// with jitter, late bursts, duplicates, sentinels and forbidden flags.
type inputGen struct {
	r      *rand.Rand
	now    Timestamp
	srcSeq uint64
	epoch  SourceEpoch
	value  int
}

func (g *inputGen) meta() InputMeta {
	r := g.r
	switch r.IntN(100) {
	case 0:
		return InputMeta{EventTime: MinTimestamp}
	case 1:
		return InputMeta{EventTime: MaxTimestamp}
	case 2:
		return InputMeta{EventTime: g.now, Flags: FlagLate}
	case 3:
		return InputMeta{EventTime: g.now, Flags: Flags(1) << (16 + r.IntN(16))}
	case 4: // near the bottom sentinel, to exercise late-threshold saturation
		g.now = MinTimestamp + Timestamp(1+r.IntN(5))
	case 5, 6, 7: // late burst
		g.now -= Timestamp(r.IntN(50))
	default:
		g.now += Timestamp(r.IntN(12) - 2)
	}
	if g.now == MinTimestamp || g.now == MaxTimestamp {
		g.now = 0
	}

	m := InputMeta{EventTime: g.now, ReceiveTime: Timestamp(r.IntN(3)), Flags: Flags(r.IntN(16)) & AdapterFlagsMask}
	if r.IntN(10) == 0 {
		m.Flags |= Flags(1) << (4 + r.IntN(12))
	}
	if r.IntN(25) == 0 {
		g.epoch++
		g.srcSeq = uint64(r.IntN(3))
	}
	if r.IntN(4) != 0 {
		g.srcSeq += uint64(r.IntN(4)) - 1 // may repeat or go backwards
		m.SourceSeq = g.srcSeq
	}
	m.SourceEpoch = g.epoch
	return m
}

func (g *inputGen) nextValue() int {
	g.value++
	return g.value
}

// randomConfig draws a small-capacity config so eviction happens constantly.
func randomConfig(r *rand.Rand, mode TimeIndexMode) Config[int] {
	tolerances := []int64{0, 0, 3, 40, math.MaxInt64}
	return Config[int]{
		Name:                 fmt.Sprintf("model-%d", r.IntN(1000)),
		Capacity:             1 + r.IntN(8),
		TimeIndex:            mode,
		MaxReadBatch:         r.IntN(6),
		LatePolicy:           LatePolicy(r.IntN(2)),
		LateTolerance:        time.Duration(tolerances[r.IntN(len(tolerances))]),
		SourceSeqPolicy:      SourceSeqPolicy(r.IntN(3)),
		GateReadsByWatermark: r.IntN(2) == 0,
	}
}

func randomRef(r *rand.Rand, m *model) Ref {
	if r.IntN(10) == 0 {
		return Ref{}
	}
	g := m.gen + Gen(r.IntN(3)) - 1
	return Ref{Gen: g, Seq: Seq(r.IntN(int(m.nextSeq) + 3))}
}

// sameErr compares error identity: both nil, or both matching want.
func sameErr(got, want error) bool {
	if want == nil {
		return got == nil
	}
	return errors.Is(got, want)
}

// TestModel_SeqPath is the T-002 property test for the Seq path: random
// operations on a real Series and the model must agree on every result,
// status, ReadInfo, entry and Stats, and CheckInvariants must hold throughout.
func TestModel_SeqPath(t *testing.T) {
	for _, mode := range allModes {
		t.Run(fmt.Sprintf("mode=%d", mode), func(t *testing.T) {
			r, _ := newRand(t)
			seen := map[Status]int{}
			batchSeen := map[string]int{}
			for run := range iters(200) {
				runSeqModel(t, r, mode, run, 0, seen, batchSeen)
			}
			wantBatch := []string{"OK", "NotFound", "Expired", "NotReady", "Invalid", "Reset", "truncated", "continued"}
			if mode == TimeIndexNone {
				wantBatch = []string{"TimeIndexDisabled"}
			}
			wantBatch = append(wantBatch, "dropped", "replaced/keptFirst", "conflict", "extractor panic",
				"batch ok", "batch failed", "sub lag", "sub reset", "sub skip")
			for _, k := range wantBatch {
				if batchSeen[k] == 0 && iters(200) >= 200 {
					t.Errorf("batch reads never produced %q (seen %v)", k, batchSeen)
				}
			}
			// Guard against a vacuous run: every point-query status must occur.
			// Only at full iteration counts: rare outcomes may legitimately not
			// occur in the reduced -short and -race runs.
			want := []Status{StatusOK, StatusNotFound, StatusExpired, StatusNotReady}
			if mode == TimeIndexNone {
				want = []Status{StatusTimeIndexDisabled}
			}
			for _, st := range want {
				if seen[st] == 0 && iters(200) >= 200 {
					t.Errorf("point queries never returned %v (seen %v)", st, seen)
				}
			}
		})
	}
}

// runSeqModel runs one random scenario of steps operations (0 = random
// length) against a real Series and the model.
func runSeqModel(
	t *testing.T,
	r *rand.Rand,
	mode TimeIndexMode,
	run int,
	steps int,
	seen map[Status]int,
	batchSeen map[string]int,
) {
	t.Helper()
	cfg := randomConfig(r, mode)
	m := newModel(cfg)
	handles := randomIndexes(r, &cfg, m)
	s := mustNew(t, cfg)
	m.sid = s.id
	var cursors []TimeCursor // every cursor returned so far, including stale ones
	var subs []subPair
	g := &inputGen{r: r, now: Timestamp(r.IntN(1000))}
	var buf []Entry[int]

	fail := func(step int, op string, format string, args ...any) {
		t.Helper()
		t.Fatalf("run %d step %d (%s), cfg %+v: %s", run, step, op, cfg, fmt.Sprintf(format, args...))
	}

	if steps == 0 {
		steps = 50 + r.IntN(250)
	}
	for step := range steps {
		switch op := r.IntN(100); {
		case op < 34:
			v, in := g.nextValue(), g.meta()
			var got AppendResult
			gotErr := catchPanic(func() (err error) { got, err = s.Append(v, in); return err })
			want, wantErr := m.append(v, in)
			if !errMatches(gotErr, wantErr) || got != want {
				fail(step, "Append", "got (%+v, %v), want (%+v, %v) for %+v", got, gotErr, want, wantErr, in)
			}
			switch {
			case got.Outcome == OutcomeDropped:
				batchSeen["dropped"]++
			case got.Index != "":
				batchSeen["replaced/keptFirst"]++
			case errors.Is(gotErr, ErrUniqueConflict):
				batchSeen["conflict"]++
			case errors.As(gotErr, new(modelPanic)):
				batchSeen["extractor panic"]++
			}
		case op < 40:
			items := make([]Input[int], r.IntN(cfg.Capacity+2))
			for i := range items {
				items[i] = Input[int]{Value: g.nextValue(), Meta: g.meta()}
			}
			dst := []AppendResult{{Index: "prefix"}}
			var got []AppendResult
			gotErr := catchPanic(func() (err error) { got, err = s.AppendBatch(dst, items); return err })
			if gotErr != nil {
				got = dst // a panic returns nothing; errors return dst unchanged
			}
			want, wantErr := m.appendBatch(items)
			if !errMatches(gotErr, wantErr) || got[0].Index != "prefix" || !slices.Equal(got[1:], want) {
				fail(step, "AppendBatch", "%d items: got (%+v, %v), want (%+v, %v)", len(items), got, gotErr, want, wantErr)
			}
			switch {
			case gotErr != nil:
				batchSeen["batch failed"]++
			case len(items) > 1:
				batchSeen["batch ok"]++
			}
		case op < 45:
			ref := randomRef(r, m)
			got, gotSt := s.Get(ref)
			want, wantSt := m.get(ref)
			if got != want || gotSt != wantSt {
				fail(step, "Get", "ref %+v: got (%+v, %v), want (%+v, %v)", ref, got, gotSt, want, wantSt)
			}
		case op < 49:
			if name, msg := runIndexLookup(r, m, handles); name != "" {
				fail(step, name, "%s", msg)
			}
		case op < 51:
			got, gotSt := s.Latest()
			want, wantSt := m.latest()
			if got != want || gotSt != wantSt {
				fail(step, "Latest", "got (%+v, %v), want (%+v, %v)", got, gotSt, want, wantSt)
			}
		case op < 59:
			from, limit := randomRef(r, m), r.IntN(8)-1
			prefix := r.IntN(3)
			buf = buf[:0]
			for i := range prefix {
				buf = append(buf, Entry[int]{Value: -1 - i})
			}
			got, gotInfo := s.ReadFrom(buf, from, limit)
			want, wantInfo := m.readFrom(from, limit)
			if gotInfo != wantInfo {
				fail(step, "ReadFrom", "from %+v limit %d: info %+v, want %+v", from, limit, gotInfo, wantInfo)
			}
			if len(got) != prefix+len(want) {
				fail(step, "ReadFrom", "len %d, want %d", len(got), prefix+len(want))
			}
			for i := range prefix {
				if got[i].Value != -1-i {
					fail(step, "ReadFrom", "prefix entry %d overwritten", i)
				}
			}
			if !reflect.DeepEqual(got[prefix:], want) && len(want) > 0 {
				fail(step, "ReadFrom", "entries %+v, want %+v", got[prefix:], want)
			}
			buf = got
		case op < 72:
			q, ts := pointQuery(r.IntN(int(numPointQueries))), randomQueryTime(r, m, g)
			got, gotSt := q.run(s, ts)
			want, wantSt := m.point(q, ts)
			seen[gotSt]++
			if got != want || gotSt != wantSt {
				fail(step, q.String(), "ts %d: got (%+v, %v), want (%+v, %v)", ts, got, gotSt, want, wantSt)
			}
			if gotSt == StatusOK && (q == qAsOf || q == qLastAt) && got.Meta.EventTime > ts {
				fail(step, q.String(), "looked ahead: EventTime %d > ts %d", got.Meta.EventTime, ts) // §16.3
			}
		case op < 85:
			name, next := runBatchOp(t, r, s, m, g, &cursors, batchSeen)
			if name != "" {
				fail(step, name, "%s", next)
			}
		case op < 89:
			if name, msg := runSubOp(r, s, m, &subs, batchSeen); name != "" {
				fail(step, name, "%s", msg)
			}
		case op < 92:
			if gotErr, wantErr := s.Clear(), m.clear(); !sameErr(gotErr, wantErr) {
				fail(step, "Clear", "got %v, want %v", gotErr, wantErr)
			}
		case op < 98:
			ts := g.now + Timestamp(r.IntN(30)-25)
			switch r.IntN(20) {
			case 0:
				ts = MaxTimestamp
			case 1:
				ts = m.watermark
			}
			if got, want := s.SetWatermark(ts), m.setWatermark(ts); got != want {
				fail(step, "SetWatermark", "ts %d: got %v, want %v", ts, got, want)
			}
		default:
			// About 0.3 closes per run whatever its length, so long runs do not
			// spend most of their steps on a closed Series.
			if r.IntN(max(1, steps/15)) == 0 {
				if err := s.Close(); err != nil {
					fail(step, "Close", "%v", err)
				}
				m.closed = true
			}
		}

		want := m.stats()
		want.Subscribers = m.openSubs(subs)
		if got := s.Stats(); !reflect.DeepEqual(got, want) {
			fail(step, "Stats", "got %+v\nwant %+v", got, want)
		}
		if s.Len() != len(m.recs) || s.OldestRef() != m.oldestRef() || s.NewestRef() != m.newestRef() ||
			s.Gen() != m.gen || s.Watermark() != m.watermark {
			fail(step, "accessors", "Len/OldestRef/NewestRef/Gen/Watermark disagree with the model")
		}
		if err := s.CheckInvariants(); err != nil {
			fail(step, "CheckInvariants", "%v", err)
		}
	}
}
