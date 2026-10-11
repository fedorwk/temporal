package temporal

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
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

	gen     Gen
	recs    []Entry[int] // retained, ascending Seq
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

	res := AppendResult{Ref: Ref{Gen: m.gen, Seq: m.nextSeq}, Flags: flags}
	if len(m.recs) == m.capacity {
		old := m.recs[0]
		m.recs = m.recs[1:]
		res.Evicted = old.Meta.Ref
		if !m.evicted {
			m.evicted, m.minEvict, m.maxEvi = true, old.Meta.EventTime, old.Meta.EventTime
		}
		m.minEvict = min(m.minEvict, old.Meta.EventTime)
		m.maxEvi = max(m.maxEvi, old.Meta.EventTime)
		m.evictedN++
	}
	all := in.Flags | flags
	m.recs = append(m.recs, Entry[int]{Value: v, Meta: Meta{
		EventTime:   in.EventTime,
		ReceiveTime: in.ReceiveTime,
		SourceSeq:   in.SourceSeq,
		SourceEpoch: in.SourceEpoch,
		Flags:       all,
		Ref:         res.Ref,
	}})
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

func (m *model) clear() error {
	if m.closed {
		return ErrClosed
	}
	m.gen++
	m.recs = nil
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
	}
	if m.observed {
		st.MaxObservedEventTime = m.maxObserved
	}
	return st
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
		Name:            fmt.Sprintf("model-%d", r.IntN(1000)),
		Capacity:        1 + r.IntN(8),
		TimeIndex:       mode,
		MaxReadBatch:    r.IntN(6),
		LatePolicy:      LatePolicy(r.IntN(2)),
		LateTolerance:   time.Duration(tolerances[r.IntN(len(tolerances))]),
		SourceSeqPolicy: SourceSeqPolicy(r.IntN(3)),
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
			for run := range iters(200) {
				runSeqModel(t, r, mode, run)
			}
		})
	}
}

func runSeqModel(t *testing.T, r *rand.Rand, mode TimeIndexMode, run int) {
	t.Helper()
	cfg := randomConfig(r, mode)
	s := mustNew(t, cfg)
	m := newModel(cfg)
	g := &inputGen{r: r, now: Timestamp(r.IntN(1000))}
	var buf []Entry[int]

	fail := func(step int, op string, format string, args ...any) {
		t.Helper()
		t.Fatalf("run %d step %d (%s), cfg %+v: %s", run, step, op, cfg, fmt.Sprintf(format, args...))
	}

	steps := 50 + r.IntN(250)
	for step := range steps {
		switch op := r.IntN(100); {
		case op < 55:
			v, in := g.nextValue(), g.meta()
			got, gotErr := s.Append(v, in)
			want, wantErr := m.append(v, in)
			if !sameErr(gotErr, wantErr) || got != want {
				fail(step, "Append", "got (%+v, %v), want (%+v, %v) for %+v", got, gotErr, want, wantErr, in)
			}
		case op < 70:
			ref := randomRef(r, m)
			got, gotSt := s.Get(ref)
			want, wantSt := m.get(ref)
			if got != want || gotSt != wantSt {
				fail(step, "Get", "ref %+v: got (%+v, %v), want (%+v, %v)", ref, got, gotSt, want, wantSt)
			}
		case op < 75:
			got, gotSt := s.Latest()
			want, wantSt := m.latest()
			if got != want || gotSt != wantSt {
				fail(step, "Latest", "got (%+v, %v), want (%+v, %v)", got, gotSt, want, wantSt)
			}
		case op < 88:
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
		case op < 91:
			if gotErr, wantErr := s.Clear(), m.clear(); !sameErr(gotErr, wantErr) {
				fail(step, "Clear", "got %v, want %v", gotErr, wantErr)
			}
		case op < 98:
			ts := m.watermark + Timestamp(r.IntN(7)-2)
			if got, want := s.SetWatermark(ts), m.setWatermark(ts); got != want {
				fail(step, "SetWatermark", "ts %d: got %v, want %v", ts, got, want)
			}
		default:
			if r.IntN(4) == 0 {
				if err := s.Close(); err != nil {
					fail(step, "Close", "%v", err)
				}
				m.closed = true
			}
		}

		if got, want := s.Stats(), m.stats(); !reflect.DeepEqual(got, want) {
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
