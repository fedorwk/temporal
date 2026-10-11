package temporal

import (
	"math"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

// seriesIDs hands out Series identities. 0 is never assigned, so a zero
// TimeCursor never matches a Series.
var seriesIDs atomic.Uint64

// envelope is the EventTime range of records evicted in the current
// generation (§15.3).
type envelope struct {
	has      bool
	min, max Timestamp
}

func (e *envelope) add(ts Timestamp) {
	if !e.has {
		*e = envelope{has: true, min: ts, max: ts}
		return
	}
	e.min = min(e.min, ts)
	e.max = max(e.max, ts)
}

// sourceSeqTracker remembers the highest SourceSeq seen in the current
// SourceEpoch (§9.2, DECISIONS S2-01).
type sourceSeqTracker struct {
	valid bool
	epoch SourceEpoch
	last  uint64
}

// counters are lifetime statistics.
type counters struct {
	appended, evicted      uint64
	dropped, rejected      uint64
	late, sourceSeqNotIncr uint64
	gapFlags, clears       uint64
}

// Series is a bounded, Seq-ordered store of records with optional event-time
// and key indexes. All methods are safe for concurrent use.
type Series[T any] struct {
	mu sync.Mutex

	cfg  config[T]
	ring ring[T]
	time timeIndex // nil when TimeIndexNone
	b    bounds
	gen  Gen
	id   uint64

	// Series-owned scratch, valid only under mu. Allocated only when time != nil.
	keyScratch []timeKey // index query results, cap MaxReadBatch+1
	delKeys    []timeKey // commit-phase evictions
	insKeys    []timeKey // commit-phase insertions

	env         envelope
	hasObserved bool
	maxObserved Timestamp // largest accepted EventTime in this generation
	srcSeq      sourceSeqTracker
	watermark   Timestamp
	closed      bool
	n           counters
}

// New creates a Series. It returns ErrInvalidConfig for a malformed config.
func New[T any](cfg Config[T]) (*Series[T], error) {
	c, err := cfg.validate()
	if err != nil {
		return nil, err
	}
	s := &Series[T]{
		cfg:       c,
		ring:      newRing[T](c.capacity),
		gen:       1,
		id:        seriesIDs.Add(1),
		watermark: MinTimestamp,
		time:      newTimeIndex(c.timeIndex),
	}
	if s.time != nil {
		s.keyScratch = make([]timeKey, 0, c.maxReadBatch+1)
		s.delKeys = make([]timeKey, 0, 1)
		s.insKeys = make([]timeKey, 0, 1)
	}
	return s, nil
}

// Append validates and stores one record, evicting the oldest record if the
// Series is full. A returned error guarantees that no data state changed.
func (s *Series[T]) Append(v T, m InputMeta) (AppendResult, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return AppendResult{}, ErrClosed
	}
	res, err := s.appendLocked(&v, &m)
	s.mu.Unlock()
	if err == nil {
		s.notify()
	}
	return res, err
}

// itemPlan is the outcome of the prepare phase for one item.
type itemPlan struct {
	flags Flags // series-set flags to apply
}

func (s *Series[T]) appendLocked(v *T, m *InputMeta) (AppendResult, error) {
	p, err := s.prepareMeta(m, s.maxObserved, s.hasObserved, &s.srcSeq, s.b.nextSeq())
	if err != nil {
		s.n.rejected++
		return AppendResult{}, err
	}
	if s.cfg.cloneOnWrite {
		c, err := s.callClone(v)
		if err != nil {
			s.n.rejected++
			return AppendResult{}, err
		}
		v = &c
	}
	res := s.commit(v, m, p)
	s.applyTime()
	return res, nil
}

// applyTime hands the transaction's evicted and inserted keys to the time
// index in one call (W-010) and resets the scratch.
func (s *Series[T]) applyTime() {
	if s.time == nil {
		return
	}
	s.time.Apply(s.delKeys, s.insKeys)
	s.delKeys = s.delKeys[:0]
	s.insKeys = s.insKeys[:0]
}

// prepareMeta runs P-03 steps 1-4 against the given tracker state without
// mutating anything.
func (s *Series[T]) prepareMeta(
	m *InputMeta,
	maxObserved Timestamp,
	hasObserved bool,
	trk *sourceSeqTracker,
	next Seq,
) (itemPlan, error) {
	var p itemPlan
	if m.EventTime == MinTimestamp || m.EventTime == MaxTimestamp {
		return p, errInvalidEventTime
	}
	if m.Flags&SeriesFlagsMask != 0 {
		return p, errSeriesFlags
	}
	if hasObserved && m.EventTime < lateThreshold(maxObserved, s.cfg.lateTolerance) {
		if s.cfg.latePolicy == LateReject {
			return p, ErrLate
		}
		p.flags |= FlagLate
	}
	if s.cfg.sourceSeqPolicy != SourceSeqIgnore && m.SourceSeq != 0 &&
		trk.valid && trk.epoch == m.SourceEpoch && m.SourceSeq <= trk.last {
		if s.cfg.sourceSeqPolicy == SourceSeqReject {
			return p, ErrSourceSeqNotIncreasing
		}
		p.flags |= FlagSourceSeqNotIncreasing
	}
	if next == seqMax {
		return p, ErrSeqExhausted
	}
	return p, nil
}

// lateThreshold is maxObserved - tolerance, saturating at MinTimestamp (P-10).
func lateThreshold(maxObserved, tolerance Timestamp) Timestamp {
	if maxObserved < MinTimestamp+tolerance {
		return MinTimestamp
	}
	return maxObserved - tolerance
}

// commit applies one prepared item. It must not call user code.
func (s *Series[T]) commit(v *T, m *InputMeta, p itemPlan) AppendResult {
	seq := s.b.nextSeq()
	res := AppendResult{Outcome: OutcomeAppended, Ref: Ref{Gen: s.gen, Seq: seq}, Flags: p.flags}

	if evicted, ok := s.b.push(s.cfg.capacity); ok {
		et := s.ring.eventTime(evicted)
		if s.time != nil {
			s.delKeys = append(s.delKeys, timeKey{eventTime: et, seq: evicted})
		}
		s.env.add(et)
		s.n.evicted++
		res.Evicted = Ref{Gen: s.gen, Seq: evicted}
	}

	flags := m.Flags | p.flags
	s.ring.put(seq, v, &slotMeta{
		eventTime:   m.EventTime,
		receiveTime: m.ReceiveTime,
		sourceSeq:   m.SourceSeq,
		epoch:       m.SourceEpoch,
		flags:       flags,
	})
	if s.time != nil {
		s.insKeys = append(s.insKeys, timeKey{eventTime: m.EventTime, seq: seq})
	}

	if !s.hasObserved || m.EventTime > s.maxObserved {
		s.hasObserved = true
		s.maxObserved = m.EventTime
	}
	if s.cfg.sourceSeqPolicy != SourceSeqIgnore && m.SourceSeq != 0 {
		s.srcSeq.observe(m.SourceEpoch, m.SourceSeq)
	}

	s.n.appended++
	if flags&FlagLate != 0 {
		s.n.late++
	}
	if flags&FlagSourceSeqNotIncreasing != 0 {
		s.n.sourceSeqNotIncr++
	}
	if flags&FlagGap != 0 {
		s.n.gapFlags++
	}
	return res
}

// observe records an accepted SourceSeq. A new epoch resets the tracker.
func (t *sourceSeqTracker) observe(epoch SourceEpoch, seq uint64) {
	if !t.valid || t.epoch != epoch {
		*t = sourceSeqTracker{valid: true, epoch: epoch, last: seq}
		return
	}
	t.last = max(t.last, seq)
}

// callClone runs Config.Clone on the write path, converting a panic into a
// *CallbackPanicError.
func (s *Series[T]) callClone(v *T) (out T, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &CallbackPanicError{Callback: "clone", Value: r, Stack: debug.Stack()}
		}
	}()
	return s.cfg.clone(*v), nil
}

// Clear removes all records and starts a new generation: Gen increments, the
// next Seq is 1, and the eviction envelope, late and SourceSeq trackers and
// watermark reset. Earlier Refs then report StatusReset.
func (s *Series[T]) Clear() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	if s.gen == math.MaxUint32 {
		s.mu.Unlock()
		return ErrGenExhausted
	}
	s.gen++
	s.ring.zero()
	if s.time != nil {
		s.time.Clear()
	}
	s.b.reset()
	s.env = envelope{}
	s.hasObserved = false
	s.maxObserved = 0
	s.srcSeq = sourceSeqTracker{}
	s.watermark = MinTimestamp
	s.n.clears++
	s.mu.Unlock()
	s.notify()
	return nil
}

// Close stops all writes. Reads keep working on retained data. Close always
// returns nil and may be called more than once.
func (s *Series[T]) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.notify()
	return nil
}

// notify wakes subscribers. It must be called after the mutex is released.
// Subscriptions arrive in Session 8; until then it does nothing.
func (s *Series[T]) notify() {}

// Len returns the number of retained records.
func (s *Series[T]) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.len()
}

// Cap returns the configured capacity.
func (s *Series[T]) Cap() int { return s.cfg.capacity }

// Gen returns the current generation.
func (s *Series[T]) Gen() Gen {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gen
}

// TimeIndexMode returns the configured event-time ordering mode.
func (s *Series[T]) TimeIndexMode() TimeIndexMode { return s.cfg.timeIndex }

// OldestRef returns the oldest retained Ref, or the zero Ref when empty.
func (s *Series[T]) OldestRef() Ref {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.oldestRef(s.gen)
}

// NewestRef returns the newest retained Ref, or the zero Ref when empty.
func (s *Series[T]) NewestRef() Ref {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.newestRef(s.gen)
}
