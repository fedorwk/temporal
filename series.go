package temporal

import (
	"fmt"
	"math"
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
	idx  []Index[T]
	adm  admission // retained bounds, late and SourceSeq trackers
	gen  Gen
	id   uint64

	// Series-owned scratch, valid only under mu. Allocated only when time != nil.
	keyScratch []timeKey  // index query results, cap MaxReadBatch+1
	delKeys    []timeKey  // commit-phase evictions
	insKeys    []timeKey  // commit-phase insertions
	xe         Entry[T]   // the entry handed to extractors; zeroed after use
	plans      []itemPlan // AppendBatch prepare results, by batch position
	cloned     []T        // AppendBatch CloneOnWrite values; cleared after use

	env       envelope
	watermark Timestamp
	closed    bool
	closeErr  error         // returned by operations on the closed Series: ErrClosed or *ClosedError
	cause     error         // the CloseWithError cause; nil for a clean close
	wmWait    chan struct{} // closed to wake WaitWatermark callers; nil when none wait
	n         counters

	subs        atomic.Pointer[[]*subscriber] // copy-on-write; read without mu by notify
	dbgNotifies atomic.Uint64                 // notify calls, counted in temporaldebug builds only
}

// New creates a Series. It returns ErrInvalidConfig for a malformed config.
func New[T any](cfg Config[T]) (*Series[T], error) {
	c, err := cfg.validate()
	if err != nil {
		return nil, err
	}
	// Check every handle before binding any, so a failure binds nothing (K-003).
	bindMu.Lock()
	defer bindMu.Unlock()
	for _, ix := range c.indexes {
		if ix.isBound() {
			return nil, fmt.Errorf("%w: index %q", ErrIndexBound, ix.indexName())
		}
	}
	s := &Series[T]{
		cfg:  c,
		ring: newRing[T](c.capacity),
		adm: admission{
			rules: admissionRules{
				capacity:        c.capacity,
				lateTolerance:   c.lateTolerance,
				latePolicy:      c.latePolicy,
				sourceSeqPolicy: c.sourceSeqPolicy,
			},
			maxObserved: MinTimestamp,
		},
		gen:       1,
		id:        seriesIDs.Add(1),
		watermark: MinTimestamp,
	}
	s.time = newTimeIndex(c.timeIndex, &s.ring.metaRing)
	if s.time != nil {
		s.keyScratch = make([]timeKey, 0, c.maxReadBatch+1)
		s.delKeys = make([]timeKey, 0, 1)
		s.insKeys = make([]timeKey, 0, 1)
	}
	s.plans = make([]itemPlan, 0, min(c.capacity, 16))
	s.idx = append([]Index[T](nil), c.indexes...)
	for _, ix := range s.idx {
		ix.bind(s, c.capacity)
	}
	return s, nil
}

// Append validates and stores one record, evicting the oldest record if the
// Series is full. A returned error guarantees that no data state changed.
//
// A panic in Config.Clone or a key extractor propagates to the caller. Those
// callbacks run before any mutation and the lock is released on the way out,
// so a caller that recovers finds the Series unchanged and usable.
func (s *Series[T]) Append(v T, m InputMeta) (AppendResult, error) {
	if s.cfg.cloneOnWrite || len(s.idx) > 0 {
		return s.appendWithCallbacks(&v, &m)
	}
	// No user code runs on this path, so nothing can panic while the lock is
	// held; the explicit unlock and the absence of an extra call layer keep
	// the hot path as cheap as possible.
	s.mu.Lock()
	if s.closed {
		err := s.closeErr
		s.mu.Unlock()
		return AppendResult{}, err
	}
	res, err := s.appendLocked(&v, &m)
	s.mu.Unlock()
	if err == nil && res.Outcome == OutcomeAppended {
		s.notify()
	}
	return res, err
}

// appendWithCallbacks is Append for a Series whose transaction runs user code
// (CloneOnWrite or key extractors): the lock is released by defer, so a
// panicking callback cannot leave it held (S12-01).
func (s *Series[T]) appendWithCallbacks(v *T, m *InputMeta) (res AppendResult, err error) {
	appended := false
	s.mu.Lock()
	defer func() {
		s.mu.Unlock()
		if appended {
			s.notify()
		}
	}()
	if s.closed {
		return AppendResult{}, s.closeErr
	}
	res, err = s.appendLocked(v, m)
	appended = err == nil && res.Outcome == OutcomeAppended
	return res, err
}

// itemPlan is the outcome of the prepare phase for one item.
type itemPlan struct {
	flags       Flags // series-set flags to apply
	drop        bool  // a ConflictDrop index already maps the key
	existing    Seq   // holder reported in AppendResult.Existing
	existingIdx int   // index that produced existing, or -1
}

func (s *Series[T]) appendLocked(v *T, m *InputMeta) (AppendResult, error) {
	flags, err := s.adm.check(m)
	if err != nil {
		s.n.rejected++
		return AppendResult{}, err
	}
	p := itemPlan{flags: flags, existingIdx: -1}
	if s.cfg.cloneOnWrite {
		c := s.cfg.clone(*v)
		v = &c
	}
	if len(s.idx) > 0 {
		if err := s.prepareIndexes(v, m, &p, s.adm.nextSeq(), s.adm.evictBelow(), 0); err != nil {
			s.n.rejected++
			return AppendResult{}, err
		}
		if p.drop {
			return s.commitDrop(&p, 0), nil
		}
	}
	res := s.commit(v, m, p, 0)
	s.applyTime()
	return res, nil
}

// prepareIndexes runs P-03 step 6 for the item at batch position pos: each
// index in Config.Indexes order extracts the key and checks conflicts. Any
// error fails the item; otherwise any Drop drops it (Drop wins, §10.3).
func (s *Series[T]) prepareIndexes(v *T, m *InputMeta, p *itemPlan, seq, evictBelow Seq, pos int) error {
	e := &s.xe
	e.Value = *v
	e.Meta = Meta{
		EventTime:   m.EventTime,
		ReceiveTime: m.ReceiveTime,
		SourceSeq:   m.SourceSeq,
		SourceEpoch: m.SourceEpoch,
		Flags:       m.Flags | p.flags,
		Ref:         Ref{Gen: s.gen, Seq: seq},
	}
	defer func() { s.xe = Entry[T]{} }()

	p.existingIdx = -1
	firstDrop := -1
	var dropHolder Seq
	for i, ix := range s.idx {
		d, err := ix.prepare(e, evictBelow, pos)
		if err != nil {
			return err
		}
		switch d.kind {
		case decDrop:
			if firstDrop < 0 {
				firstDrop, dropHolder = i, d.existing
			}
		case decReplace, decKeepFirst:
			if p.existingIdx < 0 {
				p.existingIdx, p.existing = i, d.existing
			}
		}
	}
	if firstDrop >= 0 {
		p.drop, p.existingIdx, p.existing = true, firstDrop, dropHolder
	}
	return nil
}

// commitDrop records a dropped item: no Seq is consumed and no index changes.
func (s *Series[T]) commitDrop(p *itemPlan, pos int) AppendResult {
	s.n.dropped++
	for _, ix := range s.idx {
		ix.noteDropped(pos)
	}
	return AppendResult{
		Outcome:  OutcomeDropped,
		Flags:    p.flags,
		Existing: Ref{Gen: s.gen, Seq: p.existing},
		Index:    s.idx[p.existingIdx].indexName(),
	}
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

// commit admits one prepared item (admission.advance, as push and observe)
// and stores it. It must not call user code.
func (s *Series[T]) commit(v *T, m *InputMeta, p itemPlan, pos int) AppendResult {
	seq, evicted := s.adm.push()
	s.adm.observe(m)
	res := AppendResult{Outcome: OutcomeAppended, Ref: Ref{Gen: s.gen, Seq: seq}, Flags: p.flags}
	if p.existingIdx >= 0 {
		res.Existing = Ref{Gen: s.gen, Seq: p.existing}
		res.Index = s.idx[p.existingIdx].indexName()
	}

	if evicted != 0 {
		slot := s.ring.slot(evicted)
		for _, ix := range s.idx {
			ix.evict(slot, evicted)
		}
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
	if len(s.idx) > 0 {
		slot := s.ring.slot(seq)
		for _, ix := range s.idx {
			ix.commit(slot, seq, pos)
		}
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

// Clear removes all records and starts a new generation: Gen increments, the
// next Seq is 1, and the eviction envelope, late and SourceSeq trackers and
// watermark reset. Earlier Refs then report StatusReset.
func (s *Series[T]) Clear() error {
	s.mu.Lock()
	if s.closed {
		err := s.closeErr
		s.mu.Unlock()
		return err
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
	for _, ix := range s.idx {
		ix.reset()
	}
	s.adm.reset()
	s.env = envelope{}
	s.watermark = MinTimestamp
	s.n.clears++
	s.mu.Unlock()
	s.notify()
	return nil
}

// Close stops all writes. Reads keep working on retained data. Close always
// returns nil and may be called more than once. It is CloseWithError(nil).
func (s *Series[T]) Close() error { return s.CloseWithError(nil) }

// CloseWithError closes the Series like Close and records cause as the reason.
// Writes on the closed Series, and subscriptions that have drained it, then
// return a *ClosedError that matches both ErrClosed and cause with errors.Is. A
// nil cause is a clean close: those operations return ErrClosed itself. Only
// the first close takes effect; later calls change nothing. CloseWithError
// always returns nil.
func (s *Series[T]) CloseWithError(cause error) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.cause = cause
	s.closeErr = ErrClosed
	if cause != nil {
		s.closeErr = &ClosedError{Cause: cause}
	}
	wake := s.takeWatermarkWaiters()
	s.mu.Unlock()
	if wake != nil {
		close(wake)
	}
	s.notify()
	return nil
}

// Err returns the cause the Series was closed with, or nil if it is open or
// was closed cleanly.
func (s *Series[T]) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cause
}

// Len returns the number of retained records.
func (s *Series[T]) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.adm.b.len()
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
	return s.adm.b.oldestRef(s.gen)
}

// NewestRef returns the newest retained Ref, or the zero Ref when empty.
func (s *Series[T]) NewestRef() Ref {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.adm.b.newestRef(s.gen)
}
