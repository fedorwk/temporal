package temporal

import (
	"fmt"
	"slices"
)

// Preallocated write-path errors so that rejections do not allocate.
var (
	errInvalidEventTime = fmt.Errorf("%w: EventTime is a reserved sentinel", ErrInvalidInput)
	errSeriesFlags      = fmt.Errorf("%w: series-owned flag bits set in InputMeta.Flags", ErrInvalidInput)
)

// copyOut fills dst with the retained record s. Clone runs here for
// CloneOnRead; a panic in it propagates after the caller's deferred Unlock.
func (s *Series[T]) copyOut(dst *Entry[T], seq Seq) {
	m := s.ring.meta(seq)
	dst.Meta = Meta{
		EventTime:   m.eventTime,
		ReceiveTime: m.receiveTime,
		SourceSeq:   m.sourceSeq,
		SourceEpoch: m.epoch,
		Flags:       m.flags,
		Ref:         Ref{Gen: s.gen, Seq: seq},
	}
	v := s.ring.value(seq)
	if s.cfg.cloneOnRead {
		dst.Value = s.cfg.clone(*v)
		return
	}
	dst.Value = *v
}

// effectiveLimit is min(limit, MaxReadBatch); limit <= 0 means MaxReadBatch.
func (s *Series[T]) effectiveLimit(limit int) int {
	if limit <= 0 || limit > s.cfg.maxReadBatch {
		return s.cfg.maxReadBatch
	}
	return limit
}

// Get returns the retained record ref, or why it is not available:
// NotFound (zero, future or later-generation Ref), Expired (evicted in this
// generation) or Reset (earlier generation).
func (s *Series[T]) Get(ref Ref) (Entry[T], Status) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var e Entry[T]
	switch {
	case ref.IsZero(), ref.Gen > s.gen:
		return e, StatusNotFound
	case ref.Gen < s.gen:
		return e, StatusReset
	case ref.Seq > s.b.newest:
		return e, StatusNotFound
	case !s.b.retained(ref.Seq):
		return e, StatusExpired
	}
	s.copyOut(&e, ref.Seq)
	return e, StatusOK
}

// Latest returns the newest-ingested record.
func (s *Series[T]) Latest() (Entry[T], Status) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var e Entry[T]
	if s.b.empty() {
		return e, StatusNotFound
	}
	s.copyOut(&e, s.b.newest)
	return e, StatusOK
}

// ReadFrom appends to dst the records with Seq >= from.Seq in ascending Seq
// order, up to min(limit, MaxReadBatch). A zero from starts at the oldest
// retained record. Continue a replay with ReadFrom(dst, Ref{Gen, LastRef.Seq+1}).
func (s *Series[T]) ReadFrom(dst []Entry[T], from Ref, limit int) ([]Entry[T], ReadInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()

	info := ReadInfo{Oldest: s.b.oldestRef(s.gen)}
	start := from.Seq
	if from.IsZero() {
		start = s.b.oldest
	} else {
		switch {
		case from.Gen < s.gen:
			info.Status = StatusReset
			return dst, info
		case from.Gen > s.gen:
			info.Status = StatusNotFound
			return dst, info
		}
	}

	switch {
	case s.b.empty() && start == 0, start > s.b.newest:
		info.Status = StatusNotFound
		return dst, info
	case !s.b.retained(start):
		info.Status = StatusExpired
		info.Evicted = true
		return dst, info
	}

	avail := int(s.b.newest - start + 1)
	n := min(avail, s.effectiveLimit(limit))
	base := len(dst)
	dst = slices.Grow(dst, n)[:base+n]
	for i := range n {
		s.copyOut(&dst[base+i], start+Seq(i))
	}
	info.Status = StatusOK
	info.N = n
	info.Truncated = avail > n
	info.LastRef = Ref{Gen: s.gen, Seq: start + Seq(n-1)}
	return dst, info
}
