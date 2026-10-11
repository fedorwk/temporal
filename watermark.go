package temporal

// SetWatermark declares the series complete through ts. The watermark is
// monotonic: a value <= the current watermark is ignored and returns false.
// SetWatermark on a closed Series returns false. The initial value, and the
// value after Clear, is MinTimestamp.
func (s *Series[T]) SetWatermark(ts Timestamp) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || ts <= s.watermark {
		return false
	}
	s.watermark = ts
	return true
}

// Watermark returns the current watermark.
func (s *Series[T]) Watermark() Timestamp {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.watermark
}

// Read gating (§19, P-10, P-13). Every predicate is false unless
// Config.GateReadsByWatermark is set. Callers hold s.mu.

// gatedAt reports whether a backward read bounded by ts (inclusive) must
// return StatusNotReady: AsOf, Before, LastAt, FirstAt, AtAll, PrevN.
func (s *Series[T]) gatedAt(ts Timestamp) bool {
	return s.cfg.gate && ts > s.watermark
}

// gatedBelow reports whether a read with exclusive upper bound `to` must return
// StatusNotReady: Range and Continue. `to - 1` is never computed for
// MinTimestamp, so it cannot overflow.
func (s *Series[T]) gatedBelow(to Timestamp) bool {
	return s.cfg.gate && to != MinTimestamp && to-1 > s.watermark
}

// finalAt reports whether a record found by a forward read (After, AtOrAfter,
// NextN) may be returned: with gating on, only records at or below the
// watermark are final (P-13).
func (s *Series[T]) finalAt(eventTime Timestamp) bool {
	return !s.cfg.gate || eventTime <= s.watermark
}

// complete reports whether no record can still arrive that would change a
// forward read: gating is off, or the watermark is MaxTimestamp (P-13, S4-01).
func (s *Series[T]) complete() bool {
	return !s.cfg.gate || s.watermark == MaxTimestamp
}
