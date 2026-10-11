package temporal

// Event-time point reads (§7.2, §7.5, §19, P-13). Each returns
// StatusTimeIndexDisabled first on a TimeIndexNone series, then StatusNotReady
// when the watermark gate applies, then OK, Expired or NotFound according to
// the data and the eviction envelope.

// Envelope tests (§7.2). Exact for the ordered predicates; conservative for
// equality.

func (s *Series[T]) expiredLE(ts Timestamp) bool { return s.env.has && s.env.min <= ts }
func (s *Series[T]) expiredLT(ts Timestamp) bool { return s.env.has && s.env.min < ts }
func (s *Series[T]) expiredGT(ts Timestamp) bool { return s.env.has && s.env.max > ts }
func (s *Series[T]) expiredGE(ts Timestamp) bool { return s.env.has && s.env.max >= ts }
func (s *Series[T]) expiredEQ(ts Timestamp) bool {
	return s.env.has && s.env.min <= ts && ts <= s.env.max
}

// missStatus is the status of a lookup with no retained match.
func missStatus(expired bool) Status {
	if expired {
		return StatusExpired
	}
	return StatusNotFound
}

// AsOf returns the retained record with the greatest (EventTime, Seq) such that
// EventTime <= ts: among records at the same time, the latest ingested. It
// never looks ahead.
func (s *Series[T]) AsOf(ts Timestamp) (Entry[T], Status) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var e Entry[T]
	switch {
	case s.time == nil:
		return e, StatusTimeIndexDisabled
	case s.gatedAt(ts):
		return e, StatusNotReady
	}
	k, ok := s.time.AsOf(ts)
	if !ok {
		return e, missStatus(s.expiredLE(ts))
	}
	s.copyOut(&e, k.seq)
	return e, StatusOK
}

// Before returns the retained record with the greatest (EventTime, Seq) such
// that EventTime < ts.
func (s *Series[T]) Before(ts Timestamp) (Entry[T], Status) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var e Entry[T]
	switch {
	case s.time == nil:
		return e, StatusTimeIndexDisabled
	case s.gatedAt(ts):
		return e, StatusNotReady
	}
	k, ok := s.time.Before(ts)
	if !ok {
		return e, missStatus(s.expiredLT(ts))
	}
	s.copyOut(&e, k.seq)
	return e, StatusOK
}

// After returns the retained record with the smallest (EventTime, Seq) such
// that EventTime > ts.
//
// With watermark gating, only a final answer is returned: a record whose
// EventTime is at or below the watermark, since no late record can arrive in
// front of it. Otherwise After returns StatusNotReady, including when nothing
// matches yet, unless the watermark is MaxTimestamp (series complete) or the
// eviction envelope reports StatusExpired (P-13).
func (s *Series[T]) After(ts Timestamp) (Entry[T], Status) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.time == nil {
		return Entry[T]{}, StatusTimeIndexDisabled
	}
	k, ok := s.time.After(ts)
	return s.forwardResult(k, ok, s.expiredGT(ts))
}

// AtOrAfter returns the retained record with the smallest (EventTime, Seq) such
// that EventTime >= ts. Watermark gating works as for After.
func (s *Series[T]) AtOrAfter(ts Timestamp) (Entry[T], Status) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.time == nil {
		return Entry[T]{}, StatusTimeIndexDisabled
	}
	k, ok := s.time.AtOrAfter(ts)
	return s.forwardResult(k, ok, s.expiredGE(ts))
}

// forwardResult applies P-13 to a forward point lookup.
func (s *Series[T]) forwardResult(k timeKey, ok, expired bool) (Entry[T], Status) {
	var e Entry[T]
	switch {
	case ok && s.finalAt(k.eventTime):
		s.copyOut(&e, k.seq)
		return e, StatusOK
	case ok, !expired && !s.complete():
		return e, StatusNotReady
	default:
		return e, missStatus(expired)
	}
}

// FirstAt returns the retained record with EventTime == ts and the smallest
// Seq. Expired is conservative: ts lies within the evicted EventTime range.
func (s *Series[T]) FirstAt(ts Timestamp) (Entry[T], Status) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var e Entry[T]
	switch {
	case s.time == nil:
		return e, StatusTimeIndexDisabled
	case s.gatedAt(ts):
		return e, StatusNotReady
	}
	k, ok := s.time.AtOrAfter(ts)
	if !ok || k.eventTime != ts {
		return e, missStatus(s.expiredEQ(ts))
	}
	s.copyOut(&e, k.seq)
	return e, StatusOK
}

// LastAt returns the retained record with EventTime == ts and the largest Seq.
// Expired is conservative, as for FirstAt.
func (s *Series[T]) LastAt(ts Timestamp) (Entry[T], Status) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var e Entry[T]
	switch {
	case s.time == nil:
		return e, StatusTimeIndexDisabled
	case s.gatedAt(ts):
		return e, StatusNotReady
	}
	k, ok := s.time.AsOf(ts)
	if !ok || k.eventTime != ts {
		return e, missStatus(s.expiredEQ(ts))
	}
	s.copyOut(&e, k.seq)
	return e, StatusOK
}

// LatestByEventTime returns the retained record with the greatest
// (EventTime, Seq). It is not gated by the watermark.
func (s *Series[T]) LatestByEventTime() (Entry[T], Status) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var e Entry[T]
	if s.time == nil {
		return e, StatusTimeIndexDisabled
	}
	k, ok := s.time.Max()
	if !ok {
		return e, StatusNotFound
	}
	s.copyOut(&e, k.seq)
	return e, StatusOK
}
