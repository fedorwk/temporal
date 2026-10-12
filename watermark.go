package temporal

import "context"

// SetWatermark declares the series complete through ts. The watermark is
// monotonic: a value <= the current watermark is ignored and returns false.
// SetWatermark on a closed Series returns false. The initial value, and the
// value after Clear, is MinTimestamp. An advance wakes WaitWatermark callers
// and subscriptions with WatermarkUpdates, after the lock is released.
func (s *Series[T]) SetWatermark(ts Timestamp) bool {
	s.mu.Lock()
	if s.closed || ts <= s.watermark {
		s.mu.Unlock()
		return false
	}
	s.watermark = ts
	wake := s.takeWatermarkWaiters()
	s.mu.Unlock()
	if wake != nil {
		close(wake)
	}
	s.notifyWatermark()
	return true
}

// Watermark returns the current watermark.
func (s *Series[T]) Watermark() Timestamp {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.watermark
}

// WaitWatermark blocks until the watermark reaches ts, that is until the Series
// is declared complete through ts, and returns the watermark. It returns early
// with ctx.Err() when ctx is done, and with the error writes would get
// (ErrClosed, or a *ClosedError carrying the cause) when the Series is closed
// below ts. A watermark already at or above ts returns at once, also on a
// closed Series.
//
// Unlike subscriptions, which are not woken by watermark changes, waiters are
// woken by every advance. Waiting allocates one channel per watermark advance
// that has waiters; SetWatermark without waiters allocates nothing.
func (s *Series[T]) WaitWatermark(ctx context.Context, ts Timestamp) (Timestamp, error) {
	for {
		s.mu.Lock()
		wm := s.watermark
		if wm >= ts {
			s.mu.Unlock()
			return wm, nil
		}
		if s.closed {
			err := s.closeErr
			s.mu.Unlock()
			return wm, err
		}
		if s.wmWait == nil {
			s.wmWait = make(chan struct{})
		}
		wake := s.wmWait
		s.mu.Unlock()

		select {
		case <-wake:
		case <-ctx.Done():
			return wm, ctx.Err()
		}
	}
}

// takeWatermarkWaiters detaches the channel WaitWatermark callers block on, so
// the caller can close it after releasing the lock. Callers hold s.mu.
func (s *Series[T]) takeWatermarkWaiters() chan struct{} {
	wake := s.wmWait
	s.wmWait = nil
	return wake
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
