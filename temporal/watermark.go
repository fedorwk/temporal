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
