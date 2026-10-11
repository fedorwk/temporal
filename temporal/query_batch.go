package temporal

import (
	"iter"
	"slices"
)

// Event-time batch reads (§7.4, §8, P-05, P-13, P-14). Each read asks the time
// index for keys in the Series-owned keyScratch, then copies the matching ring
// slots into the caller's dst. Status precedence: Disabled, Invalid and Reset
// (cursors only), NotReady, then OK, Expired or NotFound from the data.

// copyEntries appends the records named by keys to dst.
func (s *Series[T]) copyEntries(dst []Entry[T], keys []timeKey) []Entry[T] {
	base := len(dst)
	dst = growEntries(dst, len(keys))
	for i, k := range keys {
		s.copyOut(&dst[base+i], k.seq)
	}
	return dst
}

// copyValues appends the values named by keys to dst.
func (s *Series[T]) copyValues(dst []T, keys []timeKey) []T {
	base := len(dst)
	dst = slices.Grow(dst, len(keys))[:base+len(keys)]
	for i, k := range keys {
		v := s.ring.value(k.seq)
		if s.cfg.cloneOnRead {
			dst[base+i] = s.cfg.clone(*v)
		} else {
			dst[base+i] = *v
		}
	}
	return dst
}

// evictedIn reports whether the eviction envelope intersects [from, to).
func (s *Series[T]) evictedIn(from, to Timestamp) bool {
	return s.env.has && s.env.min < to && s.env.max >= from
}

// ascendPlan describes an ascending, continuable read: keys >= start with
// EventTime < bound. evicted is the P-05 Evicted flag for the read.
type ascendPlan struct {
	start   timeKey
	bound   Timestamp
	evicted bool
}

// ascendKeys runs plan into keyScratch and fills info. The caller copies the
// returned keys out before releasing the lock.
func (s *Series[T]) ascendKeys(p ascendPlan, limit int, info *ReadInfo) []timeKey {
	keys, more := s.time.Ascend(s.keyScratch[:0], p.start, p.bound, s.effectiveLimit(limit))
	info.N = len(keys)
	info.Evicted = p.evicted
	info.Truncated = more
	switch {
	case len(keys) > 0:
		info.Status = StatusOK
		last := keys[len(keys)-1]
		info.LastRef = Ref{Gen: s.gen, Seq: last.seq}
		if more {
			info.Next = makeCursor(s.id, s.gen, last, p.bound)
		}
	default:
		info.Status = missStatus(p.evicted)
	}
	return keys
}

// rangePlan validates a Range read and returns its plan, or a final status.
func (s *Series[T]) rangePlan(from, to Timestamp) (ascendPlan, Status) {
	switch {
	case s.time == nil:
		return ascendPlan{}, StatusTimeIndexDisabled
	case s.gatedBelow(to):
		return ascendPlan{}, StatusNotReady
	case from >= to:
		return ascendPlan{}, StatusNotFound // E-002
	}
	return ascendPlan{
		start:   timeKey{eventTime: from, seq: 0},
		bound:   to,
		evicted: s.evictedIn(from, to),
	}, StatusOK
}

// atAllPlan validates an AtAll read: Ascend({ts, 0}, ts+1).
func (s *Series[T]) atAllPlan(ts Timestamp) (ascendPlan, Status) {
	switch {
	case s.time == nil:
		return ascendPlan{}, StatusTimeIndexDisabled
	case s.gatedAt(ts):
		return ascendPlan{}, StatusNotReady
	case ts == MaxTimestamp:
		return ascendPlan{}, StatusNotFound // the sentinel is never stored
	}
	return ascendPlan{
		start:   timeKey{eventTime: ts, seq: 0},
		bound:   ts + 1,
		evicted: s.evictedIn(ts, ts+1),
	}, StatusOK
}

// continuePlan validates a cursor (§8.4, P-14) and resumes strictly after the
// last returned key, below the original bound.
func (s *Series[T]) continuePlan(c TimeCursor) (ascendPlan, Status) {
	switch {
	case s.time == nil:
		return ascendPlan{}, StatusTimeIndexDisabled
	case c.IsZero(), c.series != s.id, c.gen > s.gen:
		return ascendPlan{}, StatusInvalid
	case c.gen < s.gen:
		return ascendPlan{}, StatusReset
	}
	last, bound := c.decode()
	if s.gatedBelow(bound) {
		return ascendPlan{}, StatusNotReady
	}
	return ascendPlan{
		// last.seq+1 cannot overflow: math.MaxUint64 is never assigned.
		start:   timeKey{eventTime: last.eventTime, seq: last.seq + 1},
		bound:   bound,
		evicted: s.evictedIn(last.eventTime, bound),
	}, StatusOK
}

// Range appends to dst the records with from <= EventTime < to, ascending by
// (EventTime, Seq), up to min(limit, MaxReadBatch). A truncated read returns a
// cursor in ReadInfo.Next for Continue.
func (s *Series[T]) Range(dst []Entry[T], from, to Timestamp, limit int) ([]Entry[T], ReadInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, st := s.rangePlan(from, to)
	return s.readEntries(dst, p, st, limit)
}

// RangeValues is Range returning values only.
func (s *Series[T]) RangeValues(dst []T, from, to Timestamp, limit int) ([]T, ReadInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, st := s.rangePlan(from, to)
	return s.readValues(dst, p, st, limit)
}

// AtAll appends to dst every record with EventTime == ts, ascending by Seq, up
// to min(limit, MaxReadBatch). A truncated read returns a cursor.
func (s *Series[T]) AtAll(dst []Entry[T], ts Timestamp, limit int) ([]Entry[T], ReadInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, st := s.atAllPlan(ts)
	return s.readEntries(dst, p, st, limit)
}

// Continue returns the next page of a truncated Range, RangeValues, AtAll or
// Continue read: records strictly after the last returned one in
// (EventTime, Seq) order, below the original upper bound. A zero or foreign
// cursor returns StatusInvalid; one from an earlier generation, StatusReset.
func (s *Series[T]) Continue(dst []Entry[T], c TimeCursor, limit int) ([]Entry[T], ReadInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, st := s.continuePlan(c)
	return s.readEntries(dst, p, st, limit)
}

// ContinueValues is Continue returning values only.
func (s *Series[T]) ContinueValues(dst []T, c TimeCursor, limit int) ([]T, ReadInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, st := s.continuePlan(c)
	return s.readValues(dst, p, st, limit)
}

func (s *Series[T]) readEntries(dst []Entry[T], p ascendPlan, st Status, limit int) ([]Entry[T], ReadInfo) {
	info := ReadInfo{Status: st, Oldest: s.b.oldestRef(s.gen)}
	if st != StatusOK {
		return dst, info
	}
	keys := s.ascendKeys(p, limit, &info)
	return s.copyEntries(dst, keys), info
}

func (s *Series[T]) readValues(dst []T, p ascendPlan, st Status, limit int) ([]T, ReadInfo) {
	info := ReadInfo{Status: st, Oldest: s.b.oldestRef(s.gen)}
	if st != StatusOK {
		return dst, info
	}
	keys := s.ascendKeys(p, limit, &info)
	return s.copyValues(dst, keys), info
}

// PrevN appends to dst the n records with the greatest (EventTime, Seq) such
// that EventTime <= ts, in ascending order. Its last entry equals AsOf(ts).
// n <= 0 returns StatusNotFound; n is capped by MaxReadBatch. PrevN returns no
// cursor.
func (s *Series[T]) PrevN(dst []Entry[T], ts Timestamp, n int) ([]Entry[T], ReadInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	info := ReadInfo{Oldest: s.b.oldestRef(s.gen)}
	switch {
	case s.time == nil:
		info.Status = StatusTimeIndexDisabled
		return dst, info
	case s.gatedAt(ts):
		info.Status = StatusNotReady
		return dst, info
	case n <= 0:
		info.Status = StatusNotFound
		return dst, info
	}
	keys, more := s.time.Descend(s.keyScratch[:0], timeKey{eventTime: ts, seq: seqMax}, min(n, s.cfg.maxReadBatch))
	slices.Reverse(keys)
	info.N, info.Truncated, info.Evicted = len(keys), more, s.expiredLE(ts)
	if len(keys) == 0 {
		info.Status = missStatus(info.Evicted)
		return dst, info
	}
	info.Status = StatusOK
	info.LastRef = Ref{Gen: s.gen, Seq: keys[len(keys)-1].seq}
	return s.copyEntries(dst, keys), info
}

// NextN appends to dst the n records with the smallest (EventTime, Seq) such
// that EventTime > ts, in ascending order. n <= 0 returns StatusNotFound; n is
// capped by MaxReadBatch. NextN returns no cursor.
//
// With watermark gating, only records at or below the watermark are returned
// (P-13). If none qualify, the status is StatusNotReady unless nothing is
// retained beyond ts and the envelope reports StatusExpired, or the watermark
// is MaxTimestamp.
func (s *Series[T]) NextN(dst []Entry[T], ts Timestamp, n int) ([]Entry[T], ReadInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	info := ReadInfo{Oldest: s.b.oldestRef(s.gen)}
	switch {
	case s.time == nil:
		info.Status = StatusTimeIndexDisabled
		return dst, info
	case n <= 0:
		info.Status = StatusNotFound
		return dst, info
	}
	bound := MaxTimestamp // never stored, so it includes everything
	if !s.complete() {
		bound = s.watermark + 1 // watermark < MaxTimestamp here
	}
	keys, more := s.time.Ascend(s.keyScratch[:0], timeKey{eventTime: ts, seq: seqMax}, bound, min(n, s.cfg.maxReadBatch))
	info.N, info.Truncated, info.Evicted = len(keys), more, s.expiredGT(ts)
	if len(keys) > 0 {
		info.Status = StatusOK
		info.LastRef = Ref{Gen: s.gen, Seq: keys[len(keys)-1].seq}
		return s.copyEntries(dst, keys), info
	}
	_, pending := s.time.After(ts) // a retained match beyond the watermark
	switch {
	case !s.complete() && (pending || !info.Evicted):
		info.Status = StatusNotReady
	default:
		info.Status = missStatus(info.Evicted)
	}
	return dst, info
}

// Entries returns a paged scan of the records with from <= EventTime < to in
// (EventTime, Seq) order. The status is computed up front and covers
// StatusTimeIndexDisabled, StatusNotReady and an empty interval; when it is not
// OK the sequence yields nothing.
//
// Each page (up to MaxReadBatch entries) is copied under the lock, the lock is
// released, and then the page is yielded, so the loop body may call any
// Series method. The scan as a whole is not atomic: records evicted between
// pages are skipped, late records inserted behind the scan position are not
// seen, and the scan ends early if a later page is not OK (for example after
// Clear).
func (s *Series[T]) Entries(from, to Timestamp) (iter.Seq[Entry[T]], Status) {
	s.mu.Lock()
	_, st := s.rangePlan(from, to)
	s.mu.Unlock()
	if st != StatusOK {
		return func(func(Entry[T]) bool) {}, st
	}
	return func(yield func(Entry[T]) bool) {
		var page []Entry[T]
		page, info := s.Range(page, from, to, 0)
		for {
			if info.Status != StatusOK {
				return
			}
			for _, e := range page {
				if !yield(e) {
					return
				}
			}
			if info.Next.IsZero() {
				return
			}
			clear(page) // release references held by the previous page
			page, info = s.Continue(page[:0], info.Next, 0)
		}
	}, StatusOK
}
