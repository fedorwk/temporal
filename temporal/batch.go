package temporal

import "fmt"

// AppendBatch appends items atomically: either every item is applied (subject
// to per-item Drop outcomes) or none is, and subscribers are woken once. It
// appends one AppendResult per item to dst and returns the extended slice.
//
// A batch behaves exactly like the same Append calls in order (P-02): late
// detection, SourceSeq checks and unique-key conflicts see the earlier items of
// the batch, and a key held by a row that an earlier item (or the item itself)
// evicts is free. Dropped items consume no Seq.
//
// An empty batch is a no-op. A batch larger than Capacity fails with
// ErrBatchTooLarge. On any error dst is returned unchanged and no data state
// changes; *ConflictError carries the failing item's BatchPos, and other
// errors name the position while still matching their sentinel via errors.Is.
func (s *Series[T]) AppendBatch(dst []AppendResult, items []Input[T]) ([]AppendResult, error) {
	s.mu.Lock()
	switch {
	case s.closed:
		s.mu.Unlock()
		return dst, ErrClosed
	case len(items) == 0:
		s.mu.Unlock()
		return dst, nil // P-09
	case len(items) > s.cfg.capacity:
		s.n.rejected++
		s.mu.Unlock()
		return dst, ErrBatchTooLarge
	}
	if err := s.prepareBatch(items); err != nil {
		s.n.rejected++
		s.mu.Unlock()
		return dst, err
	}
	dst, appended := s.commitBatch(dst, items)
	s.mu.Unlock()
	if appended {
		s.notify()
	}
	return dst, nil
}

// prepareBatch runs the prepare phase for every item against a simulated
// state, filling s.plans (and s.cloned for CloneOnWrite). It mutates no data
// state; only diagnostic counters may move (P-04).
func (s *Series[T]) prepareBatch(items []Input[T]) error {
	n := len(items)
	if cap(s.plans) < n {
		s.plans = make([]itemPlan, n, max(n, 2*cap(s.plans)))
	}
	s.plans = s.plans[:n]
	if s.cfg.cloneOnWrite {
		if cap(s.cloned) < n {
			s.cloned = make([]T, n, max(n, 2*cap(s.cloned)))
		}
		s.cloned = s.cloned[:n]
	}
	if len(s.idx) > 0 {
		for _, ix := range s.idx {
			ix.beginBatch()
		}
		defer func() {
			for _, ix := range s.idx {
				ix.endBatch()
			}
		}()
	}

	// Simulated state: what s would look like after the items so far.
	next := s.b.nextSeq()
	oldest, length := s.b.oldest, s.b.len()
	maxObserved, hasObserved := s.maxObserved, s.hasObserved
	trk := s.srcSeq

	for i := range items {
		m := &items[i].Meta
		p, err := s.prepareMeta(m, maxObserved, hasObserved, &trk, next)
		if err != nil {
			s.clearCloned()
			return batchErr(i, err)
		}
		v := &items[i].Value
		if s.cfg.cloneOnWrite {
			c, err := s.callClone(v)
			if err != nil {
				s.clearCloned()
				return batchErr(i, err)
			}
			s.cloned[i] = c
			v = &s.cloned[i]
		}
		if len(s.idx) > 0 {
			evictBelow := oldest
			if length == s.cfg.capacity {
				evictBelow++
			}
			if err := s.prepareIndexes(v, m, &p, next, evictBelow, i); err != nil {
				s.clearCloned()
				return batchErr(i, err)
			}
			if !p.drop {
				for _, ix := range s.idx {
					ix.stage(i, next)
				}
			}
		}
		s.plans[i] = p
		if p.drop {
			continue // no Seq, no tracker update (P-03)
		}

		switch {
		case length == 0:
			oldest, length = next, 1
		case length == s.cfg.capacity:
			oldest++
		default:
			length++
		}
		next++
		if !hasObserved || m.EventTime > maxObserved {
			maxObserved, hasObserved = m.EventTime, true
		}
		if s.cfg.sourceSeqPolicy != SourceSeqIgnore && m.SourceSeq != 0 {
			trk.observe(m.SourceEpoch, m.SourceSeq)
		}
	}
	return nil
}

// commitBatch applies the prepared plans in order, then hands all time-index
// changes to the index in one call. It reports whether any item was stored.
func (s *Series[T]) commitBatch(dst []AppendResult, items []Input[T]) ([]AppendResult, bool) {
	appended := false
	for i := range items {
		p := s.plans[i]
		if p.drop {
			dst = append(dst, s.commitDrop(&p, i))
			continue
		}
		v := &items[i].Value
		if s.cfg.cloneOnWrite {
			v = &s.cloned[i]
		}
		dst = append(dst, s.commit(v, &items[i].Meta, p, i))
		appended = true
	}
	s.applyTime()
	s.clearCloned()
	return dst, appended
}

// clearCloned drops references held by the CloneOnWrite scratch.
func (s *Series[T]) clearCloned() {
	if s.cfg.cloneOnWrite {
		clear(s.cloned)
	}
}

// batchErr attaches the batch position to err. *ConflictError already carries
// it in BatchPos.
func batchErr(pos int, err error) error {
	if _, ok := err.(*ConflictError); ok {
		return err
	}
	return fmt.Errorf("temporal: batch item %d: %w", pos, err)
}
