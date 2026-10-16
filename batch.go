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
//
// A panic in Config.Clone or a key extractor propagates to the caller with no
// state change, as for Append.
func (s *Series[T]) AppendBatch(dst []AppendResult, items []Input[T]) ([]AppendResult, error) {
	var appended bool
	var err error
	if s.cfg.cloneOnWrite || len(s.idx) > 0 {
		dst, appended, err = s.appendBatchWithCallbacks(dst, items)
	} else {
		// No user code runs, so nothing can panic under the lock and there is
		// no clone scratch or index overlay to clean up.
		s.mu.Lock()
		dst, appended, err = s.appendBatchLocked(dst, items)
		s.mu.Unlock()
	}
	if appended {
		s.sig.records()
	}
	return dst, err
}

// appendBatchWithCallbacks runs the transaction when user code runs (Clone or
// extractors): the deferred calls release the lock and the CloneOnWrite
// scratch even during a panic (S12-01); prepareBatch ends index batches by
// defer.
func (s *Series[T]) appendBatchWithCallbacks(dst []AppendResult, items []Input[T]) ([]AppendResult, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer s.clearCloned()
	return s.appendBatchLocked(dst, items)
}

// appendBatchLocked runs one AppendBatch transaction; s.mu is held.
func (s *Series[T]) appendBatchLocked(dst []AppendResult, items []Input[T]) ([]AppendResult, bool, error) {
	switch {
	case s.closed:
		return dst, false, s.closeErr
	case len(items) == 0:
		return dst, false, nil // P-09
	case len(items) > s.cfg.capacity:
		s.n.rejected++
		return dst, false, ErrBatchTooLarge
	}
	if err := s.prepareBatch(items); err != nil {
		s.n.rejected++
		return dst, false, err
	}
	dst, appended := s.commitBatch(dst, items)
	return dst, appended, nil
}

// prepareBatch runs the prepare phase for every item against a copy of the
// admission state that it advances past each admitted item, exactly as commit
// will advance the real one, and fills s.plans (and s.cloned for
// CloneOnWrite). It mutates no data state; only diagnostic counters may move
// (P-04).
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

	sim := s.adm
	for i := range items {
		m := &items[i].Meta
		flags, err := sim.check(m)
		if err != nil {
			return batchErr(i, err)
		}
		p := itemPlan{flags: flags, existingIdx: -1}
		v := &items[i].Value
		if s.cfg.cloneOnWrite {
			s.cloned[i] = s.cfg.clone(*v)
			v = &s.cloned[i]
		}
		if len(s.idx) > 0 {
			next := sim.nextSeq()
			if err := s.prepareIndexes(v, m, &p, next, sim.evictBelow(), i); err != nil {
				return batchErr(i, err)
			}
			if !p.drop {
				for _, ix := range s.idx {
					ix.stage(i, next)
				}
			}
		}
		if !p.drop { // a dropped item consumes no Seq and updates no tracker (P-03)
			sim.push()
			sim.observe(m)
		}
		s.plans[i] = p
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

// AppendAll appends items in order through AppendBatch calls of at most
// min(Capacity, MaxReadBatch) items each, so it accepts any number of items and
// bounds how long each call holds the lock. Each chunk is atomic and wakes
// subscribers once; the call as a whole is not: when a chunk fails, the
// earlier chunks stay applied and the failing chunk and everything after it
// are not. It appends one AppendResult per applied item to dst, so
// len(result)-len(dst) is the number of items applied. A *ConflictError
// carries the item's position in items; other errors name the position of the
// failing chunk's first item and match their sentinel via errors.Is.
//
// Items beyond Capacity evict records appended earlier in the same call, as
// the equivalent Appends would.
func (s *Series[T]) AppendAll(dst []AppendResult, items []Input[T]) ([]AppendResult, error) {
	chunk := min(s.cfg.capacity, s.cfg.maxReadBatch)
	for off := 0; off < len(items); off += chunk {
		end := min(off+chunk, len(items))
		var err error
		if dst, err = s.AppendBatch(dst, items[off:end]); err != nil {
			return dst, allErr(off, err)
		}
	}
	return dst, nil
}

// allErr rebases a chunk-relative AppendBatch error onto the AppendAll input.
func allErr(off int, err error) error {
	if ce, ok := err.(*ConflictError); ok {
		abs := *ce
		abs.BatchPos += off
		return &abs
	}
	if off == 0 {
		return err
	}
	return fmt.Errorf("temporal: AppendAll chunk at item %d: %w", off, err)
}
