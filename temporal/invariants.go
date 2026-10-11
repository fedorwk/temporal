package temporal

import "fmt"

// CheckInvariants rebuilds the derivable state and compares it with what the
// Series maintains. It is O(Len) and intended for tests and debugging. It
// returns nil when every invariant holds.
func (s *Series[T]) CheckInvariants() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkInvariantsLocked()
}

func (s *Series[T]) checkInvariantsLocked() error {
	b := s.b
	if s.gen == 0 {
		return fmt.Errorf("gen is 0")
	}
	if b.newest == seqMax {
		return fmt.Errorf("newest is the reserved seq %d", seqMax)
	}
	if !b.empty() {
		if b.oldest > b.newest {
			return fmt.Errorf("oldest %d > newest %d", b.oldest, b.newest)
		}
		if b.len() > s.cfg.capacity {
			return fmt.Errorf("len %d > capacity %d", b.len(), s.cfg.capacity)
		}
	}

	if s.env.has {
		if s.env.min > s.env.max {
			return fmt.Errorf("envelope min %d > max %d", s.env.min, s.env.max)
		}
		if !s.hasObserved || s.env.max > s.maxObserved {
			return fmt.Errorf("envelope max %d not covered by max observed %d (has %v)",
				s.env.max, s.maxObserved, s.hasObserved)
		}
	} else if s.env.min != 0 || s.env.max != 0 {
		return fmt.Errorf("empty envelope has bounds [%d, %d]", s.env.min, s.env.max)
	}
	if !b.empty() && !s.hasObserved {
		return fmt.Errorf("%d records retained but nothing observed", b.len())
	}

	used := make(map[int]Seq, b.len())
	for seq := b.oldest; !b.empty() && seq <= b.newest; seq++ {
		slot := s.ring.slot(seq)
		if prev, dup := used[slot]; dup {
			return fmt.Errorf("seqs %d and %d share slot %d", prev, seq, slot)
		}
		used[slot] = seq
		if debugAssert && s.ring.seqs[slot] != seq {
			return fmt.Errorf("slot %d holds seq %d, want %d", slot, s.ring.seqs[slot], seq)
		}
		m := &s.ring.metas[slot]
		if m.eventTime == MinTimestamp || m.eventTime == MaxTimestamp {
			return fmt.Errorf("seq %d stores sentinel EventTime %d", seq, m.eventTime)
		}
		if m.eventTime > s.maxObserved {
			return fmt.Errorf("seq %d EventTime %d > max observed %d", seq, m.eventTime, s.maxObserved)
		}
		if m.flags&SeriesFlagsMask&^(FlagLate|FlagSourceSeqNotIncreasing) != 0 {
			return fmt.Errorf("seq %d has reserved flag bits %#x", seq, m.flags)
		}
	}
	if err := s.checkTimeIndexLocked(); err != nil {
		return err
	}
	for _, ix := range s.idx {
		if err := ix.checkLocked(); err != nil {
			return err
		}
	}
	return nil
}

// checkTimeIndexLocked verifies that the time index holds exactly one key per
// retained record, with matching EventTime, in strictly increasing order.
func (s *Series[T]) checkTimeIndexLocked() error {
	if s.time == nil {
		return nil
	}
	if got, want := s.time.Len(), s.b.len(); got != want {
		return fmt.Errorf("time index len %d, series len %d", got, want)
	}
	const chunk = 64
	keys := make([]timeKey, 0, chunk)
	start := timeKey{eventTime: MinTimestamp, seq: 0}
	var prev timeKey
	seen := 0
	for {
		var more bool
		keys, more = s.time.Ascend(keys[:0], start, MaxTimestamp, chunk)
		for _, k := range keys {
			if seen > 0 && !keyLess(prev, k) {
				return fmt.Errorf("time index keys out of order: %v then %v", prev, k)
			}
			if !s.b.retained(k.seq) {
				return fmt.Errorf("time index key %v is not retained (bounds %+v)", k, s.b)
			}
			if et := s.ring.eventTime(k.seq); et != k.eventTime {
				return fmt.Errorf("time index key %v, ring EventTime %d", k, et)
			}
			prev = k
			seen++
		}
		if !more {
			break
		}
		start = timeKey{eventTime: prev.eventTime, seq: prev.seq + 1}
	}
	if seen != s.b.len() {
		return fmt.Errorf("time index ascend saw %d keys, series len %d", seen, s.b.len())
	}
	if k, ok := s.time.Min(); ok != (seen > 0) || (ok && seen > 0 && k.eventTime > prev.eventTime) {
		return fmt.Errorf("time index Min %v (ok %v) inconsistent with scan", k, ok)
	}
	if k, ok := s.time.Max(); ok && k != prev {
		return fmt.Errorf("time index Max %v, last scanned %v", k, prev)
	}
	return nil
}
