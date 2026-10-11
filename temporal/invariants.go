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
	return nil
}
