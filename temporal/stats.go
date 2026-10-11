package temporal

// IndexStats describes one key index.
type IndexStats struct {
	Name      string
	Unique    bool
	Keys      int
	Conflicts uint64 // rejected
	Dropped   uint64
	Replaced  uint64
	KeptFirst uint64
}

// Stats is a diagnostic snapshot of a Series. Fields whose meaning depends on
// data that does not exist are zero: MinEventTime and MaxEventTime when the
// Series is empty or has no time index, MinEvicted and MaxEvicted when
// HasEvicted is false. MaxObservedEventTime is MinTimestamp until a record is
// accepted in the current generation.
type Stats struct {
	Name                   string
	Gen                    Gen
	TimeIndex              TimeIndexMode
	Capacity, Len          int
	Oldest, Newest         Ref
	Appended, Evicted      uint64 // lifetime
	Dropped, Rejected      uint64 // lifetime
	Late, SourceSeqNotIncr uint64 // flagged records, lifetime
	GapFlags               uint64 // records carrying FlagGap, lifetime
	Clears                 uint64
	MinEventTime           Timestamp // retained; time index only
	MaxEventTime           Timestamp // retained; time index only
	MaxObservedEventTime   Timestamp // this generation; every mode
	HasEvicted             bool
	MinEvicted, MaxEvicted Timestamp // this generation
	Watermark              Timestamp
	Subscribers            int
	Indexes                []IndexStats
}

// Stats returns a diagnostic snapshot. It is not a hot-path call and may
// allocate.
func (s *Series[T]) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()

	st := Stats{
		Name:                 s.cfg.name,
		Gen:                  s.gen,
		TimeIndex:            s.cfg.timeIndex,
		Capacity:             s.cfg.capacity,
		Len:                  s.b.len(),
		Oldest:               s.b.oldestRef(s.gen),
		Newest:               s.b.newestRef(s.gen),
		Appended:             s.n.appended,
		Evicted:              s.n.evicted,
		Dropped:              s.n.dropped,
		Rejected:             s.n.rejected,
		Late:                 s.n.late,
		SourceSeqNotIncr:     s.n.sourceSeqNotIncr,
		GapFlags:             s.n.gapFlags,
		Clears:               s.n.clears,
		MaxObservedEventTime: MinTimestamp,
		HasEvicted:           s.env.has,
		MinEvicted:           s.env.min,
		MaxEvicted:           s.env.max,
		Watermark:            s.watermark,
	}
	if s.hasObserved {
		st.MaxObservedEventTime = s.maxObserved
	}
	return st
}
