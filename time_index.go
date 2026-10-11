package temporal

// timeIndex is the internal event-time ordering capability. It orders keys by
// (EventTime, Seq) and holds exactly one key per retained record.
//
// All methods are called with Series.mu held. Implementations hold no lock,
// call no Series method or user code, and retain no reference to argument
// slices.
type timeIndex interface {
	// Mutation.
	Set(k timeKey)
	Delete(k timeKey)
	Apply(del, ins []timeKey) // one call per committed transaction; del and ins are disjoint
	Clear()

	// Point primitives (keys only; the Series resolves Seq to a slot).
	AsOf(ts Timestamp) (timeKey, bool)      // greatest key with EventTime <= ts
	Before(ts Timestamp) (timeKey, bool)    // greatest key with EventTime <  ts
	After(ts Timestamp) (timeKey, bool)     // smallest key with EventTime >  ts
	AtOrAfter(ts Timestamp) (timeKey, bool) // smallest key with EventTime >= ts
	Min() (timeKey, bool)
	Max() (timeKey, bool)

	// Ordered scans. Each appends up to limit keys to dst and reports whether
	// more matching keys remain.
	Ascend(dst []timeKey, start timeKey, to Timestamp, limit int) ([]timeKey, bool) // keys >= start with EventTime < to
	Descend(dst []timeKey, start timeKey, limit int) ([]timeKey, bool)              // keys <= start, descending

	Len() int
}

// newTimeIndex returns the implementation for mode, or nil for TimeIndexNone.
// r is the Series' ring, which TimeIndexOrdered searches directly.
func newTimeIndex(mode TimeIndexMode, r *metaRing) timeIndex {
	switch mode {
	case TimeIndexBTree:
		return newBTreeTimeIndex()
	case TimeIndexOrdered:
		return newOrderedTimeIndex(r)
	default:
		return nil
	}
}
