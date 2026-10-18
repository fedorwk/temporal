package temporal

// admission decides whether the next record is admitted (P-03 steps 1-4) and
// how admitting it advances the Series: the retained Seq interval, the late
// tracker and the SourceSeq tracker. It is a plain value. A record is admitted
// with check, then push and observe once nothing can drop it. The commit phase
// does this on the Series' own state; AppendBatch's prepare phase does it on a
// copy, item by item. Both run the same rules, which makes a batch equivalent
// to the same Appends in order (P-02).
type admission struct {
	rules       admissionRules // fixed at New; copied along, never changed
	b           bounds
	maxObserved Timestamp // largest accepted EventTime this generation, or MinTimestamp (S2-02)
	srcSeq      sourceSeqTracker
}

// admissionRules is the configuration admission applies.
type admissionRules struct {
	capacity        int
	lateTolerance   Timestamp
	latePolicy      LatePolicy
	sourceSeqPolicy SourceSeqPolicy
}

// sourceSeqTracker remembers the highest SourceSeq seen in the current
// SourceEpoch (§9.2, DECISIONS S2-01). last == 0 means nothing was seen: a
// SourceSeq of 0 is "not provided" and never tracked.
type sourceSeqTracker struct {
	epoch SourceEpoch
	last  uint64
}

// check runs P-03 steps 1-4 for m without changing a. It returns the series
// flags the record gets (FlagLate, FlagSourceSeqNotIncreasing), or the error
// that rejects it.
func (a *admission) check(m *InputMeta) (Flags, error) {
	var flags Flags
	if m.EventTime == MinTimestamp || m.EventTime == MaxTimestamp {
		return 0, errInvalidEventTime
	}
	if m.Flags&SeriesFlagsMask != 0 {
		return 0, errSeriesFlags
	}
	// With nothing observed the threshold is MinTimestamp, which no valid
	// EventTime is below.
	if m.EventTime < lateThreshold(a.maxObserved, a.rules.lateTolerance) {
		if a.rules.latePolicy == LateReject {
			return 0, ErrLate
		}
		flags |= FlagLate
	}
	if a.tracksSourceSeq(m) && a.srcSeq.epoch == m.SourceEpoch && m.SourceSeq <= a.srcSeq.last {
		if a.rules.sourceSeqPolicy == SourceSeqReject {
			return 0, ErrSourceSeqNotIncreasing
		}
		flags |= FlagSourceSeqNotIncreasing
	}
	if a.nextSeq() == seqMax {
		return 0, ErrSeqExhausted
	}
	return flags, nil
}

// nextSeq is the Seq the next admitted record receives.
func (a *admission) nextSeq() Seq { return a.b.nextSeq() }

// evictBelow is the lowest Seq still retained once the next record is
// admitted: rows below it count as evicted for the key-index conflict check
// (P-02, P-11).
func (a *admission) evictBelow() Seq {
	if a.b.len() == a.rules.capacity {
		return a.b.oldest + 1
	}
	return a.b.oldest
}

// push and observe admit a record that check accepted and no index dropped.
// They are two methods, always called together, only so that each stays small
// enough to inline on the write path.

// push assigns the next Seq and evicts the oldest record if the Series is
// full. It returns the assigned Seq and the evicted Seq, or 0 if nothing was
// evicted (Seq 0 is never assigned).
func (a *admission) push() (seq, evicted Seq) {
	evicted, _ = a.b.push(a.rules.capacity)
	return a.b.newest, evicted
}

// observe records m's EventTime and SourceSeq in the late and SourceSeq
// trackers.
func (a *admission) observe(m *InputMeta) {
	a.maxObserved = max(a.maxObserved, m.EventTime)
	if a.tracksSourceSeq(m) {
		a.srcSeq.observe(m.SourceEpoch, m.SourceSeq)
	}
}

func (a *admission) tracksSourceSeq(m *InputMeta) bool {
	return a.rules.sourceSeqPolicy != SourceSeqIgnore && m.SourceSeq != 0
}

// observed reports whether a record was accepted in this generation.
func (a *admission) observed() bool { return a.maxObserved != MinTimestamp }

// reset starts a new generation: no records, nothing observed, next Seq 1.
func (a *admission) reset() { *a = admission{rules: a.rules, maxObserved: MinTimestamp} }

// observe records an accepted SourceSeq. A new epoch resets the tracker.
func (t *sourceSeqTracker) observe(epoch SourceEpoch, seq uint64) {
	if t.epoch != epoch {
		*t = sourceSeqTracker{epoch: epoch, last: seq}
		return
	}
	t.last = max(t.last, seq)
}

// lateThreshold is maxObserved - tolerance, saturating at MinTimestamp (P-10).
func lateThreshold(maxObserved, tolerance Timestamp) Timestamp {
	if maxObserved < MinTimestamp+tolerance {
		return MinTimestamp
	}
	return maxObserved - tolerance
}
