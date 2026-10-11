package temporal

// noTimeIndex is the zero-size "no ordering capability" implementation. The
// Series represents TimeIndexNone with a nil timeIndex and never routes calls
// through this type; it exists for the common test harness and capability
// reporting. Mutators do nothing and every primitive reports absence.
type noTimeIndex struct{}

var _ timeIndex = noTimeIndex{}

func (noTimeIndex) Set(timeKey)                         {}
func (noTimeIndex) Delete(timeKey)                      {}
func (noTimeIndex) Apply(_, _ []timeKey)                {}
func (noTimeIndex) Clear()                              {}
func (noTimeIndex) AsOf(Timestamp) (timeKey, bool)      { return timeKey{}, false }
func (noTimeIndex) Before(Timestamp) (timeKey, bool)    { return timeKey{}, false }
func (noTimeIndex) After(Timestamp) (timeKey, bool)     { return timeKey{}, false }
func (noTimeIndex) AtOrAfter(Timestamp) (timeKey, bool) { return timeKey{}, false }
func (noTimeIndex) Min() (timeKey, bool)                { return timeKey{}, false }
func (noTimeIndex) Max() (timeKey, bool)                { return timeKey{}, false }
func (noTimeIndex) Len() int                            { return 0 }

func (noTimeIndex) Ascend(dst []timeKey, _ timeKey, _ Timestamp, _ int) ([]timeKey, bool) {
	return dst, false
}

func (noTimeIndex) Descend(dst []timeKey, _ timeKey, _ int) ([]timeKey, bool) {
	return dst, false
}
