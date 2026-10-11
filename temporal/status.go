package temporal

import (
	"errors"
	"fmt"
	"strconv"
)

// Status explains the outcome of a read.
type Status uint8

const (
	// StatusOK means at least one matching record was returned.
	StatusOK Status = iota
	// StatusNotFound means no matching record is retained and no record
	// evicted in the current generation could have matched.
	StatusNotFound
	// StatusExpired means no matching record is retained, but a record
	// evicted in the current generation matched or may have matched.
	StatusExpired
	// StatusReset means the supplied Ref or TimeCursor belongs to an earlier
	// generation.
	StatusReset
	// StatusNotReady means the query bound exceeds the watermark and
	// Config.GateReadsByWatermark is set.
	StatusNotReady
	// StatusTimeIndexDisabled means an event-time query was issued on a
	// Series created with TimeIndexNone.
	StatusTimeIndexDisabled
	// StatusInvalid means the request is malformed, such as a zero or
	// foreign TimeCursor.
	StatusInvalid
)

var statusNames = [...]string{
	StatusOK:                "OK",
	StatusNotFound:          "NotFound",
	StatusExpired:           "Expired",
	StatusReset:             "Reset",
	StatusNotReady:          "NotReady",
	StatusTimeIndexDisabled: "TimeIndexDisabled",
	StatusInvalid:           "Invalid",
}

// String returns the status name.
func (s Status) String() string {
	if int(s) < len(statusNames) {
		return statusNames[s]
	}
	return "Status(" + strconv.Itoa(int(s)) + ")"
}

// Err maps StatusOK to nil and every other status to its sentinel error, so
// callers may use errors.Is.
func (s Status) Err() error {
	switch s {
	case StatusOK:
		return nil
	case StatusNotFound:
		return ErrNotFound
	case StatusExpired:
		return ErrExpired
	case StatusReset:
		return ErrReset
	case StatusNotReady:
		return ErrNotReady
	case StatusTimeIndexDisabled:
		return ErrTimeIndexDisabled
	default:
		return ErrInvalidInput
	}
}

// Sentinel errors.
var (
	ErrNotFound               = errors.New("temporal: not found")
	ErrExpired                = errors.New("temporal: expired")
	ErrReset                  = errors.New("temporal: reset by Clear")
	ErrNotReady               = errors.New("temporal: not ready (beyond watermark)")
	ErrTimeIndexDisabled      = errors.New("temporal: time index disabled")
	ErrLagged                 = errors.New("temporal: subscriber lagged")
	ErrClosed                 = errors.New("temporal: series closed")
	ErrInvalidConfig          = errors.New("temporal: invalid config")
	ErrInvalidInput           = errors.New("temporal: invalid input")
	ErrLate                   = errors.New("temporal: late event rejected")
	ErrSourceSeqNotIncreasing = errors.New("temporal: source sequence not increasing")
	ErrUniqueConflict         = errors.New("temporal: unique key conflict")
	ErrBatchTooLarge          = errors.New("temporal: batch larger than capacity")
	ErrCallbackPanic          = errors.New("temporal: callback panicked")
	ErrSeqExhausted           = errors.New("temporal: sequence exhausted")
	ErrGenExhausted           = errors.New("temporal: generation exhausted")
	ErrIndexBound             = errors.New("temporal: index already bound to a series")
)

// LagError reports that a subscriber fell behind retention.
type LagError struct {
	Cursor Cursor // the subscriber's position when lag was detected
	Oldest Ref    // oldest retained Ref at that time
	Missed uint64 // number of records evicted before they were read
}

func (e *LagError) Error() string {
	return fmt.Sprintf(
		"temporal: subscriber lagged: cursor gen %d next %d, oldest gen %d seq %d, missed %d",
		e.Cursor.Gen, e.Cursor.Next, e.Oldest.Gen, e.Oldest.Seq, e.Missed,
	)
}

// Is reports whether target is ErrLagged.
func (e *LagError) Is(target error) bool { return target == ErrLagged }

// ConflictError reports a unique-key conflict under ConflictReject.
type ConflictError struct {
	Index    string
	Key      string // fmt %v of the key, for diagnostics only
	Existing Ref
	BatchPos int
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf(
		"temporal: unique key conflict on index %q key %s: existing gen %d seq %d (batch position %d)",
		e.Index, e.Key, e.Existing.Gen, e.Existing.Seq, e.BatchPos,
	)
}

// Is reports whether target is ErrUniqueConflict.
func (e *ConflictError) Is(target error) bool { return target == ErrUniqueConflict }

// CallbackPanicError reports a recovered panic from a user callback.
type CallbackPanicError struct {
	Callback string // "extractor:<index>" or "clone"
	Value    any
	Stack    []byte
}

func (e *CallbackPanicError) Error() string {
	return fmt.Sprintf("temporal: panic in %s callback: %v", e.Callback, e.Value)
}

// Is reports whether target is ErrCallbackPanic.
func (e *CallbackPanicError) Is(target error) bool { return target == ErrCallbackPanic }

// ReadInfo describes the result of a batch read.
type ReadInfo struct {
	Status    Status     // OK if N > 0; otherwise per the status rules applied to the whole query
	N         int        // number of entries appended to dst by this call
	Truncated bool       // limit reached while more matching retained records exist
	Evicted   bool       // records evicted in this generation could have matched
	Next      TimeCursor // non-zero iff Truncated and the read supports continuation
	LastRef   Ref        // Ref of the last returned entry
	Oldest    Ref        // oldest retained Ref at the time of the read
}

// TimeCursor is an opaque continuation token for time-ordered reads. Callers
// must not inspect or construct cursors.
type TimeCursor struct {
	series uint64    // identity of the producing Series; 0 is never assigned
	gen    Gen       // generation at the time of the read
	state  [3]uint64 // last returned (EventTime, Seq) and the exclusive upper bound
}

// IsZero reports whether c is the zero cursor.
func (c TimeCursor) IsZero() bool { return c.series == 0 }

// makeCursor encodes a continuation strictly after last, below bound.
func makeCursor(series uint64, gen Gen, last timeKey, bound Timestamp) TimeCursor {
	return TimeCursor{
		series: series,
		gen:    gen,
		state:  [3]uint64{uint64(last.eventTime), uint64(last.seq), uint64(bound)},
	}
}

// decode returns the last returned key and the exclusive upper bound.
func (c TimeCursor) decode() (last timeKey, bound Timestamp) {
	last = timeKey{eventTime: Timestamp(c.state[0]), seq: Seq(c.state[1])}
	return last, Timestamp(c.state[2])
}

// Outcome says whether an appended item was stored.
type Outcome uint8

const (
	// OutcomeAppended means the record was stored and indexed.
	OutcomeAppended Outcome = iota
	// OutcomeDropped means the record was not stored because a ConflictDrop
	// unique index already maps its key.
	OutcomeDropped
)

// AppendResult describes one appended item.
type AppendResult struct {
	Outcome  Outcome
	Ref      Ref    // assigned identity; zero when Dropped
	Flags    Flags  // series-set flags applied (FlagLate, FlagSourceSeqNotIncreasing)
	Evicted  Ref    // record evicted by this append; zero if none
	Existing Ref    // Dropped: holder of the key; Replace: record that lost the mapping; KeepFirst: record that kept it
	Index    string // name of the index that produced Existing, if any
}
