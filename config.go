package temporal

import (
	"fmt"
	"time"
)

// TimeIndexMode selects the event-time ordering capability of a Series. It is
// fixed at New.
type TimeIndexMode uint8

const (
	// TimeIndexNone disables event-time queries. They return
	// StatusTimeIndexDisabled and Append pays no ordering cost.
	TimeIndexNone TimeIndexMode = iota
	// TimeIndexBTree orders records by (EventTime, Seq) in a B-tree.
	TimeIndexBTree
)

func (m TimeIndexMode) valid() bool { return m <= TimeIndexBTree }

// LatePolicy decides what happens to late records. Lateness is always flagged.
type LatePolicy uint8

const (
	// LateAccept stores late records with FlagLate.
	LateAccept LatePolicy = iota
	// LateReject fails the append with ErrLate.
	LateReject
)

func (p LatePolicy) valid() bool { return p <= LateReject }

// SourceSeqPolicy decides how SourceSeq is checked within a SourceEpoch.
type SourceSeqPolicy uint8

const (
	// SourceSeqIgnore does not interpret SourceSeq.
	SourceSeqIgnore SourceSeqPolicy = iota
	// SourceSeqFlag stores non-increasing records with FlagSourceSeqNotIncreasing.
	SourceSeqFlag
	// SourceSeqReject fails the append with ErrSourceSeqNotIncreasing.
	SourceSeqReject
)

func (p SourceSeqPolicy) valid() bool { return p <= SourceSeqReject }

// CloneMode selects when Config.Clone runs. It is ignored when Clone is nil.
type CloneMode uint8

const (
	// CloneOnRead clones every value copied out of the Series.
	CloneOnRead CloneMode = iota
	// CloneOnWrite clones each value once, before it is stored.
	CloneOnWrite
	// CloneOnReadWrite clones on both paths.
	CloneOnReadWrite
)

func (m CloneMode) valid() bool { return m <= CloneOnReadWrite }

// ConflictPolicy decides what a unique index does with a key that is already
// mapped to a retained record.
type ConflictPolicy uint8

const (
	// ConflictReject fails the append with ErrUniqueConflict.
	ConflictReject ConflictPolicy = iota
	// ConflictDrop does not store the item and reports OutcomeDropped.
	ConflictDrop
	// ConflictReplace stores the item and points the key at it.
	ConflictReplace
	// ConflictKeepFirst stores the item without entering it into this index.
	ConflictKeepFirst
)

func (p ConflictPolicy) valid() bool { return p <= ConflictKeepFirst }

// Order is a Seq order for key-index results.
type Order uint8

const (
	Ascending Order = iota
	Descending
)

func (o Order) valid() bool { return o <= Descending }

// LagPolicy decides what a subscription does when it falls behind retention.
type LagPolicy uint8

const (
	// LagFail returns a *LagError and leaves the cursor unchanged.
	LagFail LagPolicy = iota
	// LagSkip jumps to the oldest retained record and counts the skipped ones.
	LagSkip
)

func (p LagPolicy) valid() bool { return p <= LagSkip }

// defaultMaxReadBatch is used when Config.MaxReadBatch is 0.
const defaultMaxReadBatch = 4096

// Config configures a Series.
type Config[T any] struct {
	Name                 string
	Capacity             int           // required, >= 1
	TimeIndex            TimeIndexMode // zero value = TimeIndexNone; immutable after New
	MaxReadBatch         int           // 0 = default 4096
	Clone                func(T) T     // optional
	CloneMode            CloneMode
	LatePolicy           LatePolicy
	LateTolerance        time.Duration // 0 = any EventTime below the max is late
	SourceSeqPolicy      SourceSeqPolicy
	GateReadsByWatermark bool
	Indexes              []Index[T]
}

// config is the validated, normalized form of Config.
type config[T any] struct {
	name            string
	capacity        int
	timeIndex       TimeIndexMode
	maxReadBatch    int
	clone           func(T) T
	cloneOnRead     bool
	cloneOnWrite    bool
	latePolicy      LatePolicy
	lateTolerance   Timestamp // nanoseconds, >= 0
	sourceSeqPolicy SourceSeqPolicy
	gate            bool
	indexes         []Index[T]
}

// validate checks c and returns its normalized form. Every failure wraps
// ErrInvalidConfig. Index binding (ErrIndexBound) is checked by New.
func (c *Config[T]) validate() (config[T], error) {
	switch {
	case c.Capacity < 1:
		return config[T]{}, fmt.Errorf("%w: Capacity must be >= 1, got %d", ErrInvalidConfig, c.Capacity)
	case c.MaxReadBatch < 0:
		return config[T]{}, fmt.Errorf("%w: MaxReadBatch must be >= 0, got %d", ErrInvalidConfig, c.MaxReadBatch)
	case !c.TimeIndex.valid():
		return config[T]{}, fmt.Errorf("%w: unknown TimeIndexMode %d", ErrInvalidConfig, c.TimeIndex)
	case !c.CloneMode.valid():
		return config[T]{}, fmt.Errorf("%w: unknown CloneMode %d", ErrInvalidConfig, c.CloneMode)
	case !c.LatePolicy.valid():
		return config[T]{}, fmt.Errorf("%w: unknown LatePolicy %d", ErrInvalidConfig, c.LatePolicy)
	case !c.SourceSeqPolicy.valid():
		return config[T]{}, fmt.Errorf("%w: unknown SourceSeqPolicy %d", ErrInvalidConfig, c.SourceSeqPolicy)
	case c.LateTolerance < 0:
		return config[T]{}, fmt.Errorf("%w: LateTolerance must be >= 0, got %v", ErrInvalidConfig, c.LateTolerance)
	}

	seen := make(map[string]struct{}, len(c.Indexes))
	for i, ix := range c.Indexes {
		if ix == nil {
			return config[T]{}, fmt.Errorf("%w: Indexes[%d] is nil", ErrInvalidConfig, i)
		}
		name := ix.indexName()
		if _, dup := seen[name]; dup {
			return config[T]{}, fmt.Errorf("%w: duplicate index name %q", ErrInvalidConfig, name)
		}
		if err := ix.validate(); err != nil {
			return config[T]{}, err
		}
		seen[name] = struct{}{}
	}

	n := config[T]{
		name:            c.Name,
		capacity:        c.Capacity,
		timeIndex:       c.TimeIndex,
		maxReadBatch:    c.MaxReadBatch,
		latePolicy:      c.LatePolicy,
		lateTolerance:   Timestamp(c.LateTolerance),
		sourceSeqPolicy: c.SourceSeqPolicy,
		gate:            c.GateReadsByWatermark,
		indexes:         c.Indexes,
	}
	if n.maxReadBatch == 0 {
		n.maxReadBatch = defaultMaxReadBatch
	}
	// CloneMode is ignored when Clone is nil (P-17).
	if c.Clone != nil {
		n.clone = c.Clone
		n.cloneOnRead = c.CloneMode == CloneOnRead || c.CloneMode == CloneOnReadWrite
		n.cloneOnWrite = c.CloneMode == CloneOnWrite || c.CloneMode == CloneOnReadWrite
	}
	return n, nil
}
