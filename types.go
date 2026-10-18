package temporal

import (
	"math"
	"time"
)

// Seq is the local ingest sequence. It is strictly increasing within a
// generation and starts at 1 for the first record of each generation.
type Seq uint64

// SeqNone is the invalid Seq. The zero Ref therefore never names a record.
const SeqNone Seq = 0

// seqMax is reserved as a search sentinel and is never assigned to a record.
const seqMax Seq = math.MaxUint64

// Gen is the Series generation. It starts at 1 and increments on every Clear.
type Gen uint32

// Ref is the public identity of a record.
type Ref struct {
	Gen Gen
	Seq Seq
}

// IsZero reports whether r is the zero (invalid) Ref.
func (r Ref) IsZero() bool { return r.Seq == SeqNone }

// Timestamp is a point in time as Unix nanoseconds, UTC. The representable
// range covers the years 1677 to 2262.
type Timestamp int64

// MinTimestamp and MaxTimestamp are reserved sentinels. Append rejects a record
// whose EventTime equals either of them.
const (
	MinTimestamp Timestamp = math.MinInt64
	MaxTimestamp Timestamp = math.MaxInt64
)

// FromTime converts t to a Timestamp. The result is undefined if t is outside
// the range representable as int64 Unix nanoseconds.
func FromTime(t time.Time) Timestamp { return Timestamp(t.UnixNano()) }

// Time converts ts to a time.Time in UTC.
func (ts Timestamp) Time() time.Time { return time.Unix(0, int64(ts)).UTC() }

// SourceEpoch identifies a connection, session or source restart. A change of
// epoch resets SourceSeq checking.
type SourceEpoch uint32

// Flags annotate a record. Bits 0-15 are supplied by the adapter; bits 16-31
// are set by the Series and must not be supplied by callers.
type Flags uint32

const (
	// FlagGap marks that the adapter detected a source gap before this record.
	FlagGap Flags = 1 << 0
	// FlagSnapshot marks a record that comes from a snapshot or recovery.
	FlagSnapshot Flags = 1 << 1
	// FlagReplay marks a record that comes from a file or recorded replay.
	FlagReplay Flags = 1 << 2
	// FlagCorrection marks a record that corrects an earlier record.
	FlagCorrection Flags = 1 << 3
	// AdapterFlagsMask covers the adapter-owned bits. Bits 4-15 are
	// application-defined.
	AdapterFlagsMask Flags = 0x0000FFFF

	// FlagLate is set by the Series on records whose EventTime is below
	// MaxObservedEventTime minus Config.LateTolerance.
	FlagLate Flags = 1 << 16
	// FlagSourceSeqNotIncreasing is set by the Series when SourceSeq checking
	// is enabled and SourceSeq did not increase within the same SourceEpoch.
	FlagSourceSeqNotIncreasing Flags = 1 << 17
	// SeriesFlagsMask covers the Series-owned bits. Bits 18-31 are reserved.
	SeriesFlagsMask Flags = 0xFFFF0000
)

// InputMeta is the metadata supplied with a record on Append.
type InputMeta struct {
	EventTime   Timestamp // required; MinTimestamp and MaxTimestamp are rejected
	ReceiveTime Timestamp // 0 = unknown
	SourceSeq   uint64    // 0 = not provided
	SourceEpoch SourceEpoch
	Flags       Flags // adapter bits (0-15) only
}

// meta is the Meta the record gets when stored as ref with the series-set
// flags.
func (m *InputMeta) meta(flags Flags, ref Ref) Meta {
	return Meta{
		EventTime:   m.EventTime,
		ReceiveTime: m.ReceiveTime,
		SourceSeq:   m.SourceSeq,
		SourceEpoch: m.SourceEpoch,
		Flags:       m.Flags | flags,
		Ref:         ref,
	}
}

// Meta is the metadata of a stored record.
type Meta struct {
	EventTime   Timestamp
	ReceiveTime Timestamp
	SourceSeq   uint64
	SourceEpoch SourceEpoch
	Flags       Flags // adapter bits | series-set bits
	Ref         Ref   // assigned by the Series
}

// Entry is a record copied out of a Series.
type Entry[T any] struct {
	Meta  Meta
	Value T
}

// Input is one item of an AppendBatch call.
type Input[T any] struct {
	Value T
	Meta  InputMeta
}

// Cursor is a subscription position: the generation and the next Seq to read.
type Cursor struct {
	Gen  Gen
	Next Seq
}

// timeKey is the internal event-time ordering key: EventTime, then Seq.
type timeKey struct {
	eventTime Timestamp
	seq       Seq
}

// keyLess orders timeKeys lexicographically by (eventTime, seq).
func keyLess(a, b timeKey) bool {
	if a.eventTime != b.eventTime {
		return a.eventTime < b.eventTime
	}
	return a.seq < b.seq
}
