package temporal

import (
	"math/bits"
	"testing"
	"time"
)

// §6.1: Timestamp is int64 Unix nanoseconds, UTC.
func TestTimestamp_RoundTrip(t *testing.T) {
	cases := []time.Time{
		time.Date(2026, 10, 11, 12, 30, 45, 123456789, time.UTC),
		time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC), // pre-1970
		time.Date(1700, 3, 4, 5, 6, 7, 8, time.UTC),
		time.Date(2262, 4, 11, 0, 0, 0, 1, time.UTC),
		time.Date(2026, 1, 2, 3, 4, 5, 6, time.FixedZone("UTC+3", 3*3600)),
	}
	for _, tc := range cases {
		ts := FromTime(tc)
		if int64(ts) != tc.UnixNano() {
			t.Fatalf("FromTime(%v) = %d, want %d", tc, ts, tc.UnixNano())
		}
		got := ts.Time()
		if !got.Equal(tc) {
			t.Fatalf("round trip %v: got %v", tc, got)
		}
		if got.Location() != time.UTC {
			t.Fatalf("Time() location = %v, want UTC", got.Location())
		}
	}
}

func TestTimestamp_Sentinels(t *testing.T) {
	if MinTimestamp != Timestamp(-1<<63) || MaxTimestamp != Timestamp(1<<63-1) {
		t.Fatalf("sentinels = %d, %d", MinTimestamp, MaxTimestamp)
	}
}

// §6.3: adapter and series flag masks are disjoint and cover all bits.
func TestFlags_Masks(t *testing.T) {
	if AdapterFlagsMask&SeriesFlagsMask != 0 {
		t.Fatal("masks overlap")
	}
	if AdapterFlagsMask|SeriesFlagsMask != ^Flags(0) {
		t.Fatal("masks do not cover all 32 bits")
	}
	for _, f := range []Flags{FlagGap, FlagSnapshot, FlagReplay, FlagCorrection} {
		if f&AdapterFlagsMask != f || bits.OnesCount32(uint32(f)) != 1 {
			t.Fatalf("adapter flag %#x not a single adapter bit", f)
		}
	}
	for _, f := range []Flags{FlagLate, FlagSourceSeqNotIncreasing} {
		if f&SeriesFlagsMask != f || bits.OnesCount32(uint32(f)) != 1 {
			t.Fatalf("series flag %#x not a single series bit", f)
		}
	}
	if FlagGap != 1 || FlagSnapshot != 2 || FlagReplay != 4 || FlagCorrection != 8 ||
		FlagLate != 1<<16 || FlagSourceSeqNotIncreasing != 1<<17 {
		t.Fatal("flag bit positions differ from §6.3")
	}
}

// §6.2: the zero Ref is invalid.
func TestRef_IsZero(t *testing.T) {
	cases := []struct {
		ref  Ref
		zero bool
	}{
		{Ref{}, true},
		{Ref{Gen: 3}, true}, // Seq 0 is invalid regardless of Gen
		{Ref{Gen: 1, Seq: 1}, false},
		{Ref{Seq: 1}, false},
	}
	for _, tc := range cases {
		if got := tc.ref.IsZero(); got != tc.zero {
			t.Errorf("%+v.IsZero() = %v, want %v", tc.ref, got, tc.zero)
		}
	}
}

// §6.5: timeKey order is EventTime, then Seq.
func TestKeyLess(t *testing.T) {
	cases := []struct {
		a, b timeKey
		less bool
	}{
		{timeKey{1, 5}, timeKey{2, 1}, true},
		{timeKey{2, 1}, timeKey{1, 5}, false},
		{timeKey{1, 1}, timeKey{1, 2}, true},
		{timeKey{1, 2}, timeKey{1, 1}, false},
		{timeKey{1, 1}, timeKey{1, 1}, false},
		{timeKey{MinTimestamp, seqMax}, timeKey{MinTimestamp + 1, 0}, true},
		{timeKey{-5, 9}, timeKey{3, 1}, true},
	}
	for _, tc := range cases {
		if got := keyLess(tc.a, tc.b); got != tc.less {
			t.Errorf("keyLess(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.less)
		}
	}
}
