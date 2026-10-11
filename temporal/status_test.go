package temporal

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// §7.1: Status.Err maps each status to its sentinel.
func TestStatus_Err(t *testing.T) {
	cases := []struct {
		s    Status
		want error
		name string
	}{
		{StatusOK, nil, "OK"},
		{StatusNotFound, ErrNotFound, "NotFound"},
		{StatusExpired, ErrExpired, "Expired"},
		{StatusReset, ErrReset, "Reset"},
		{StatusNotReady, ErrNotReady, "NotReady"},
		{StatusTimeIndexDisabled, ErrTimeIndexDisabled, "TimeIndexDisabled"},
		{StatusInvalid, ErrInvalidInput, "Invalid"},
	}
	for _, tc := range cases {
		if got := tc.s.Err(); got != tc.want {
			t.Errorf("%v.Err() = %v, want %v", tc.s, got, tc.want)
		}
		if got := tc.s.String(); got != tc.name {
			t.Errorf("String() = %q, want %q", got, tc.name)
		}
	}
	if got := Status(200).String(); got != "Status(200)" {
		t.Errorf("unknown status String() = %q", got)
	}
}

func TestSentinels_Distinct(t *testing.T) {
	all := []error{
		ErrNotFound, ErrExpired, ErrReset, ErrNotReady, ErrTimeIndexDisabled,
		ErrLagged, ErrClosed, ErrInvalidConfig, ErrInvalidInput, ErrLate,
		ErrSourceSeqNotIncreasing, ErrUniqueConflict, ErrBatchTooLarge,
		ErrSeqExhausted, ErrGenExhausted, ErrIndexBound,
	}
	for i, a := range all {
		if !strings.HasPrefix(a.Error(), "temporal: ") {
			t.Errorf("%q lacks the temporal: prefix", a)
		}
		for j, b := range all {
			if i != j && errors.Is(a, b) {
				t.Errorf("%v matches %v", a, b)
			}
		}
	}
}

// §21: typed errors match their sentinel through errors.Is and errors.As,
// also when wrapped.
func TestTypedErrors_IsAs(t *testing.T) {
	lag := &LagError{Cursor: Cursor{Gen: 1, Next: 3}, Oldest: Ref{Gen: 1, Seq: 10}, Missed: 7}
	conflict := &ConflictError{Index: "tradeID", Key: "42", Existing: Ref{Gen: 1, Seq: 5}, BatchPos: 2}

	cases := []struct {
		err      error
		sentinel error
		check    func(error) bool
	}{
		{lag, ErrLagged, func(e error) bool {
			var le *LagError
			return errors.As(e, &le) && le.Missed == 7 && le.Oldest.Seq == 10 && le.Cursor.Next == 3
		}},
		{conflict, ErrUniqueConflict, func(e error) bool {
			var ce *ConflictError
			return errors.As(e, &ce) && ce.Index == "tradeID" && ce.Existing.Seq == 5 && ce.BatchPos == 2
		}},
	}
	for _, tc := range cases {
		for _, err := range []error{tc.err, fmt.Errorf("wrapped: %w", tc.err)} {
			if !errors.Is(err, tc.sentinel) {
				t.Errorf("errors.Is(%v, %v) = false", err, tc.sentinel)
			}
			if errors.Is(err, ErrNotFound) {
				t.Errorf("%v unexpectedly matches ErrNotFound", err)
			}
			if !tc.check(err) {
				t.Errorf("errors.As failed for %v", err)
			}
			if !strings.HasPrefix(tc.err.Error(), "temporal: ") {
				t.Errorf("%q lacks the temporal: prefix", tc.err.Error())
			}
		}
	}
}

// §8.4: the cursor round-trips its position, including sentinel values.
func TestTimeCursor_EncodeDecode(t *testing.T) {
	if !(TimeCursor{}).IsZero() {
		t.Fatal("zero cursor not IsZero")
	}
	cases := []struct {
		last  timeKey
		bound Timestamp
	}{
		{timeKey{eventTime: 100, seq: 7}, 200},
		{timeKey{eventTime: -5, seq: 1}, 0},
		{timeKey{eventTime: MinTimestamp + 1, seq: seqMax - 1}, MaxTimestamp},
		{timeKey{eventTime: MaxTimestamp - 1, seq: 1}, MinTimestamp},
	}
	for _, tc := range cases {
		c := makeCursor(9, 4, tc.last, tc.bound)
		if c.IsZero() {
			t.Fatal("non-zero cursor reports IsZero")
		}
		if c.series != 9 || c.gen != 4 {
			t.Fatalf("identity = (%d, %d)", c.series, c.gen)
		}
		last, bound := c.decode()
		if last != tc.last || bound != tc.bound {
			t.Fatalf("decode = %v, %d; want %v, %d", last, bound, tc.last, tc.bound)
		}
	}
}
