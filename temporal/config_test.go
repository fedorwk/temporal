package temporal

import (
	"errors"
	"testing"
	"time"
)

// namedIndex returns a unique index with the given name.
func namedIndex(name string) Index[int] {
	return NewUniqueIndex(name, func(e *Entry[int]) (int, bool) { return e.Value, true }, ConflictReject)
}

// §12.1: New returns ErrInvalidConfig for every malformed config.
func TestConfig_ValidateInvalid(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config[int]
	}{
		{"zero capacity", Config[int]{Capacity: 0}},
		{"negative capacity", Config[int]{Capacity: -1}},
		{"negative MaxReadBatch", Config[int]{Capacity: 1, MaxReadBatch: -1}},
		{"unknown TimeIndexMode", Config[int]{Capacity: 1, TimeIndex: TimeIndexBTree + 1}},
		{"unknown CloneMode", Config[int]{Capacity: 1, CloneMode: CloneOnReadWrite + 1}},
		{"unknown LatePolicy", Config[int]{Capacity: 1, LatePolicy: LateReject + 1}},
		{"unknown SourceSeqPolicy", Config[int]{Capacity: 1, SourceSeqPolicy: SourceSeqReject + 1}},
		{"negative LateTolerance", Config[int]{Capacity: 1, LateTolerance: -time.Nanosecond}},
		{"duplicate index names", Config[int]{Capacity: 1, Indexes: []Index[int]{
			namedIndex("a"), namedIndex("b"), namedIndex("a"),
		}}},
		{"nil index", Config[int]{Capacity: 1, Indexes: []Index[int]{nil}}},
		{"unknown ConflictPolicy", Config[int]{Capacity: 1, Indexes: []Index[int]{
			NewUniqueIndex("u", func(e *Entry[int]) (int, bool) { return 0, true }, ConflictKeepFirst+1),
		}}},
		{"nil unique extractor", Config[int]{Capacity: 1, Indexes: []Index[int]{
			NewUniqueIndex[int, int]("u", nil, ConflictReject),
		}}},
		{"nil multi extractor", Config[int]{Capacity: 1, Indexes: []Index[int]{
			NewMultiIndex[int, int]("m", nil),
		}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.cfg.validate()
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("validate() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestConfig_ValidateNormalizes(t *testing.T) {
	clone := func(v int) int { return v }

	t.Run("defaults", func(t *testing.T) {
		n, err := (&Config[int]{Capacity: 3}).validate()
		if err != nil {
			t.Fatal(err)
		}
		if n.capacity != 3 || n.maxReadBatch != 4096 || n.timeIndex != TimeIndexNone {
			t.Fatalf("normalized = %+v", n)
		}
		if n.clone != nil || n.cloneOnRead || n.cloneOnWrite {
			t.Fatal("clone enabled without a Clone func")
		}
	})

	t.Run("explicit values", func(t *testing.T) {
		n, err := (&Config[int]{
			Name:                 "trades",
			Capacity:             10,
			TimeIndex:            TimeIndexBTree,
			MaxReadBatch:         7,
			LatePolicy:           LateReject,
			LateTolerance:        2 * time.Millisecond,
			SourceSeqPolicy:      SourceSeqFlag,
			GateReadsByWatermark: true,
			Indexes:              []Index[int]{namedIndex("a"), namedIndex("b")},
		}).validate()
		if err != nil {
			t.Fatal(err)
		}
		if n.name != "trades" || n.maxReadBatch != 7 || n.timeIndex != TimeIndexBTree ||
			n.latePolicy != LateReject || n.lateTolerance != 2_000_000 ||
			n.sourceSeqPolicy != SourceSeqFlag || !n.gate || len(n.indexes) != 2 {
			t.Fatalf("normalized = %+v", n)
		}
	})

	// P-17: CloneMode is ignored when Clone is nil.
	t.Run("clone modes", func(t *testing.T) {
		cases := []struct {
			clone       func(int) int
			mode        CloneMode
			read, write bool
		}{
			{nil, CloneOnRead, false, false},
			{nil, CloneOnReadWrite, false, false},
			{clone, CloneOnRead, true, false},
			{clone, CloneOnWrite, false, true},
			{clone, CloneOnReadWrite, true, true},
		}
		for _, tc := range cases {
			n, err := (&Config[int]{Capacity: 1, Clone: tc.clone, CloneMode: tc.mode}).validate()
			if err != nil {
				t.Fatal(err)
			}
			if n.cloneOnRead != tc.read || n.cloneOnWrite != tc.write {
				t.Errorf("mode %d (clone set %v): read=%v write=%v, want %v %v",
					tc.mode, tc.clone != nil, n.cloneOnRead, n.cloneOnWrite, tc.read, tc.write)
			}
		}
	})
}
