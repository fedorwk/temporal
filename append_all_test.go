package temporal

import (
	"errors"
	"slices"
	"testing"
)

func inputs(from, n int) []Input[int] {
	items := make([]Input[int], n)
	for i := range items {
		v := from + i
		items[i] = Input[int]{Value: v, Meta: at(Timestamp(v))}
	}
	return items
}

// AppendAll accepts more items than Capacity and behaves like the same Appends
// in order: the newest Capacity records are retained.
func TestAppendAll_Chunks(t *testing.T) {
	for _, mode := range allModes {
		s := mustNew(t, withMode(mode, Config[int]{Capacity: 5, MaxReadBatch: 3}))
		sub := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest(), OnLag: LagSkip})

		res, err := s.AppendAll(nil, inputs(1, 12))
		if err != nil || len(res) != 12 {
			t.Fatalf("mode %d: AppendAll = %d results, %v", mode, len(res), err)
		}
		for i, r := range res {
			if r.Outcome != OutcomeAppended || r.Ref != (Ref{Gen: 1, Seq: Seq(i + 1)}) {
				t.Fatalf("mode %d: result %d = %+v", mode, i, r)
			}
		}
		got, info := s.ReadFrom(nil, Ref{}, 0) // pages of MaxReadBatch
		got, _ = s.ReadFrom(got, Ref{Gen: 1, Seq: info.LastRef.Seq + 1}, 0)
		if !slices.Equal(values(got), []int{8, 9, 10, 11, 12}) {
			t.Fatalf("mode %d: retained %v", mode, values(got))
		}
		if st := s.Stats(); st.Appended != 12 || st.Evicted != 7 {
			t.Fatalf("mode %d: stats %+v", mode, st)
		}
		if got, err := sub.TryNext(nil, 0); err != nil || len(got) == 0 {
			t.Fatalf("mode %d: subscriber saw %v, %v", mode, values(got), err)
		}
		mustInvariants(t, s)
	}
}

// A failing chunk leaves earlier chunks applied, applies nothing from itself,
// and the error names the absolute position.
func TestAppendAll_PartialFailure(t *testing.T) {
	t.Run("conflict", func(t *testing.T) {
		ix := NewUniqueIndex("id", byID, ConflictReject)
		s := mustNew(t, Config[trade]{Capacity: 16, MaxReadBatch: 2, Indexes: []Index[trade]{ix}})
		items := []Input[trade]{
			{Value: trade{ID: 1}, Meta: at(1)},
			{Value: trade{ID: 2}, Meta: at(2)},
			{Value: trade{ID: 3}, Meta: at(3)},
			{Value: trade{ID: 1}, Meta: at(4)}, // conflicts with item 0
			{Value: trade{ID: 5}, Meta: at(5)},
		}
		res, err := s.AppendAll(nil, items)
		var ce *ConflictError
		if !errors.As(err, &ce) || ce.BatchPos != 3 {
			t.Fatalf("err = %v", err)
		}
		if len(res) != 2 || s.Len() != 2 {
			t.Fatalf("applied %d results, Len %d", len(res), s.Len())
		}
		mustInvariants(t, s)
	})
	t.Run("late", func(t *testing.T) {
		s := mustNew(t, Config[int]{Capacity: 16, MaxReadBatch: 2, LatePolicy: LateReject})
		items := inputs(10, 4)
		items[3].Meta.EventTime = 1
		res, err := s.AppendAll(nil, items)
		if !errors.Is(err, ErrLate) || len(res) != 2 || s.Len() != 2 {
			t.Fatalf("err %v, %d results, Len %d", err, len(res), s.Len())
		}
	})
	t.Run("closed", func(t *testing.T) {
		s := mustNew(t, Config[int]{Capacity: 4})
		s.CloseWithError(errFeed)
		if _, err := s.AppendAll(nil, inputs(1, 2)); !errors.Is(err, ErrClosed) || !errors.Is(err, errFeed) {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestAppendAll_Empty(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 4})
	dst := make([]AppendResult, 1, 4)
	res, err := s.AppendAll(dst, nil)
	if err != nil || len(res) != 1 || s.Len() != 0 {
		t.Fatalf("AppendAll(nil) = %d, %v", len(res), err)
	}
}

// With a reused dst, AppendAll allocates nothing on the TimeIndexNone path.
func TestAllocs_AppendAll(t *testing.T) {
	skipAllocTest(t)
	s := mustNew(t, Config[int]{Capacity: 64, MaxReadBatch: 16})
	items := inputs(1, 100)
	dst := make([]AppendResult, 0, len(items))
	if n := allocsPerOp(100, func() {
		var err error
		if dst, err = s.AppendAll(dst[:0], items); err != nil {
			t.Fatal(err)
		}
	}); n != 0 {
		t.Fatalf("AppendAll allocs = %v", n)
	}
}
