package temporal

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

func mustSubscribe[T any](t testing.TB, s *Series[T], opts SubscribeOptions) *Subscription[T] {
	t.Helper()
	sub, err := s.Subscribe(opts)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	return sub
}

func values[T any](es []Entry[T]) []T {
	out := []T{}
	for _, e := range es {
		out = append(out, e.Value)
	}
	return out
}

// S-001, S-002: start positions.
func TestSubscribe_StartPositions(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 4})
	empty := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest()})
	if empty.Cursor() != (Cursor{Gen: 1, Next: 1}) {
		t.Fatalf("StartOldest on empty: %+v (P-07)", empty.Cursor())
	}
	for i := 1; i <= 6; i++ {
		mustAppend(t, s, i, at(Timestamp(i)))
	}
	cases := []struct {
		name  string
		start StartFrom
		want  Cursor
	}{
		{"zero value is StartNext", StartFrom{}, Cursor{1, 7}},
		{"StartNext", StartNext(), Cursor{1, 7}},
		{"StartOldest", StartOldest(), Cursor{1, 3}},
		{"StartAt zero ref", StartAt(Ref{}), Cursor{1, 3}},
		{"StartAt", StartAt(Ref{1, 5}), Cursor{1, 5}},
	}
	for _, c := range cases {
		sub := mustSubscribe(t, s, SubscribeOptions{Start: c.start})
		if sub.Cursor() != c.want {
			t.Errorf("%s: cursor %+v, want %+v", c.name, sub.Cursor(), c.want)
		}
	}
	if _, err := s.Subscribe(SubscribeOptions{OnLag: LagSkip + 1}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("invalid OnLag: %v", err)
	}
	if got := s.Stats().Subscribers; got != 6 {
		t.Fatalf("Subscribers = %d", got)
	}
}

// T-007, S-003, S-010: replay across batches in Seq order with any limit.
func TestSubscription_ReplayAcrossBatches(t *testing.T) {
	for _, limit := range []int{0, 1, 2, 3, 7} {
		s := mustNew(t, Config[int]{Capacity: 64, MaxReadBatch: 5})
		sub := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest()})
		v := 0
		var want []int
		for _, n := range []int{1, 3, 0, 6, 2} {
			items := make([]Input[int], n)
			for i := range items {
				v++
				items[i] = Input[int]{Value: v, Meta: at(Timestamp(v))}
				want = append(want, v)
			}
			if _, err := s.AppendBatch(nil, items); err != nil {
				t.Fatal(err)
			}
		}
		var got []int
		for {
			batch, err := sub.TryNext(nil, limit)
			if err != nil {
				t.Fatal(err)
			}
			if len(batch) == 0 {
				break
			}
			if eff := min(limit, 5); limit > 0 && len(batch) > eff || len(batch) > 5 {
				t.Fatalf("limit %d: batch of %d", limit, len(batch))
			}
			got = append(got, values(batch)...)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("limit %d: got %v, want %v", limit, got, want)
		}
		if sub.Cursor() != (Cursor{Gen: 1, Next: Seq(v + 1)}) {
			t.Fatalf("cursor %+v", sub.Cursor())
		}
	}
}

// S-004, S-005, E-008, T-007: LagFail reports the cursor, oldest and missed
// count without data or cursor movement; LagSkip jumps and counts.
func TestSubscription_Lag(t *testing.T) {
	setup := func(policy LagPolicy) (*Series[int], *Subscription[int]) {
		s := mustNew(t, Config[int]{Capacity: 4})
		sub := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest(), OnLag: policy})
		for i := 1; i <= 10; i++ {
			mustAppend(t, s, i, at(Timestamp(i)))
		}
		return s, sub
	}

	t.Run("LagFail", func(t *testing.T) {
		_, sub := setup(LagFail)
		for range 2 {
			dst := []Entry[int]{{Value: -1}}
			got, err := sub.Next(context.Background(), dst, 0)
			var le *LagError
			if !errors.Is(err, ErrLagged) || !errors.As(err, &le) {
				t.Fatalf("error = %v", err)
			}
			want := LagError{Cursor: Cursor{1, 1}, Oldest: Ref{1, 7}, Missed: 6}
			if *le != want || len(got) != 1 {
				t.Fatalf("lag %+v, dst %v", *le, got)
			}
			if sub.Cursor() != (Cursor{1, 1}) || sub.Skipped() != 0 {
				t.Fatalf("cursor moved: %+v", sub.Cursor())
			}
		}
		if err := sub.Seek(StartOldest()); err != nil {
			t.Fatal(err)
		}
		got, err := sub.TryNext(nil, 0)
		if err != nil || !slices.Equal(values(got), []int{7, 8, 9, 10}) {
			t.Fatalf("after Seek: %v %v", values(got), err)
		}
	})

	t.Run("LagSkip", func(t *testing.T) {
		_, sub := setup(LagSkip)
		got, err := sub.Next(context.Background(), nil, 2)
		if err != nil || !slices.Equal(values(got), []int{7, 8}) || sub.Skipped() != 6 {
			t.Fatalf("got %v err %v skipped %d", values(got), err, sub.Skipped())
		}
		if sub.Cursor() != (Cursor{1, 9}) {
			t.Fatalf("cursor %+v", sub.Cursor())
		}
	})

	t.Run("one record behind", func(t *testing.T) {
		for _, policy := range []LagPolicy{LagFail, LagSkip} {
			s := mustNew(t, Config[int]{Capacity: 4})
			sub := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest(), OnLag: policy})
			for i := 1; i <= 5; i++ { // evicts exactly Seq 1
				mustAppend(t, s, i, at(Timestamp(i)))
			}
			got, err := sub.TryNext(nil, 0)
			var le *LagError
			switch policy {
			case LagFail:
				if !errors.As(err, &le) || le.Missed != 1 || le.Oldest != (Ref{1, 2}) || len(got) != 0 {
					t.Fatalf("LagFail: %v %v", values(got), err)
				}
			case LagSkip:
				if err != nil || !slices.Equal(values(got), []int{2, 3, 4, 5}) || sub.Skipped() != 1 {
					t.Fatalf("LagSkip: %v %v skipped %d", values(got), err, sub.Skipped())
				}
			}
		}
	})

	t.Run("StartAt an evicted ref lags", func(t *testing.T) {
		s, _ := setup(LagFail)
		sub := mustSubscribe(t, s, SubscribeOptions{Start: StartAt(Ref{1, 2})})
		if _, err := sub.TryNext(nil, 0); !errors.Is(err, ErrLagged) {
			t.Fatalf("error = %v", err)
		}
	})
}

// S-006, T-016: after Clear, reads return ErrReset until Seek; every Seek
// variant resynchronizes.
func TestSubscription_ResetAndSeek(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 8})
	for i := 1; i <= 3; i++ {
		mustAppend(t, s, i, at(Timestamp(i)))
	}
	sub := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest()})
	if _, err := sub.TryNext(nil, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Clear(); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 4; i++ {
		mustAppend(t, s, 10*i, at(Timestamp(i)))
	}
	before := sub.Cursor()
	for range 2 {
		if _, err := sub.Next(context.Background(), nil, 0); !errors.Is(err, ErrReset) {
			t.Fatalf("error = %v, want ErrReset", err)
		}
	}
	if sub.Cursor() != before {
		t.Fatal("cursor moved while reset")
	}

	cases := []struct {
		name  string
		start StartFrom
		want  []int
		err   error
	}{
		{"StartOldest", StartOldest(), []int{10, 20, 30, 40}, nil},
		{"StartNext", StartNext(), []int{}, nil},
		{"StartAt current gen", StartAt(Ref{2, 3}), []int{30, 40}, nil},
		{"StartAt beyond newest+1 clamps", StartAt(Ref{2, 99}), []int{}, nil},
		{"StartAt zero ref", StartAt(Ref{}), []int{10, 20, 30, 40}, nil},
		{"StartAt old gen", StartAt(Ref{1, 2}), nil, ErrReset},
		{"StartAt future gen", StartAt(Ref{3, 1}), nil, ErrInvalidInput},
	}
	for _, c := range cases {
		if err := sub.Seek(c.start); err != nil {
			t.Fatal(err)
		}
		got, err := sub.TryNext(nil, 0)
		if !errors.Is(err, c.err) && !(c.err == nil && err == nil) {
			t.Fatalf("%s: error %v, want %v", c.name, err, c.err)
		}
		if c.err == nil && !slices.Equal(values(got), c.want) {
			t.Fatalf("%s: got %v, want %v", c.name, values(got), c.want)
		}
	}
	if err := sub.Seek(StartAt(Ref{2, 99})); err != nil {
		t.Fatal(err)
	}
	sub.TryNext(nil, 0)
	mustAppend(t, s, 50, at(5))
	if got, _ := sub.TryNext(nil, 0); !slices.Equal(values(got), []int{50}) {
		t.Fatalf("clamped cursor did not wait for future data: %v", values(got))
	}
}

// T-019, E-014, S-008, S-009: cancellation and close unblock Next with the
// specified error and an unchanged cursor.
func TestSubscription_UnblockAndClose(t *testing.T) {
	blocked := func(t *testing.T, sub *Subscription[int], ctx context.Context) <-chan error {
		t.Helper()
		ch := make(chan error, 1)
		go func() {
			_, err := sub.Next(ctx, nil, 0)
			ch <- err
		}()
		select {
		case err := <-ch:
			t.Fatalf("Next returned early: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
		return ch
	}
	wait := func(t *testing.T, ch <-chan error) error {
		t.Helper()
		select {
		case err := <-ch:
			return err
		case <-time.After(5 * time.Second):
			t.Fatal("Next did not unblock")
			return nil
		}
	}

	t.Run("ctx cancel", func(t *testing.T) {
		s := mustNew(t, Config[int]{Capacity: 4})
		sub := mustSubscribe(t, s, SubscribeOptions{})
		ctx, cancel := context.WithCancel(context.Background())
		ch := blocked(t, sub, ctx)
		cancel()
		if err := wait(t, ch); !errors.Is(err, context.Canceled) || sub.Cursor() != (Cursor{1, 1}) {
			t.Fatalf("err %v cursor %+v", err, sub.Cursor())
		}
		// E-014: a cancelled ctx wins even when data is available.
		mustAppend(t, s, 1, at(1))
		if got, err := sub.Next(ctx, nil, 0); !errors.Is(err, context.Canceled) || len(got) != 0 || sub.Cursor().Next != 1 {
			t.Fatalf("cancelled ctx with data: %v %v %+v", got, err, sub.Cursor())
		}
	})

	t.Run("Subscription.Close", func(t *testing.T) {
		s := mustNew(t, Config[int]{Capacity: 4})
		sub := mustSubscribe(t, s, SubscribeOptions{})
		ch := blocked(t, sub, context.Background())
		if err := sub.Close(); err != nil {
			t.Fatal(err)
		}
		if err := wait(t, ch); !errors.Is(err, ErrClosed) || sub.Cursor() != (Cursor{1, 1}) {
			t.Fatalf("err %v cursor %+v", err, sub.Cursor())
		}
		if err := sub.Close(); err != nil {
			t.Fatal("second Close")
		}
		if s.Stats().Subscribers != 0 {
			t.Fatal("Close did not unsubscribe")
		}
		if err := sub.Seek(StartOldest()); !errors.Is(err, ErrClosed) {
			t.Fatalf("Seek after Close: %v", err)
		}
		mustAppend(t, s, 1, at(1))
		if _, err := sub.TryNext(nil, 0); !errors.Is(err, ErrClosed) {
			t.Fatalf("TryNext after Close: %v", err)
		}
	})

	t.Run("Series.Close drains then ErrClosed", func(t *testing.T) {
		s := mustNew(t, Config[int]{Capacity: 8})
		sub := mustSubscribe(t, s, SubscribeOptions{})
		ch := blocked(t, sub, context.Background())
		mustAppend(t, s, 1, at(1))
		if err := wait(t, ch); err != nil {
			t.Fatal(err)
		}
		mustAppend(t, s, 2, at(2))
		mustAppend(t, s, 3, at(3))
		s.Close()
		got, err := sub.Next(context.Background(), nil, 1)
		if err != nil || !slices.Equal(values(got), []int{2}) {
			t.Fatalf("drain 1: %v %v", values(got), err)
		}
		got, err = sub.Next(context.Background(), nil, 0)
		if err != nil || !slices.Equal(values(got), []int{3}) {
			t.Fatalf("drain 2: %v %v", values(got), err)
		}
		cur := sub.Cursor()
		if _, err := sub.Next(context.Background(), nil, 0); !errors.Is(err, ErrClosed) || sub.Cursor() != cur {
			t.Fatalf("after drain: %v", err)
		}
		// A blocked Next is woken by Series.Close.
		s2 := mustNew(t, Config[int]{Capacity: 8})
		sub2 := mustSubscribe(t, s2, SubscribeOptions{})
		ch2 := blocked(t, sub2, context.Background())
		s2.Close()
		if err := wait(t, ch2); !errors.Is(err, ErrClosed) {
			t.Fatalf("blocked Next on Series.Close: %v", err)
		}
		// P-07: Subscribe on a closed Series succeeds and drains.
		late := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest()})
		if got, err := late.Next(context.Background(), nil, 0); err != nil || len(got) != 3 {
			t.Fatalf("late subscriber: %v %v", values(got), err)
		}
	})
}

// S-007, S-011, T-018, §17.3: wake-ups are coalesced, producers never block,
// and watermark changes do not wake.
func TestSubscription_WakeCoalescing(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 16})
	var subs []*Subscription[int]
	for range 50 {
		subs = append(subs, mustSubscribe(t, s, SubscribeOptions{})) // never read
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 20000 {
			if _, err := s.Append(i, at(Timestamp(i+1))); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("producer blocked by stalled subscribers")
	}
	for _, sub := range subs {
		if n := len(sub.Wake()); n != 1 {
			t.Fatalf("wake buffer length %d, want exactly 1 pending token", n)
		}
	}
	<-subs[0].Wake()
	s.SetWatermark(5)
	if len(subs[0].Wake()) != 0 {
		t.Fatal("SetWatermark woke a subscriber")
	}
	// A select loop around TryNext.
	mustAppend(t, s, -1, at(30000))
	select {
	case <-subs[0].Wake():
	default:
		t.Fatal("no wake-up after Append")
	}
}

// W-002, §9.4, §17.3: one notification per committed batch; none for a failed
// or fully dropped batch. Counted in temporaldebug builds.
func TestNotify_OncePerTransaction(t *testing.T) {
	if !debugAssert {
		t.Skip("notification counter exists only in temporaldebug builds")
	}
	ix := NewUniqueIndex("id", byID, ConflictDrop)
	s := mustNew(t, Config[trade]{Capacity: 8, LatePolicy: LateReject, Indexes: []Index[trade]{ix}})
	count := func() uint64 { return s.dbgNotifies.Load() }
	base := count()
	if _, err := s.AppendBatch(nil, tradeInputs([3]int{1, 0, 1}, [3]int{2, 0, 2}, [3]int{3, 0, 3})); err != nil {
		t.Fatal(err)
	}
	if got := count() - base; got != 1 {
		t.Fatalf("batch: %d notifications", got)
	}
	s.AppendBatch(nil, tradeInputs([3]int{1, 0, 4}, [3]int{2, 0, 5})) // all dropped
	s.AppendBatch(nil, tradeInputs([3]int{9, 0, 6}, [3]int{8, 0, 1})) // late reject
	s.Append(trade{ID: 1}, at(7))                                     // dropped
	if got := count() - base; got != 1 {
		t.Fatalf("no-op transactions notified: %d", got)
	}
	s.Append(trade{ID: 4}, at(8))
	s.Clear()
	s.Close()
	if got := count() - base; got != 4 {
		t.Fatalf("Append, Clear, Close: %d notifications, want 4", got)
	}
}

// T-018, T-001: several producers and consumers; every record arrives exactly
// once and in Seq order at every consumer.
func TestSubscription_Stress(t *testing.T) {
	const producers, consumers = 4, 4
	per := iters(5000)
	total := producers * per
	s := mustNew(t, Config[int]{Capacity: total, MaxReadBatch: 64})
	subs := make([]*Subscription[int], consumers)
	for i := range subs {
		subs[i] = mustSubscribe(t, s, SubscribeOptions{Start: StartOldest()})
	}
	var wg sync.WaitGroup
	results := make([][]Entry[int], consumers)
	for i, sub := range subs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]Entry[int], 0, 64)
			for len(results[i]) < total {
				var err error
				buf, err = sub.Next(context.Background(), buf[:0], 0)
				if err != nil {
					t.Error(err)
					return
				}
				results[i] = append(results[i], buf...)
			}
		}()
	}
	var pw sync.WaitGroup
	for p := range producers {
		pw.Add(1)
		go func() {
			defer pw.Done()
			for i := 0; i < per; {
				if i%3 == 0 && i+4 <= per {
					items := make([]Input[int], 4)
					for j := range items {
						items[j] = Input[int]{Value: p*per + i + j, Meta: at(1)}
					}
					if _, err := s.AppendBatch(nil, items); err != nil {
						t.Error(err)
						return
					}
					i += 4
					continue
				}
				if _, err := s.Append(p*per+i, at(1)); err != nil {
					t.Error(err)
					return
				}
				i++
			}
		}()
	}
	pw.Wait()
	wg.Wait()
	all, _ := s.ReadFrom(nil, Ref{}, 0)
	for len(all) < total {
		more, _ := s.ReadFrom(nil, Ref{Gen: 1, Seq: Seq(len(all) + 1)}, 0)
		all = append(all, more...)
	}
	for i, got := range results {
		if !slices.Equal(got, all) {
			t.Fatalf("consumer %d saw %d records, differing from the Series", i, len(got))
		}
	}
}

// S-013, Q-017: subscriptions behave identically in every mode.
func TestSubscription_ModeIndependence(t *testing.T) {
	run := func(mode TimeIndexMode) []any {
		s := mustNew(t, Config[int]{Capacity: 5, TimeIndex: mode})
		fail := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest()})
		skip := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest(), OnLag: LagSkip})
		var trace []any
		for i := range 40 {
			mustAppend(t, s, i, at(Timestamp(40-i)))
			if i%7 == 0 {
				for _, sub := range []*Subscription[int]{fail, skip} {
					got, err := sub.TryNext(nil, 3)
					trace = append(trace, values(got), errString(err), sub.Cursor(), sub.Skipped())
				}
			}
			if i == 25 {
				s.Clear()
			}
			if i == 30 {
				fail.Seek(StartOldest())
				skip.Seek(StartNext())
			}
		}
		return trace
	}
	a, b := run(TimeIndexNone), run(TimeIndexBTree)
	if len(a) != len(b) {
		t.Fatal("trace lengths differ")
	}
	for i := range a {
		if !equalAny(a[i], b[i]) {
			t.Fatalf("step %d: %v vs %v", i, a[i], b[i])
		}
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func equalAny(a, b any) bool {
	if x, ok := a.([]int); ok {
		return slices.Equal(x, b.([]int))
	}
	return a == b
}

// T-015, S-005: a Clone panic during Next leaves the mutex free and the cursor
// unmoved.
func TestSubscription_ClonePanic(t *testing.T) {
	clone := func(v int) int {
		if v < 0 {
			panic("clone")
		}
		return v
	}
	s := mustNew(t, Config[int]{Capacity: 4, Clone: clone})
	sub := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest(), OnLag: LagSkip})
	mustAppend(t, s, 1, at(1))
	mustAppend(t, s, -1, at(2))
	mustAppend(t, s, 3, at(3))
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("no panic")
			}
		}()
		sub.TryNext(nil, 0)
	}()
	if sub.Cursor() != (Cursor{1, 1}) || sub.Skipped() != 0 {
		t.Fatalf("cursor moved: %+v", sub.Cursor())
	}
	mustAppend(t, s, 4, at(4)) // the lock is free
}

// T-026, W-009, S-007: Append stays allocation-free with subscribers attached;
// reads with sufficient cap(dst) do not allocate.
func TestAllocs_Subscriptions(t *testing.T) {
	skipAllocTest(t)
	s := mustNew(t, Config[int]{Capacity: 4096})
	var subs []*Subscription[int]
	for range 8 {
		subs = append(subs, mustSubscribe(t, s, SubscribeOptions{Start: StartOldest(), OnLag: LagSkip}))
	}
	ts := Timestamp(0)
	appendOne := func() {
		ts++
		if _, err := s.Append(1, at(ts)); err != nil {
			t.Fatal(err)
		}
	}
	if n := allocsPerOp(2000, appendOne); n != 0 {
		t.Errorf("Append with subscribers: %.4f allocs/op", n)
	}
	dst := make([]Entry[int], 0, 16)
	ctx := context.Background()
	// One record per call: 3x200+1 calls stay within the 4096 retained records,
	// so Next never blocks.
	if n := allocsPerOp(200, func() { dst, _ = subs[0].TryNext(dst[:0], 1) }); n != 0 {
		t.Errorf("TryNext: %.4f allocs/op", n)
	}
	if n := allocsPerOp(200, func() { dst, _ = subs[1].Next(ctx, dst[:0], 1) }); n != 0 {
		t.Errorf("Next with data available: %.4f allocs/op", n)
	}
}

// S12-02: NextState returns the source state captured with the read, and the
// zero state on errors.
func TestSubscription_NextState(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 4})
	sub := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest()})
	ctx := context.Background()
	mustAppend(t, s, 1, at(30))
	mustAppend(t, s, 2, at(10)) // late: MaxObservedEventTime stays 30
	s.SetWatermark(25)

	got, state, err := sub.NextState(ctx, nil, 1)
	want := SourceState{Gen: 1, Newest: Ref{1, 2}, Watermark: 25, MaxObservedEventTime: 30}
	if err != nil || len(got) != 1 || state != want {
		t.Fatalf("NextState = %v %+v %v, want state %+v", values(got), state, err, want)
	}
	if got[0].Meta.Ref.Seq == state.Newest.Seq {
		t.Fatal("first batch should not be caught up")
	}
	got, state, _ = sub.NextState(ctx, nil, 0)
	if got[len(got)-1].Meta.Ref != state.Newest {
		t.Fatal("second batch should reach Newest")
	}

	s.Clear()
	if _, state, err := sub.NextState(ctx, nil, 0); !errors.Is(err, ErrReset) || state != (SourceState{}) {
		t.Fatalf("after Clear: %+v %v, want zero state and ErrReset", state, err)
	}
	sub.Seek(StartOldest())
	mustAppend(t, s, 3, at(1))
	if _, state, _ := sub.NextState(ctx, nil, 0); state != (SourceState{Gen: 2, Newest: Ref{2, 1}, Watermark: MinTimestamp, MaxObservedEventTime: 1}) {
		t.Fatalf("after Clear and Seek: %+v", state)
	}
}
