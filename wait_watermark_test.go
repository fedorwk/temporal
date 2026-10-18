package temporal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// waitAsync runs WaitWatermark on its own goroutine.
func waitAsync(ctx context.Context, s *Series[int], ts Timestamp) <-chan error {
	ch := make(chan error, 1)
	go func() {
		wm, err := s.WaitWatermark(ctx, ts)
		if err == nil && wm < ts {
			err = errors.New("returned below the target")
		}
		ch <- err
	}()
	return ch
}

func pending(t *testing.T, ch <-chan error) {
	t.Helper()
	select {
	case err := <-ch:
		t.Fatalf("WaitWatermark returned early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
}

func returned(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("WaitWatermark did not return")
		return nil
	}
}

func TestWaitWatermark_AlreadyReached(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 4})
	s.SetWatermark(10)
	if wm, err := s.WaitWatermark(context.Background(), 10); err != nil || wm != 10 {
		t.Fatalf("WaitWatermark = %d, %v", wm, err)
	}
	s.Close()
	if wm, err := s.WaitWatermark(context.Background(), 5); err != nil || wm != 10 {
		t.Fatalf("WaitWatermark on closed Series below the watermark = %d, %v", wm, err)
	}
}

// Each waiter returns once the watermark reaches its own target, however the
// watermark advances.
func TestWaitWatermark_WakesOnAdvance(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 4})
	ctx := context.Background()
	w10, w20 := waitAsync(ctx, s, 10), waitAsync(ctx, s, 20)
	pending(t, w10)

	s.SetWatermark(5)
	pending(t, w10)
	s.SetWatermark(15)
	if err := returned(t, w10); err != nil {
		t.Fatal(err)
	}
	pending(t, w20)
	s.SetWatermark(MaxTimestamp)
	if err := returned(t, w20); err != nil {
		t.Fatal(err)
	}
}

func TestWaitWatermark_Cancel(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 4})
	ctx, cancel := context.WithCancel(context.Background())
	w := waitAsync(ctx, s, 10)
	pending(t, w)
	cancel()
	if err := returned(t, w); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

// Closing below the target ends the wait with the close error.
func TestWaitWatermark_Close(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 4})
	w := waitAsync(context.Background(), s, 10)
	pending(t, w)
	s.CloseWithError(errFeed)
	if err := returned(t, w); !errors.Is(err, ErrClosed) || !errors.Is(err, errFeed) {
		t.Fatalf("err = %v", err)
	}

	clean := mustNew(t, Config[int]{Capacity: 4})
	w = waitAsync(context.Background(), clean, 10)
	pending(t, w)
	clean.Close()
	if err := returned(t, w); err != ErrClosed {
		t.Fatalf("err = %v", err)
	}
}

// Clear resets the watermark; a waiter keeps waiting for the new timeline.
func TestWaitWatermark_Clear(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 4})
	s.SetWatermark(5)
	w := waitAsync(context.Background(), s, 10)
	pending(t, w)
	if err := s.Clear(); err != nil {
		t.Fatal(err)
	}
	pending(t, w)
	s.SetWatermark(10)
	if err := returned(t, w); err != nil {
		t.Fatal(err)
	}
}

// Many waiters and a producer advancing one step at a time: every waiter
// returns at its target.
func TestWaitWatermark_Concurrent(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 4})
	const n = 200
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wm, err := s.WaitWatermark(context.Background(), Timestamp(i))
			if err == nil && wm < Timestamp(i) {
				err = errors.New("returned below the target")
			}
			errs <- err
		}()
	}
	for ts := Timestamp(1); ts <= n; ts++ {
		s.SetWatermark(ts)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

// SetWatermark without waiters allocates nothing.
func TestAllocs_SetWatermarkNoWaiters(t *testing.T) {
	skipAllocTest(t)
	s := mustNew(t, Config[int]{Capacity: 4})
	ts := Timestamp(0)
	if n := allocsPerOp(1000, func() { ts++; s.SetWatermark(ts) }); n != 0 {
		t.Fatalf("SetWatermark allocs = %v", n)
	}
}

// readResult is what one Read returned.
type readResult struct {
	got []Entry[int]
	u   Update
	err error
}

// nextState runs Read on its own goroutine.
func nextState(sub *Subscription[int]) <-chan readResult {
	ch := make(chan readResult, 1)
	go func() {
		got, u, err := sub.Read(context.Background(), nil, 0)
		ch <- readResult{got, u, err}
	}()
	return ch
}

// With WatermarkUpdates, a watermark set after the last record wakes the
// subscription, which returns the new state with no records, once.
func TestSubscription_WatermarkUpdates(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 8})
	sub := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest(), WatermarkUpdates: true})
	ctx := context.Background()

	mustAppend(t, s, 1, at(10))
	got, u, err := sub.Read(ctx, nil, 0)
	if err != nil || len(got) != 1 || u.State.Watermark != MinTimestamp || u.WatermarkAdvanced {
		t.Fatalf("records: %v %+v %v", values(got), u, err)
	}

	ch := nextState(sub)
	select {
	case r := <-ch:
		t.Fatalf("returned without a change: %+v", r)
	case <-time.After(20 * time.Millisecond):
	}
	s.SetWatermark(10)
	select {
	case r := <-ch:
		if r.err != nil || len(r.got) != 0 || r.u.State.Watermark != 10 || r.u.State.Newest != (Ref{1, 1}) || !r.u.WatermarkAdvanced || r.u.Records != 0 {
			t.Fatalf("update: %+v", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SetWatermark did not wake the subscription")
	}

	// Reported once: nothing more is pending.
	if got, err := sub.TryNext(nil, 0); err != nil || len(got) != 0 {
		t.Fatalf("TryNext after update: %v %v", values(got), err)
	}

	// Records returned together with an advance report it; it is not repeated.
	mustAppend(t, s, 2, at(20))
	s.SetWatermark(20)
	got, u, err = sub.Read(ctx, nil, 0)
	if err != nil || len(got) != 1 || u.State.Watermark != 20 || !u.WatermarkAdvanced || u.Records != 1 {
		t.Fatalf("record with advance: %v %+v %v", values(got), u, err)
	}
	if got, err := sub.TryNext(nil, 0); err != nil || len(got) != 0 {
		t.Fatalf("TryNext after record: %v %v", values(got), err)
	}
}

// A watermark set just before Close is still reported before ErrClosed.
func TestSubscription_WatermarkUpdatesBeforeClose(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 8})
	sub := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest(), WatermarkUpdates: true})
	mustAppend(t, s, 1, at(10))
	s.SetWatermark(MaxTimestamp)
	s.Close()

	ctx := context.Background()
	got, u, err := sub.Read(ctx, nil, 0)
	if err != nil || len(got) != 1 || u.State.Watermark != MaxTimestamp || !u.WatermarkAdvanced {
		t.Fatalf("drain: %v %+v %v", values(got), u, err)
	}
	if _, _, err := sub.Read(ctx, nil, 0); err != ErrClosed {
		t.Fatalf("after drain: %v", err)
	}
}

// Without the option, watermark changes neither wake nor return.
func TestSubscription_NoWatermarkUpdates(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 8})
	sub := mustSubscribe(t, s, SubscribeOptions{})
	s.SetWatermark(10)
	if got, err := sub.TryNext(nil, 0); err != nil || len(got) != 0 {
		t.Fatalf("TryNext: %v %v", values(got), err)
	}
	select {
	case <-sub.Wake():
		t.Fatal("SetWatermark woke a subscription without WatermarkUpdates")
	default:
	}
}

// Seek forgets the reported watermark, so the current one is reported again.
func TestSubscription_WatermarkUpdatesSeek(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 8})
	sub := mustSubscribe(t, s, SubscribeOptions{WatermarkUpdates: true})
	s.SetWatermark(10)
	ctx := context.Background()
	if _, u, err := sub.Read(ctx, nil, 0); err != nil || u.State.Watermark != 10 || !u.WatermarkAdvanced {
		t.Fatalf("first: %+v %v", u, err)
	}
	if err := sub.Seek(StartNext()); err != nil {
		t.Fatal(err)
	}
	if _, u, err := sub.Read(ctx, nil, 0); err != nil || u.State.Watermark != 10 || !u.WatermarkAdvanced {
		t.Fatalf("after Seek: %+v %v", u, err)
	}
}

// S13-04: each watermark advance is reported once, alone or with records, and
// a drain loop over TryRead stops on a read that reports neither.
func TestUpdate_WatermarkAdvancedOnce(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 8})
	sub := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest(), WatermarkUpdates: true})
	idle := func(step string) {
		t.Helper()
		if got, u, err := sub.TryRead(nil, 0); err != nil || len(got) != 0 || u.Records != 0 || u.WatermarkAdvanced {
			t.Fatalf("%s: want idle, got %v %+v %v", step, values(got), u, err)
		}
	}
	idle("start")

	s.SetWatermark(5) // alone
	if _, u, err := sub.TryRead(nil, 0); err != nil || u.Records != 0 || !u.WatermarkAdvanced || u.State.Watermark != 5 {
		t.Fatalf("alone: %+v %v", u, err)
	}
	idle("after alone")

	mustAppend(t, s, 1, at(10)) // records without an advance
	if _, u, err := sub.TryRead(nil, 0); err != nil || u.Records != 1 || u.WatermarkAdvanced {
		t.Fatalf("records only: %+v %v", u, err)
	}
	idle("after records")

	mustAppend(t, s, 2, at(20)) // records with an advance
	s.SetWatermark(20)
	if _, u, err := sub.TryRead(nil, 0); err != nil || u.Records != 1 || !u.WatermarkAdvanced {
		t.Fatalf("records with advance: %+v %v", u, err)
	}
	idle("after records with advance")

	if err := sub.Seek(StartNext()); err != nil { // forgets what was reported
		t.Fatal(err)
	}
	if _, u, err := sub.TryRead(nil, 0); err != nil || u.Records != 0 || !u.WatermarkAdvanced || u.State.Watermark != 20 {
		t.Fatalf("after Seek: %+v %v", u, err)
	}
	idle("after Seek")

	// One wake-up for an advance and a close: the advance comes first, then
	// the close error.
	s.SetWatermark(30)
	s.Close()
	<-sub.Wake()
	if _, u, err := sub.TryRead(nil, 0); err != nil || !u.WatermarkAdvanced || u.State.Watermark != 30 {
		t.Fatalf("advance before close: %+v %v", u, err)
	}
	if _, u, err := sub.TryRead(nil, 0); err != ErrClosed || u != (Update{}) {
		t.Fatalf("close: %+v %v", u, err)
	}
}

// Without WatermarkUpdates, an empty read never reports an advance; the next
// records carry it.
func TestUpdate_WatermarkAdvancedWithoutUpdates(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 8})
	sub := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest()})
	s.SetWatermark(10)
	if _, u, err := sub.TryRead(nil, 0); err != nil || u.WatermarkAdvanced || u.State.Watermark != 10 {
		t.Fatalf("empty: %+v %v", u, err)
	}
	mustAppend(t, s, 1, at(20))
	if _, u, err := sub.TryRead(nil, 0); err != nil || u.Records != 1 || !u.WatermarkAdvanced {
		t.Fatalf("records: %+v %v", u, err)
	}
	mustAppend(t, s, 2, at(30))
	if _, u, err := sub.TryRead(nil, 0); err != nil || u.Records != 1 || u.WatermarkAdvanced {
		t.Fatalf("records again: %+v %v", u, err)
	}
}

// Records counts what this read appended, not len(dst).
func TestUpdate_RecordsAppended(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 8})
	sub := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest()})
	for v := range 3 {
		mustAppend(t, s, v, at(Timestamp(v+1)))
	}
	dst := make([]Entry[int], 2, 8)
	got, u, err := sub.TryRead(dst, 2)
	if err != nil || len(got) != 4 || u.Records != 2 {
		t.Fatalf("first: len %d %+v %v", len(got), u, err)
	}
	got, u, err = sub.Read(context.Background(), got, 0)
	if err != nil || len(got) != 5 || u.Records != 1 {
		t.Fatalf("second: len %d %+v %v", len(got), u, err)
	}
}

// The Update is zero on every error, from Read and TryRead alike.
func TestUpdate_ZeroOnError(t *testing.T) {
	ctx := context.Background()
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	cases := []struct {
		name  string
		setup func(t *testing.T) (*Subscription[int], context.Context)
	}{
		{"reset", func(t *testing.T) (*Subscription[int], context.Context) {
			s := mustNew(t, Config[int]{Capacity: 4})
			sub := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest()})
			mustAppend(t, s, 1, at(1))
			s.Clear()
			return sub, ctx
		}},
		{"lag", func(t *testing.T) (*Subscription[int], context.Context) {
			s := mustNew(t, Config[int]{Capacity: 2})
			sub := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest(), OnLag: LagFail})
			for v := range 4 {
				mustAppend(t, s, v, at(Timestamp(v+1)))
			}
			return sub, ctx
		}},
		{"later generation", func(t *testing.T) (*Subscription[int], context.Context) {
			s := mustNew(t, Config[int]{Capacity: 4})
			mustAppend(t, s, 1, at(1))
			return mustSubscribe(t, s, SubscribeOptions{Start: StartAt(Ref{Gen: 9, Seq: 1})}), ctx
		}},
		{"subscription closed", func(t *testing.T) (*Subscription[int], context.Context) {
			s := mustNew(t, Config[int]{Capacity: 4})
			sub := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest()})
			mustAppend(t, s, 1, at(1))
			sub.Close()
			return sub, ctx
		}},
		{"series closed", func(t *testing.T) (*Subscription[int], context.Context) {
			s := mustNew(t, Config[int]{Capacity: 4})
			sub := mustSubscribe(t, s, SubscribeOptions{WatermarkUpdates: true})
			s.CloseWithError(errFeed)
			return sub, ctx
		}},
		{"ctx done", func(t *testing.T) (*Subscription[int], context.Context) {
			s := mustNew(t, Config[int]{Capacity: 4})
			sub := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest()})
			mustAppend(t, s, 1, at(1))
			return sub, cancelled
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sub, ctx := c.setup(t)
			if _, u, err := sub.Read(ctx, nil, 0); err == nil || u != (Update{}) {
				t.Fatalf("Read: %+v %v", u, err)
			}
			if ctx.Err() != nil {
				return // TryRead takes no context
			}
			if _, u, err := sub.TryRead(nil, 0); err == nil || u != (Update{}) {
				t.Fatalf("TryRead: %+v %v", u, err)
			}
		})
	}
}
