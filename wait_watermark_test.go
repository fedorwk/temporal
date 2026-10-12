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
