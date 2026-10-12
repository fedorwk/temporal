package temporal

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

var errFeed = errors.New("feed disconnected")

// CloseWithError: writes and drained subscriptions report a *ClosedError that
// matches both ErrClosed and the cause; reads keep working.
func TestCloseWithError_Semantics(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 4})
	mustAppend(t, s, 1, at(1))
	sub := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest()})
	if err := s.Err(); err != nil {
		t.Fatalf("Err before close = %v", err)
	}
	if err := s.CloseWithError(errFeed); err != nil {
		t.Fatal(err)
	}
	if err := s.Err(); err != errFeed {
		t.Fatalf("Err = %v", err)
	}

	check := func(what string, err error) {
		t.Helper()
		var ce *ClosedError
		if !errors.Is(err, ErrClosed) || !errors.Is(err, errFeed) || !errors.As(err, &ce) || ce.Cause != errFeed {
			t.Fatalf("%s = %v", what, err)
		}
	}
	_, err := s.Append(2, at(2))
	check("Append", err)
	_, err = s.AppendBatch(nil, []Input[int]{{Value: 2, Meta: at(2)}})
	check("AppendBatch", err)
	check("Clear", s.Clear())

	got, err := sub.Next(context.Background(), nil, 0)
	if err != nil || !slices.Equal(values(got), []int{1}) {
		t.Fatalf("drain: %v %v", values(got), err)
	}
	_, err = sub.Next(context.Background(), nil, 0)
	check("Next after drain", err)
	_, err = sub.TryNext(nil, 0)
	check("TryNext after drain", err)

	if e, st := s.Latest(); st != StatusOK || e.Value != 1 {
		t.Fatalf("Latest after close = %+v, %v", e, st)
	}
	mustInvariants(t, s)
}

// Only the first close takes effect, and a clean close reports ErrClosed itself.
func TestCloseWithError_FirstCloseWins(t *testing.T) {
	t.Run("clean then cause", func(t *testing.T) {
		s := mustNew(t, Config[int]{Capacity: 4})
		s.Close()
		s.CloseWithError(errFeed)
		if err := s.Err(); err != nil {
			t.Fatalf("Err = %v", err)
		}
		if _, err := s.Append(1, at(1)); err != ErrClosed {
			t.Fatalf("Append = %v", err)
		}
	})
	t.Run("cause then cause", func(t *testing.T) {
		s := mustNew(t, Config[int]{Capacity: 4})
		s.CloseWithError(errFeed)
		s.CloseWithError(errors.New("other"))
		if err := s.Err(); err != errFeed {
			t.Fatalf("Err = %v", err)
		}
	})
	t.Run("nil cause", func(t *testing.T) {
		s := mustNew(t, Config[int]{Capacity: 4})
		s.CloseWithError(nil)
		if _, err := s.Append(1, at(1)); err != ErrClosed {
			t.Fatalf("Append = %v", err)
		}
	})
}

// A Next blocked on an empty Series is woken by CloseWithError and returns
// the cause; closing the subscription itself still reports plain ErrClosed.
func TestCloseWithError_WakesSubscriber(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 4})
	sub := mustSubscribe(t, s, SubscribeOptions{})
	ch := make(chan error, 1)
	go func() {
		_, err := sub.Next(context.Background(), nil, 0)
		ch <- err
	}()
	time.Sleep(10 * time.Millisecond)
	s.CloseWithError(errFeed)
	select {
	case err := <-ch:
		if !errors.Is(err, ErrClosed) || !errors.Is(err, errFeed) {
			t.Fatalf("Next = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Next did not unblock")
	}

	sub.Close()
	if _, err := sub.TryNext(nil, 0); err != ErrClosed {
		t.Fatalf("TryNext after Subscription.Close = %v", err)
	}
}

func TestClosedError_Message(t *testing.T) {
	err := &ClosedError{Cause: errFeed}
	if got, want := err.Error(), "temporal: series closed: feed disconnected"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}
