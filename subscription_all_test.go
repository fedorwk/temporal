package temporal

import (
	"context"
	"errors"
	"slices"
	"testing"
)

// All yields every record across batches and ends silently after a clean
// close of a drained Series.
func TestSubscriptionAll_CleanClose(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 16, MaxReadBatch: 3})
	for v := 1; v <= 10; v++ {
		mustAppend(t, s, v, at(Timestamp(v)))
	}
	s.Close()
	sub := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest()})

	var got []int
	for e, err := range sub.All(context.Background(), 0) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, e.Value)
	}
	if !slices.Equal(got, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}) {
		t.Fatalf("got %v", got)
	}
}

// A close with a cause and a cancelled ctx are yielded once as errors.
func TestSubscriptionAll_Errors(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 16})
	mustAppend(t, s, 1, at(1))
	s.CloseWithError(errFeed)
	sub := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest()})
	var errs []error
	n := 0
	for _, err := range sub.All(context.Background(), 0) {
		if err != nil {
			errs = append(errs, err)
			continue
		}
		n++
	}
	if n != 1 || len(errs) != 1 || !errors.Is(errs[0], errFeed) {
		t.Fatalf("records %d, errors %v", n, errs)
	}

	open := mustNew(t, Config[int]{Capacity: 16})
	sub = mustSubscribe(t, open, SubscribeOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, err := range sub.All(ctx, 0) {
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v", err)
		}
	}
}

// Breaking out stops reading; the subscription continues where it stopped.
func TestSubscriptionAll_Break(t *testing.T) {
	s := mustNew(t, Config[int]{Capacity: 16})
	for v := 1; v <= 5; v++ {
		mustAppend(t, s, v, at(Timestamp(v)))
	}
	sub := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest()})
	for e, err := range sub.All(context.Background(), 2) {
		if err != nil || e.Value == 1 { // mid-batch: 2 was read but not yielded
			break
		}
	}
	got, err := sub.TryNext(nil, 0)
	if err != nil || !slices.Equal(values(got), []int{2, 3, 4, 5}) {
		t.Fatalf("after break: %v %v", values(got), err)
	}
}
