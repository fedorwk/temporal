package temporal_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/fedorwk/temporal/temporal"
)

type Trade struct {
	ID    uint64
	Price int64
	Qty   int64
	Side  uint8
}

const ns = 1_760_000_000_000_000_000 // a base timestamp in Unix ns

// A raw trade series with idempotent ingestion and event-time queries.
func Example() {
	byID := temporal.NewUniqueIndex("trade_id",
		func(e *temporal.Entry[Trade]) (uint64, bool) { return e.Value.ID, true },
		temporal.ConflictDrop) // re-ingestion after a reconnect is a no-op

	trades, err := temporal.New(temporal.Config[Trade]{
		Name:      "btcusdt.trades",
		Capacity:  1 << 20,
		TimeIndex: temporal.TimeIndexBTree, // strategy code queries by event time
		Indexes:   []temporal.Index[Trade]{byID},
	})
	if err != nil {
		panic(err)
	}

	res, _ := trades.Append(Trade{ID: 7, Price: 104250, Qty: 3}, temporal.InputMeta{
		EventTime: ns + 100, ReceiveTime: ns + 150, SourceSeq: 7, SourceEpoch: 1,
	})
	fmt.Println(res.Outcome == temporal.OutcomeAppended, res.Ref)

	dup, _ := trades.Append(Trade{ID: 7, Price: 104250, Qty: 3}, temporal.InputMeta{EventTime: ns + 100})
	fmt.Println(dup.Outcome == temporal.OutcomeDropped, dup.Existing)

	trades.Append(Trade{ID: 8, Price: 104260, Qty: 1}, temporal.InputMeta{EventTime: ns + 90}) // late
	last, st := trades.AsOf(ns + 95)
	fmt.Println(st, last.Value.ID, last.Meta.Flags&temporal.FlagLate != 0)

	t, st := byID.Get(7)
	fmt.Println(st, t.Value.Price)
	// Output:
	// true {1 1}
	// true {1 1}
	// OK 8 true
	// OK 104250
}

// Every no-data answer says why.
func ExampleSeries_AsOf_statuses() {
	s, _ := temporal.New(temporal.Config[int]{Capacity: 2, TimeIndex: temporal.TimeIndexBTree})
	_, st := s.AsOf(ns)
	fmt.Println("empty:", st)

	for i := range 4 { // capacity 2: the first two are evicted
		s.Append(i, temporal.InputMeta{EventTime: ns + temporal.Timestamp(i)})
	}
	_, st = s.AsOf(ns + 1)
	fmt.Println("evicted:", st)

	relay, _ := temporal.New(temporal.Config[int]{Capacity: 2}) // TimeIndexNone
	_, st = relay.AsOf(ns)
	fmt.Println("no index:", st)
	// Output:
	// empty: NotFound
	// evicted: Expired
	// no index: TimeIndexDisabled
}

// Range reads are bounded; the opaque cursor continues them.
func ExampleSeries_Continue() {
	s, _ := temporal.New(temporal.Config[int]{Capacity: 100, TimeIndex: temporal.TimeIndexBTree})
	for i := range 10 {
		s.Append(i, temporal.InputMeta{EventTime: ns + temporal.Timestamp(i)})
	}
	buf := make([]temporal.Entry[int], 0, 16)
	buf, info := s.Range(buf[:0], ns+2, ns+8, 4)
	for !info.Next.IsZero() {
		buf, info = s.Continue(buf, info.Next, 4)
	}
	for _, e := range buf {
		fmt.Print(e.Value, " ")
	}
	fmt.Println()
	// Output:
	// 2 3 4 5 6 7
}

// A subscriber drains records in Seq order and reports lag deterministically.
func ExampleSubscription_Next() {
	s, _ := temporal.New(temporal.Config[int]{Capacity: 4})
	sub, _ := s.Subscribe(temporal.SubscribeOptions{Start: temporal.StartOldest()})
	defer sub.Close()

	for i := range 3 {
		s.Append(i, temporal.InputMeta{EventTime: ns + temporal.Timestamp(i)})
	}
	batch, err := sub.Next(context.Background(), nil, 0)
	fmt.Println(len(batch), err)

	for i := range 10 { // overruns retention while the subscriber is idle
		s.Append(i, temporal.InputMeta{EventTime: ns + temporal.Timestamp(i)})
	}
	_, err = sub.Next(context.Background(), nil, 0)
	var lag *temporal.LagError
	if errors.As(err, &lag) {
		fmt.Println("lagged, missed", lag.Missed)
		sub.Seek(temporal.StartOldest()) // resynchronize
	}
	batch, err = sub.Next(context.Background(), nil, 0)
	fmt.Println(len(batch), err)
	// Output:
	// 3 <nil>
	// lagged, missed 6
	// 4 <nil>
}

// deriveNotional is a complete derive loop: it maps each trade to its notional
// value, appends each batch's outputs atomically, advances a checkpoint only
// after the append succeeded, and forwards the source watermark once it has
// caught up. It runs on the caller's goroutine, so shutdown, retries and panics
// follow the caller's rules.
func deriveNotional(
	ctx context.Context,
	src *temporal.Series[Trade],
	dst *temporal.Series[int64],
	from temporal.StartFrom,
	checkpoint *temporal.Cursor,
) error {
	sub, err := src.Subscribe(temporal.SubscribeOptions{Start: from})
	if err != nil {
		return err
	}
	defer sub.Close()
	in := make([]temporal.Entry[Trade], 0, 256)
	out := make([]temporal.Input[int64], 0, 256)
	results := make([]temporal.AppendResult, 0, 256)
	for {
		var state temporal.SourceState
		in, state, err = sub.NextState(ctx, in[:0], 256)
		if err != nil {
			// ctx done, ErrClosed, ErrReset (source cleared) or *LagError
			// (fell behind retention): stop, then rebuild from the checkpoint.
			return err
		}
		out = out[:0]
		for _, e := range in {
			out = append(out, temporal.Input[int64]{
				Value: e.Value.Price * e.Value.Qty,
				Meta:  temporal.InputMeta{EventTime: e.Meta.EventTime, SourceSeq: uint64(e.Meta.Ref.Seq)},
			})
		}
		if results, err = dst.AppendBatch(results[:0], out); err != nil {
			return err // the checkpoint still points at this batch
		}
		last := in[len(in)-1].Meta.Ref
		*checkpoint = temporal.Cursor{Gen: last.Gen, Next: last.Seq + 1} // persist externally if needed
		if last.Seq == state.Newest.Seq {
			// Caught up: every source record up to the watermark is processed.
			dst.SetWatermark(state.Watermark)
		}
	}
}

// A derived series maintained by a loop the caller owns, using NextState.
func ExampleSubscription_NextState() {
	raw, _ := temporal.New(temporal.Config[Trade]{Capacity: 1024})
	notional, _ := temporal.New(temporal.Config[int64]{Capacity: 1024, TimeIndex: temporal.TimeIndexBTree})

	ctx, cancel := context.WithCancel(context.Background())
	var checkpoint temporal.Cursor
	done := make(chan error, 1)
	go func() { done <- deriveNotional(ctx, raw, notional, temporal.StartOldest(), &checkpoint) }()

	raw.Append(Trade{ID: 1, Price: 100, Qty: 2}, temporal.InputMeta{EventTime: ns + 1})
	raw.Append(Trade{ID: 2, Price: 101, Qty: 3}, temporal.InputMeta{EventTime: ns + 2})
	raw.SetWatermark(ns + 2)
	raw.Append(Trade{ID: 3, Price: 102, Qty: 1}, temporal.InputMeta{EventTime: ns + 3})

	for notional.Len() < 3 || notional.Watermark() != ns+2 { // wait for the loop (example only)
		time.Sleep(time.Millisecond)
	}
	cancel()
	err := <-done

	e, _ := notional.AsOf(ns + 2)
	fmt.Println(e.Value, checkpoint, err)
	// Output:
	// 303 {1 4} context canceled
}
