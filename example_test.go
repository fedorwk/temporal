package temporal_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/fedorwk/temporal"
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

// A feed that arrives in event-time order gets every event-time query at
// almost no ingest cost; an out-of-order record is rejected.
func Example_orderedFeed() {
	quotes, err := temporal.New(temporal.Config[int64]{
		Capacity:   1 << 16,
		TimeIndex:  temporal.TimeIndexOrdered,
		LatePolicy: temporal.LateReject, // required by TimeIndexOrdered
	})
	if err != nil {
		panic(err)
	}
	quotes.Append(104250, temporal.InputMeta{EventTime: ns + 10})
	quotes.Append(104255, temporal.InputMeta{EventTime: ns + 20})
	quotes.Append(104260, temporal.InputMeta{EventTime: ns + 20}) // equal times are fine

	_, err = quotes.Append(104240, temporal.InputMeta{EventTime: ns + 15})
	fmt.Println(errors.Is(err, temporal.ErrLate))

	q, st := quotes.AsOf(ns + 25)
	fmt.Println(st, q.Value)
	q, st = quotes.Before(ns + 20)
	fmt.Println(st, q.Value)
	// Output:
	// true
	// OK 104260
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

// A non-blocking reader drains a subscription after each wake-up until a read
// finds nothing new. One wake-up can stand for several changes, here a
// watermark advance and a close, which are reported one read at a time.
func ExampleSubscription_TryRead() {
	s, _ := temporal.New(temporal.Config[int]{Capacity: 8})
	sub, _ := s.Subscribe(temporal.SubscribeOptions{Start: temporal.StartOldest(), WatermarkUpdates: true})
	defer sub.Close()

	var buf []temporal.Entry[int]
	drain := func() {
		<-sub.Wake() // in a real loop: one case of a select over several sources
		for {
			var u temporal.Update
			var err error
			buf, u, err = sub.TryRead(buf[:0], 0)
			switch {
			case err != nil:
				fmt.Println("stop:", err)
				return
			case u.Records == 0 && !u.WatermarkAdvanced:
				fmt.Println("idle: wait on Wake")
				return
			}
			if u.WatermarkAdvanced {
				fmt.Printf("records=%d watermark=+%d\n", u.Records, u.State.Watermark-ns)
			} else {
				fmt.Printf("records=%d\n", u.Records)
			}
		}
	}

	s.Append(1, temporal.InputMeta{EventTime: ns + 1})
	s.Append(2, temporal.InputMeta{EventTime: ns + 2})
	drain()

	s.SetWatermark(ns + 2)
	s.Close()
	drain()
	// Output:
	// records=2
	// idle: wait on Wake
	// records=0 watermark=+2
	// stop: temporal: series closed
}

// deriveNotional is a complete derive loop: it maps each trade to its notional
// value, appends each batch's outputs atomically, advances a checkpoint only
// after the append succeeded, and forwards the source watermark once it has
// caught up, including a watermark set after the last record
// (WatermarkUpdates). It runs on the caller's goroutine, so shutdown, retries and panics
// follow the caller's rules.
func deriveNotional(
	ctx context.Context,
	src *temporal.Series[Trade],
	dst *temporal.Series[int64],
	from temporal.StartFrom,
	checkpoint *temporal.Cursor,
) error {
	sub, err := src.Subscribe(temporal.SubscribeOptions{Start: from, WatermarkUpdates: true})
	if err != nil {
		return err
	}
	defer sub.Close()
	in := make([]temporal.Entry[Trade], 0, 256)
	out := make([]temporal.Input[int64], 0, 256)
	results := make([]temporal.AppendResult, 0, 256)
	for {
		var u temporal.Update
		in, u, err = sub.Read(ctx, in[:0], 256)
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
		if len(in) > 0 {
			if results, err = dst.AppendBatch(results[:0], out); err != nil {
				return err // the checkpoint still points at this batch
			}
			last := in[len(in)-1].Meta.Ref
			*checkpoint = temporal.Cursor{Gen: last.Gen, Next: last.Seq + 1} // persist externally if needed
		}
		if checkpoint.Next == u.State.Newest.Seq+1 {
			// Caught up: every source record up to the watermark is processed.
			dst.SetWatermark(u.State.Watermark)
		}
	}
}

// A derived series maintained by a loop the caller owns, using Read.
func ExampleSubscription_Read() {
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
