package temporal

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"
)

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitDone(t *testing.T, d *Deriver) {
	t.Helper()
	select {
	case <-d.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("deriver did not stop")
	}
}

// double maps each source record to one output with twice the value.
func double(batch []Entry[int], out []Input[int]) ([]Input[int], error) {
	for _, e := range batch {
		out = append(out, Input[int]{Value: 2 * e.Value, Meta: InputMeta{
			EventTime:   e.Meta.EventTime,
			SourceSeq:   uint64(e.Meta.Ref.Seq),
			SourceEpoch: SourceEpoch(e.Meta.Ref.Gen),
		}})
	}
	return out, nil
}

// D-001, D-003, D-007, D-008: a running pipeline in every mode combination.
func TestDerived_Pipeline(t *testing.T) {
	for _, srcMode := range allModes {
		for _, dstMode := range allModes {
			t.Run(fmt.Sprintf("%d->%d", srcMode, dstMode), func(t *testing.T) {
				src := mustNew(t, Config[int]{Capacity: 64, TimeIndex: srcMode})
				dst := mustNew(t, Config[int]{Capacity: 64, TimeIndex: dstMode})
				for i := 1; i <= 10; i++ {
					mustAppend(t, src, i, at(Timestamp(100+i)))
				}
				d, err := StartDerived(context.Background(), src, dst, double, DeriveOptions{Name: "x2", Version: "v1", BatchSize: 3})
				if err != nil {
					t.Fatal(err)
				}
				defer d.Stop()
				waitFor(t, "10 outputs", func() bool { return dst.Len() == 10 })
				for i := 11; i <= 15; i++ {
					mustAppend(t, src, i, at(Timestamp(100+i)))
				}
				waitFor(t, "15 outputs", func() bool { return d.Checkpoint() == (Cursor{1, 16}) })
				got, _ := dst.ReadFrom(nil, Ref{}, 0)
				for i, e := range got {
					if e.Value != 2*(i+1) || e.Meta.SourceSeq != uint64(i+1) || e.Meta.EventTime != Timestamp(101+i) {
						t.Fatalf("output %d = %+v", i, e)
					}
				}
				st := d.Status()
				if st.Name != "x2" || st.Version != "v1" || st.State != DeriverRunning || st.LagRecords != 0 ||
					st.LagTime != 0 || st.LastErr != nil {
					t.Fatalf("status %+v", st)
				}
				if dstMode == TimeIndexBTree {
					if e, st := dst.AsOf(105); st != StatusOK || e.Value != 10 {
						t.Fatalf("dst AsOf = %+v %v", e, st)
					}
				}
			})
		}
	}
}

// D-004, T-020: every failure path stops the worker with the error recorded and
// the processed cursor at the last committed batch.
func TestDerived_FailuresKeepCursor(t *testing.T) {
	type scenario struct {
		name    string
		setup   func(src, dst *Series[int]) DeriveFunc[int, int]
		trigger func(t *testing.T, src *Series[int])
		opts    DeriveOptions
		wantErr error
	}
	errDerive := errors.New("derive failed")
	failOn := func(v int) DeriveFunc[int, int] {
		return func(batch []Entry[int], out []Input[int]) ([]Input[int], error) {
			for _, e := range batch {
				if e.Value == v {
					return out, errDerive
				}
			}
			return double(batch, out)
		}
	}
	cases := []scenario{
		{
			name:    "derive error",
			setup:   func(_, _ *Series[int]) DeriveFunc[int, int] { return failOn(6) },
			wantErr: errDerive,
		},
		{
			name: "derive panic",
			setup: func(_, _ *Series[int]) DeriveFunc[int, int] {
				return func(batch []Entry[int], out []Input[int]) ([]Input[int], error) {
					if batch[0].Value == 6 {
						panic("boom")
					}
					return double(batch, out)
				}
			},
			wantErr: ErrCallbackPanic,
		},
		{
			name: "unique conflict on append",
			setup: func(_, dst *Series[int]) DeriveFunc[int, int] {
				return func(batch []Entry[int], out []Input[int]) ([]Input[int], error) {
					for _, e := range batch {
						v := e.Value
						if v == 6 {
							v = 1 // collides with the first output's key
						}
						out = append(out, Input[int]{Value: v, Meta: at(e.Meta.EventTime)})
					}
					return out, nil
				}
			},
			wantErr: ErrUniqueConflict,
		},
		{
			name: "batch too large",
			setup: func(_, _ *Series[int]) DeriveFunc[int, int] {
				return func(batch []Entry[int], out []Input[int]) ([]Input[int], error) {
					if batch[0].Value == 6 {
						for range 100 {
							out = append(out, Input[int]{Value: 0, Meta: at(1)})
						}
						return out, nil
					}
					return double(batch, out)
				}
			},
			wantErr: ErrBatchTooLarge,
		},
		{
			name:  "source lag under LagFail",
			setup: func(_, _ *Series[int]) DeriveFunc[int, int] { return double },
			trigger: func(t *testing.T, src *Series[int]) {
				for i := 100; i < 140; i++ { // overruns capacity 16 while the worker is blocked
					mustAppend(t, src, i, at(Timestamp(i)))
				}
			},
			wantErr: ErrLagged,
		},
		{
			name:  "source Clear",
			setup: func(_, _ *Series[int]) DeriveFunc[int, int] { return double },
			trigger: func(t *testing.T, src *Series[int]) {
				src.Clear()
				mustAppend(t, src, 1, at(1))
			},
			wantErr: ErrReset,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src := mustNew(t, Config[int]{Capacity: 16})
			uniq := NewUniqueIndex("v", func(e *Entry[int]) (int, bool) { return e.Value, true }, ConflictReject)
			dst := mustNew(t, Config[int]{Capacity: 32, Indexes: []Index[int]{uniq}})
			for i := 1; i <= 5; i++ {
				mustAppend(t, src, i, at(Timestamp(i)))
			}
			fn := c.setup(src, dst)
			gate := make(chan struct{})
			entered := make(chan struct{}, 1)
			blocking := func(batch []Entry[int], out []Input[int]) ([]Input[int], error) {
				if batch[0].Value >= 6 {
					select {
					case entered <- struct{}{}:
					default:
					}
					<-gate // hold the worker inside fn while the trigger runs
				}
				return fn(batch, out)
			}
			opts := c.opts
			opts.BatchSize = 5
			d, err := StartDerived(context.Background(), src, dst, blocking, opts)
			if err != nil {
				t.Fatal(err)
			}
			waitFor(t, "first batch", func() bool { return d.Checkpoint() == (Cursor{1, 6}) })
			before := dst.Stats()
			want := Cursor{1, 6}
			if c.trigger != nil {
				// The worker commits the batch holding record 6, then its next
				// read hits the lag or reset.
				mustAppend(t, src, 6, at(6))
				<-entered
				c.trigger(t, src)
				want = Cursor{1, 7}
			} else {
				for i := 6; i <= 8; i++ {
					mustAppend(t, src, i, at(Timestamp(i)))
				}
			}
			close(gate)
			waitDone(t, d)
			st := d.Status()
			if st.State != DeriverFailed || !errors.Is(st.LastErr, c.wantErr) || !errors.Is(d.Err(), c.wantErr) {
				t.Fatalf("status %+v, want Failed with %v", st, c.wantErr)
			}
			if d.Checkpoint() != want {
				t.Fatalf("processed cursor %+v, want %+v", d.Checkpoint(), want)
			}
			after := dst.Stats()
			if c.trigger == nil {
				after.Rejected, before.Rejected = 0, 0
				after.Indexes[0].Conflicts, before.Indexes[0].Conflicts = 0, 0
				if !statsEqual(before, after) {
					t.Fatalf("destination changed: %+v vs %+v", after, before)
				}
			} else if after.Len != before.Len+1 {
				t.Fatalf("destination len %d, want %d", after.Len, before.Len+1)
			}
			if src.Stats().Subscribers != 0 {
				t.Fatal("worker did not close its subscription")
			}
		})
	}
}

// §18.4, T-020: the destination watermark follows the source only when a batch
// reached the source's newest record; WatermarkManual never touches it.
func TestDerived_WatermarkPropagation(t *testing.T) {
	src := mustNew(t, Config[int]{Capacity: 64})
	for i := 1; i <= 6; i++ {
		mustAppend(t, src, i, at(Timestamp(i)))
	}
	src.SetWatermark(4)
	dst := mustNew(t, Config[int]{Capacity: 64})
	manual := mustNew(t, Config[int]{Capacity: 64})

	release := make(chan struct{}, 10)
	var seenFirst bool
	gated := func(batch []Entry[int], out []Input[int]) ([]Input[int], error) {
		if !seenFirst {
			seenFirst = true
		} else {
			<-release
		}
		return double(batch, out)
	}
	d, err := StartDerived(context.Background(), src, dst, gated, DeriveOptions{BatchSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Stop()
	defer close(release) // runs first: a gated worker can exit if the test fails early
	m, err := StartDerived(context.Background(), src, manual, double, DeriveOptions{Watermark: WatermarkManual})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	waitFor(t, "first batch", func() bool { return d.Checkpoint().Next == 4 })
	if dst.Watermark() != MinTimestamp {
		t.Fatalf("propagated before catching up: %d", dst.Watermark())
	}
	release <- struct{}{}
	waitFor(t, "caught up", func() bool { return dst.Watermark() == 4 })
	src.SetWatermark(10)
	mustAppend(t, src, 7, at(7))
	release <- struct{}{}
	waitFor(t, "second propagation", func() bool { return dst.Watermark() == 10 })
	waitFor(t, "manual caught up", func() bool { return m.Checkpoint().Next == 8 })
	if manual.Watermark() != MinTimestamp {
		t.Fatal("WatermarkManual changed the destination watermark")
	}
}

// D-006: two derivers over the same source produce identical destinations;
// D-005: a checkpoint resumes exactly where the first stopped.
func TestDerived_DeterministicAndCheckpoint(t *testing.T) {
	src := mustNew(t, Config[int]{Capacity: 256, TimeIndex: TimeIndexBTree})
	running := 0
	sum := func(batch []Entry[int], out []Input[int]) ([]Input[int], error) {
		for _, e := range batch {
			running += e.Value
			out = append(out, Input[int]{Value: running, Meta: at(e.Meta.EventTime)})
		}
		return out, nil
	}
	r, _ := newRand(t)
	for i := 1; i <= 100; i++ {
		mustAppend(t, src, r.IntN(1000), at(Timestamp(r.IntN(50)+1)))
	}
	a := mustNew(t, Config[int]{Capacity: 256})
	b := mustNew(t, Config[int]{Capacity: 256, TimeIndex: TimeIndexBTree})
	da, _ := StartDerived(context.Background(), src, a, double, DeriveOptions{BatchSize: 7})
	db, _ := StartDerived(context.Background(), src, b, double, DeriveOptions{BatchSize: 13})
	waitFor(t, "both done", func() bool { return a.Len() == 100 && b.Len() == 100 })
	da.Stop()
	db.Stop()
	ea, _ := a.ReadFrom(nil, Ref{}, 0)
	eb, _ := b.ReadFrom(nil, Ref{}, 0)
	if !slices.Equal(ea, eb) {
		t.Fatal("destinations differ")
	}
	if da.Status().State != DeriverStopped || da.Err() != nil {
		t.Fatalf("after Stop: %+v", da.Status())
	}

	// Checkpoint/resume with a stateful function.
	first := mustNew(t, Config[int]{Capacity: 256})
	d1, _ := StartDerived(context.Background(), src, first, sum, DeriveOptions{BatchSize: 10})
	waitFor(t, "first 100", func() bool { return d1.Checkpoint().Next == 101 })
	d1.Stop()
	cp := d1.Checkpoint()
	for i := 101; i <= 120; i++ {
		mustAppend(t, src, i, at(Timestamp(i)))
	}
	d2, _ := StartDerived(context.Background(), src, first, sum, DeriveOptions{Start: StartAt(Ref{cp.Gen, cp.Next})})
	waitFor(t, "resumed", func() bool { return d2.Checkpoint().Next == 121 })
	d2.Stop()
	all, _ := first.ReadFrom(nil, Ref{}, 0)
	srcAll, _ := src.ReadFrom(nil, Ref{}, 0)
	total := 0
	for i, e := range srcAll {
		total += e.Value
		if all[i].Value != total {
			t.Fatalf("output %d = %d, want running sum %d", i, all[i].Value, total)
		}
	}
}

// D-007: lag metrics; source Close drains and stops the worker.
func TestDerived_LagAndSourceClose(t *testing.T) {
	src := mustNew(t, Config[int]{Capacity: 64})
	dst := mustNew(t, Config[int]{Capacity: 64})
	gate := make(chan struct{})
	slow := func(batch []Entry[int], out []Input[int]) ([]Input[int], error) {
		<-gate
		return double(batch, out)
	}
	mustAppend(t, src, 1, at(1000))
	for i := 2; i <= 5; i++ {
		mustAppend(t, src, i, at(Timestamp(1000+i*10)))
	}
	// Lag is measured against the snapshot taken with each read, so all five
	// records exist before the first read (of Seq 1 only, BatchSize 1).
	d, _ := StartDerived(context.Background(), src, dst, slow, DeriveOptions{BatchSize: 1})
	gate <- struct{}{}
	waitFor(t, "first batch", func() bool { return d.Checkpoint().Next == 2 })
	st := d.Status()
	if st.LagRecords != 4 || st.LagTime != 50 {
		t.Fatalf("lag %d records %v; want 4 and 50ns", st.LagRecords, st.LagTime)
	}
	close(gate)
	waitFor(t, "caught up", func() bool { return d.Status().LagRecords == 0 && dst.Len() == 5 })
	src.Close()
	waitDone(t, d)
	if st := d.Status(); st.State != DeriverStopped || !errors.Is(d.Err(), ErrClosed) {
		t.Fatalf("after source Close: %+v", st)
	}
}

// StartDerived validates its arguments; cancelling the parent ctx stops it.
func TestDerived_ValidationAndCancel(t *testing.T) {
	src := mustNew(t, Config[int]{Capacity: 4})
	dst := mustNew(t, Config[int]{Capacity: 4})
	bad := []struct {
		name string
		call func() error
	}{
		{"nil fn", func() error {
			_, err := StartDerived[int, int](context.Background(), src, dst, nil, DeriveOptions{})
			return err
		}},
		{"nil src", func() error {
			_, err := StartDerived(context.Background(), nil, dst, double, DeriveOptions{})
			return err
		}},
		{"self", func() error {
			_, err := StartDerived(context.Background(), src, src, double, DeriveOptions{})
			return err
		}},
		{"bad lag", func() error {
			_, err := StartDerived(context.Background(), src, dst, double, DeriveOptions{OnLag: 9})
			return err
		}},
		{"bad watermark", func() error {
			_, err := StartDerived(context.Background(), src, dst, double, DeriveOptions{Watermark: 9})
			return err
		}},
		{"negative batch", func() error {
			_, err := StartDerived(context.Background(), src, dst, double, DeriveOptions{BatchSize: -1})
			return err
		}},
	}
	for _, b := range bad {
		if err := b.call(); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: %v", b.name, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	d, err := StartDerived(ctx, src, dst, double, DeriveOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	waitDone(t, d)
	if d.Status().State != DeriverStopped || d.Err() != nil {
		t.Fatalf("after cancel: %+v", d.Status())
	}
	if DeriverFailed.String() != "Failed" || DeriverState(9).String() != "DeriverState(9)" {
		t.Fatal("DeriverState.String")
	}
}
