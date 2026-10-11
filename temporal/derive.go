package temporal

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"
)

// DeriveFunc turns one batch of source entries into outputs, appended to out.
// It runs outside all Series locks and may read other series. It owns its
// algorithm state; after an error the Deriver stops (D-004).
type DeriveFunc[In, Out any] func(batch []Entry[In], out []Input[Out]) ([]Input[Out], error)

// WatermarkMode selects how a Deriver maintains the destination watermark.
type WatermarkMode uint8

const (
	// WatermarkPropagate sets the destination watermark to the source
	// watermark after a batch that reached the source's newest record (§18.4).
	WatermarkPropagate WatermarkMode = iota
	// WatermarkManual leaves the destination watermark to the derive function's
	// owner.
	WatermarkManual
)

func (m WatermarkMode) valid() bool { return m <= WatermarkManual }

// DeriveOptions configures StartDerived.
type DeriveOptions struct {
	Name      string
	Version   string    // derivation algorithm/config version, recorded in status
	Start     StartFrom // zero value = StartOldest
	BatchSize int       // 0 = the source's MaxReadBatch
	OnLag     LagPolicy // zero value = LagFail
	Watermark WatermarkMode
}

// DeriverState is the lifecycle state of a Deriver.
type DeriverState uint8

const (
	DeriverRunning DeriverState = iota
	DeriverStopped              // ctx cancelled, Stop called, or the source closed and drained
	DeriverFailed               // a derive, append, lag or reset error; see LastErr
)

func (s DeriverState) String() string {
	switch s {
	case DeriverRunning:
		return "Running"
	case DeriverStopped:
		return "Stopped"
	case DeriverFailed:
		return "Failed"
	}
	return fmt.Sprintf("DeriverState(%d)", uint8(s))
}

// DeriverStatus is a snapshot of a Deriver for monitoring (D-007).
type DeriverStatus struct {
	Name, Version string
	State         DeriverState
	Processed     Cursor        // next source Seq whose outputs are not yet committed
	LagRecords    uint64        // source Newest - last processed Seq, at the last read
	LagTime       time.Duration // source MaxObservedEventTime - last processed EventTime
	LastErr       error
}

// Deriver consumes a source Series and appends derived records to a
// destination Series on its own goroutine.
type Deriver struct {
	mu     sync.Mutex // guards status; outside every Series consistency domain
	status DeriverStatus
	cancel context.CancelFunc
	done   chan struct{}
}

// Status returns the current status.
func (d *Deriver) Status() DeriverStatus {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.status
}

// Checkpoint returns the processed cursor: every source record before
// Checkpoint().Next has had its outputs committed. It is in-memory only (D-005);
// persist it externally and resume with StartAt(Ref{Gen, Next}).
func (d *Deriver) Checkpoint() Cursor {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.status.Processed
}

// Stop cancels the worker and waits for it to exit.
func (d *Deriver) Stop() {
	d.cancel()
	<-d.done
}

// Done is closed when the worker has exited.
func (d *Deriver) Done() <-chan struct{} { return d.done }

// Err returns the error that ended the worker: nil while running or after
// Stop/ctx cancellation, ErrClosed after the source closed, otherwise the
// failure.
func (d *Deriver) Err() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.status.State == DeriverRunning {
		return nil
	}
	return d.status.LastErr
}

func (d *Deriver) update(f func(*DeriverStatus)) {
	d.mu.Lock()
	f(&d.status)
	d.mu.Unlock()
}

// StartDerived starts a worker that reads src in Seq order, calls fn on each
// batch outside all locks, and appends the outputs of a batch to dst with one
// AppendBatch. The processed cursor advances only after that append succeeds.
// The worker stops on ctx cancellation, Stop, or the first error.
func StartDerived[In, Out any](
	ctx context.Context,
	src *Series[In],
	dst *Series[Out],
	fn DeriveFunc[In, Out],
	opts DeriveOptions,
) (*Deriver, error) {
	switch {
	case src == nil || dst == nil || fn == nil:
		return nil, fmt.Errorf("%w: StartDerived needs a source, a destination and a function", ErrInvalidConfig)
	case any(src) == any(dst):
		return nil, fmt.Errorf("%w: a series cannot derive into itself", ErrInvalidConfig)
	case !opts.OnLag.valid() || !opts.Watermark.valid() || opts.BatchSize < 0 || opts.Start.kind > startAt:
		return nil, fmt.Errorf("%w: invalid DeriveOptions", ErrInvalidConfig)
	}
	sub, err := src.Subscribe(SubscribeOptions{
		Name:  opts.Name,
		Start: opts.Start.orDefault(StartOldest()),
		OnLag: opts.OnLag,
	})
	if err != nil {
		return nil, err
	}
	wctx, cancel := context.WithCancel(ctx)
	d := &Deriver{
		cancel: cancel,
		done:   make(chan struct{}),
		status: DeriverStatus{Name: opts.Name, Version: opts.Version, Processed: sub.Cursor()},
	}
	w := &deriveWorker[In, Out]{d: d, sub: sub, dst: dst, fn: fn, opts: opts, limit: opts.BatchSize}
	go w.run(wctx)
	return d, nil
}

type deriveWorker[In, Out any] struct {
	d     *Deriver
	sub   *Subscription[In]
	dst   *Series[Out]
	fn    DeriveFunc[In, Out]
	opts  DeriveOptions
	limit int // 0 = source MaxReadBatch
}

func (w *deriveWorker[In, Out]) run(ctx context.Context) {
	defer close(w.d.done)
	defer w.sub.Close()
	defer w.d.cancel()

	var in []Entry[In]
	var out []Input[Out]
	var results []AppendResult
	for {
		var snap sourceSnapshot
		var err error
		in, snap, err = w.sub.next(ctx, in[:0], w.limit)
		if err != nil {
			w.stop(ctx, err)
			return
		}
		out, err = w.derive(in, out[:0])
		if err == nil && len(out) > 0 {
			results, err = w.dst.AppendBatch(results[:0], out)
		}
		clear(out) // release references to derived values
		if err != nil {
			w.fail(err)
			return
		}

		last := in[len(in)-1]
		if w.opts.Watermark == WatermarkPropagate && last.Meta.Ref.Seq == snap.newest {
			w.dst.SetWatermark(snap.watermark)
		}
		processed := Cursor{Gen: last.Meta.Ref.Gen, Next: last.Meta.Ref.Seq + 1}
		lagRecords := uint64(snap.newest - last.Meta.Ref.Seq)
		lagTime := time.Duration(0)
		if snap.maxObserved != MinTimestamp {
			lagTime = time.Duration(snap.maxObserved - last.Meta.EventTime)
		}
		w.d.update(func(st *DeriverStatus) {
			st.Processed, st.LagRecords, st.LagTime = processed, lagRecords, lagTime
		})
		clear(in) // release references to source values
	}
}

// derive calls the derive function, converting a panic into an error.
func (w *deriveWorker[In, Out]) derive(in []Entry[In], out []Input[Out]) (res []Input[Out], err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &CallbackPanicError{Callback: "derive:" + w.opts.Name, Value: r, Stack: debug.Stack()}
		}
	}()
	return w.fn(in, out)
}

// stop ends the worker after a read error: cancellation and a drained closed
// source stop it; reset, lag and other errors fail it.
func (w *deriveWorker[In, Out]) stop(ctx context.Context, err error) {
	switch {
	case ctx.Err() != nil:
		w.d.update(func(st *DeriverStatus) { st.State = DeriverStopped })
	case errors.Is(err, ErrClosed):
		w.d.update(func(st *DeriverStatus) { st.State, st.LastErr = DeriverStopped, err })
	default:
		w.fail(err)
	}
}

func (w *deriveWorker[In, Out]) fail(err error) {
	w.d.update(func(st *DeriverStatus) { st.State, st.LastErr = DeriverFailed, err })
}
