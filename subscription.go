package temporal

import (
	"context"
	"fmt"
	"iter"
	"slices"
	"sync"
)

type startKind uint8

const (
	startNext startKind = iota // zero value
	startOldest
	startAt
)

// StartFrom is a subscription start position.
type StartFrom struct {
	kind startKind
	ref  Ref
}

// StartOldest starts at the oldest record retained when the position is
// applied (Subscribe or Seek).
func StartOldest() StartFrom { return StartFrom{kind: startOldest} }

// StartNext starts after the newest record: only future records are read.
func StartNext() StartFrom { return StartFrom{kind: startNext} }

// StartAt starts at ref. It is validated on the next read: an earlier
// generation yields ErrReset, a later one ErrInvalidInput, and a Seq beyond
// Newest+1 is clamped to Newest+1. The zero Ref is the same as StartOldest.
func StartAt(ref Ref) StartFrom { return StartFrom{kind: startAt, ref: ref} }

// SubscribeOptions configures a subscription.
type SubscribeOptions struct {
	Name  string
	Start StartFrom // zero value = StartNext
	OnLag LagPolicy
	// WatermarkUpdates makes the subscription follow the watermark too:
	// SetWatermark wakes it, and NextState (and Next, TryNext) also return,
	// with no records, when the watermark has advanced past the last state
	// returned and no record is pending. Derive loops use it to forward
	// completeness that arrives after the last record.
	WatermarkUpdates bool
}

// subscriber is the notification endpoint shared with the Series.
type subscriber struct {
	wake      chan struct{} // buffer 1: coalesced wake-ups
	done      chan struct{} // closed by Subscription.Close
	wmUpdates bool          // woken by SetWatermark; immutable
	closeOnce sync.Once
}

// Subscription reads a Series in Seq order. It must be used by one goroutine
// at a time; Close may be called from any goroutine.
type Subscription[T any] struct {
	s       *Series[T]
	sub     *subscriber
	name    string
	onLag   LagPolicy
	cur     Cursor
	skipped uint64
	lastWM  Timestamp // watermark of the last returned state (WatermarkUpdates)
}

// SourceState is the state of the subscribed Series captured in the same lock
// acquisition as a read (see NextState).
type SourceState struct {
	Gen                  Gen
	Newest               Ref       // newest retained record; zero if the Series is empty
	Watermark            Timestamp // the Series watermark at the read
	MaxObservedEventTime Timestamp // MinTimestamp if nothing was accepted in Gen
}

// Subscribe registers a subscription. It succeeds on a closed Series, whose
// retained records can still be drained (P-07).
func (s *Series[T]) Subscribe(opts SubscribeOptions) (*Subscription[T], error) {
	if !opts.OnLag.valid() || opts.Start.kind > startAt {
		return nil, fmt.Errorf("%w: invalid SubscribeOptions", ErrInvalidInput)
	}
	sub := &Subscription[T]{
		s: s,
		sub: &subscriber{
			wake:      make(chan struct{}, 1),
			done:      make(chan struct{}),
			wmUpdates: opts.WatermarkUpdates,
		},
		name:   opts.Name,
		onLag:  opts.OnLag,
		lastWM: MinTimestamp,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sub.cur = s.startCursor(opts.Start)
	var list []*subscriber
	if p := s.subs.Load(); p != nil {
		list = slices.Clone(*p)
	}
	list = append(list, sub.sub)
	s.subs.Store(&list)
	return sub, nil
}

// startCursor resolves a start position in the current generation.
func (s *Series[T]) startCursor(from StartFrom) Cursor {
	switch {
	case from.kind == startAt && !from.ref.IsZero():
		return Cursor{Gen: from.ref.Gen, Next: from.ref.Seq}
	case from.kind == startNext || s.adm.b.empty():
		return Cursor{Gen: s.gen, Next: s.adm.b.nextSeq()}
	default: // StartOldest, or StartAt with the zero Ref
		return Cursor{Gen: s.gen, Next: s.adm.b.oldest}
	}
}

// unsubscribe removes sub from the notification list.
func (s *Series[T]) unsubscribe(sub *subscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.subs.Load()
	if p == nil {
		return
	}
	list := slices.DeleteFunc(slices.Clone(*p), func(x *subscriber) bool { return x == sub })
	s.subs.Store(&list)
}

// notify wakes every subscriber without blocking. It is called after the
// mutex is released; a full wake buffer means a wake-up is already pending.
func (s *Series[T]) notify() {
	if debugAssert {
		s.dbgNotifies.Add(1)
	}
	p := s.subs.Load()
	if p == nil {
		return
	}
	for _, sub := range *p {
		select {
		case sub.wake <- struct{}{}:
		default:
		}
	}
}

// notifyWatermark wakes the subscribers that follow the watermark. Like
// notify it is called after the mutex is released and never blocks.
func (s *Series[T]) notifyWatermark() {
	p := s.subs.Load()
	if p == nil {
		return
	}
	for _, sub := range *p {
		if !sub.wmUpdates {
			continue
		}
		select {
		case sub.wake <- struct{}{}:
		default:
		}
	}
}

// subscriberCount returns the number of open subscriptions.
func (s *Series[T]) subscriberCount() int {
	if p := s.subs.Load(); p != nil {
		return len(*p)
	}
	return 0
}

func (sub *Subscription[T]) closed() bool {
	select {
	case <-sub.sub.done:
		return true
	default:
		return false
	}
}

// poll is one locked step of the §17.2 algorithm. It reports whether data was
// returned; err is non-nil for ErrClosed, ErrReset, ErrInvalidInput and lag
// under LagFail. Lag is evaluated in the same lock acquisition as the copy,
// and the cursor moves only after a successful copy (S-005).
func (sub *Subscription[T]) poll(dst []Entry[T], limit int) ([]Entry[T], SourceState, bool, error) {
	s := sub.s
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := SourceState{
		Gen:                  s.gen,
		Newest:               s.adm.b.newestRef(s.gen),
		Watermark:            s.watermark,
		MaxObservedEventTime: s.adm.maxObserved,
	}
	switch {
	case sub.closed():
		return dst, snap, false, ErrClosed
	case sub.cur.Gen < s.gen:
		return dst, snap, false, ErrReset
	case sub.cur.Gen > s.gen:
		return dst, snap, false, fmt.Errorf("%w: subscription cursor from a later generation", ErrInvalidInput)
	}

	next := min(sub.cur.Next, s.adm.b.nextSeq()) // clamps StartAt beyond Newest+1
	var skip uint64
	if !s.adm.b.empty() && next < s.adm.b.oldest {
		missed := uint64(s.adm.b.oldest - next)
		if sub.onLag == LagFail {
			return dst, snap, false, &LagError{
				Cursor: sub.cur,
				Oldest: Ref{Gen: s.gen, Seq: s.adm.b.oldest},
				Missed: missed,
			}
		}
		skip, next = missed, s.adm.b.oldest
	}
	if !s.adm.b.empty() && next <= s.adm.b.newest {
		n := min(int(s.adm.b.newest-next+1), s.effectiveLimit(limit))
		base := len(dst)
		dst = growEntries(dst, n)
		for i := range n {
			s.copyOut(&dst[base+i], next+Seq(i))
		}
		sub.cur.Next = next + Seq(n)
		sub.skipped += skip
		sub.lastWM = snap.Watermark
		return dst, snap, true, nil
	}
	sub.cur.Next = next
	if sub.sub.wmUpdates && snap.Watermark > sub.lastWM {
		sub.lastWM = snap.Watermark
		return dst, snap, true, nil
	}
	if s.closed {
		return dst, snap, false, s.closeErr
	}
	return dst, snap, false, nil
}

// Next blocks until at least one record is available and appends up to
// min(limit, MaxReadBatch) records to dst in ascending Seq order, advancing
// the cursor. It returns early with ctx.Err() if ctx is done (checked first,
// without moving the cursor), ErrClosed after Close or once a closed Series is
// drained (a *ClosedError carrying the cause if the Series was closed with
// CloseWithError), ErrReset after Clear until Seek, and a *LagError (ErrLagged)
// if the cursor fell behind retention under LagFail.
func (sub *Subscription[T]) Next(ctx context.Context, dst []Entry[T], limit int) ([]Entry[T], error) {
	out, _, err := sub.NextState(ctx, dst, limit)
	return out, err
}

// NextState is Next that also returns the state of the source Series captured
// under the same lock as the records (zero on error). It is the building block
// for derived series: after processing a batch whose last record is
// state.Newest, every record up to state.Watermark has been seen, so the
// watermark may be forwarded to a destination series. A producer usually
// advances the watermark after appending, so subscribe with WatermarkUpdates:
// NextState then also returns with no records when only the watermark moved,
// and the loop can forward it at once. See ExampleSubscription_NextState for a
// complete derive loop.
func (sub *Subscription[T]) NextState(ctx context.Context, dst []Entry[T], limit int) ([]Entry[T], SourceState, error) {
	for {
		if err := ctx.Err(); err != nil {
			return dst, SourceState{}, err
		}
		out, state, got, err := sub.poll(dst, limit)
		switch {
		case err != nil:
			return out, SourceState{}, err
		case got:
			return out, state, nil
		}
		select {
		case <-sub.sub.wake:
		case <-ctx.Done():
			return dst, SourceState{}, ctx.Err()
		case <-sub.sub.done:
			return dst, SourceState{}, ErrClosed
		}
	}
}

// All returns an iterator over the subscription's records in Seq order. It
// reads batches of up to min(limit, MaxReadBatch) records with Next, yielding
// (entry, nil) for each, and stops when reading stops: after a clean close of
// a drained Series it simply ends, otherwise it yields (zero Entry, err) once
// with the error Next returned (ctx.Err(), a *ClosedError, ErrReset, a
// *LagError, ...). Breaking out of the loop stops reading and leaves the
// subscription open, positioned right after the last record yielded. The iterator reuses one batch buffer and, like the
// subscription, must be used by one goroutine at a time.
func (sub *Subscription[T]) All(ctx context.Context, limit int) iter.Seq2[Entry[T], error] {
	return func(yield func(Entry[T], error) bool) {
		buf := make([]Entry[T], 0, sub.s.effectiveLimit(limit))
		for {
			var err error
			buf, err = sub.Next(ctx, buf[:0], limit)
			if err != nil {
				if err != ErrClosed {
					yield(Entry[T]{}, err)
				}
				return
			}
			for i, e := range buf {
				if !yield(e, nil) {
					if i+1 < len(buf) {
						// Rewind past what was read but not yielded.
						_ = sub.Seek(StartAt(buf[i+1].Meta.Ref))
					}
					return
				}
			}
		}
	}
}

// TryNext is Next without blocking: it may return zero entries and a nil error.
func (sub *Subscription[T]) TryNext(dst []Entry[T], limit int) ([]Entry[T], error) {
	out, _, _, err := sub.poll(dst, limit)
	return out, err
}

// TryNextState is NextState without blocking: it may return zero entries and a
// nil error. The state is captured under the same lock as the entries, so
// after a call that returns nothing new, every record up to state.Watermark
// has been read. Loops that wait on several subscriptions use it with Wake.
// With WatermarkUpdates, a watermark advance is reported before a close, and
// one wake-up may stand for both: after a wake-up, call until a call returns
// neither records nor a changed watermark (or an error).
func (sub *Subscription[T]) TryNextState(dst []Entry[T], limit int) ([]Entry[T], SourceState, error) {
	out, state, _, err := sub.poll(dst, limit)
	if err != nil {
		return out, SourceState{}, err
	}
	return out, state, nil
}

// Seek moves the cursor to a start position in the current generation. It is
// the way to resume after ErrReset or a lag error.
func (sub *Subscription[T]) Seek(from StartFrom) error {
	if from.kind > startAt {
		return fmt.Errorf("%w: invalid StartFrom", ErrInvalidInput)
	}
	if sub.closed() {
		return ErrClosed
	}
	s := sub.s
	s.mu.Lock()
	defer s.mu.Unlock()
	sub.cur = s.startCursor(from)
	sub.lastWM = MinTimestamp
	return nil
}

// Cursor returns the subscription position.
func (sub *Subscription[T]) Cursor() Cursor { return sub.cur }

// Skipped returns the number of records skipped under LagSkip.
func (sub *Subscription[T]) Skipped() uint64 { return sub.skipped }

// Wake returns the coalesced wake channel, for select loops around TryNext.
// A receive means the Series may have changed.
func (sub *Subscription[T]) Wake() <-chan struct{} { return sub.sub.wake }

// Close stops the subscription and unblocks a concurrent Next with ErrClosed.
// It is idempotent and always returns nil.
func (sub *Subscription[T]) Close() error {
	sub.sub.closeOnce.Do(func() {
		close(sub.sub.done)
		sub.s.unsubscribe(sub.sub)
	})
	return nil
}
