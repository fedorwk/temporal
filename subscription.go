package temporal

import (
	"context"
	"fmt"
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
}

// subscriber is the notification endpoint shared with the Series.
type subscriber struct {
	wake      chan struct{} // buffer 1: coalesced wake-ups
	done      chan struct{} // closed by Subscription.Close
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
		s:     s,
		sub:   &subscriber{wake: make(chan struct{}, 1), done: make(chan struct{})},
		name:  opts.Name,
		onLag: opts.OnLag,
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
	case from.kind == startNext || s.b.empty():
		return Cursor{Gen: s.gen, Next: s.b.nextSeq()}
	default: // StartOldest, or StartAt with the zero Ref
		return Cursor{Gen: s.gen, Next: s.b.oldest}
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
		Newest:               s.b.newestRef(s.gen),
		Watermark:            s.watermark,
		MaxObservedEventTime: MinTimestamp,
	}
	if s.hasObserved {
		snap.MaxObservedEventTime = s.maxObserved
	}
	switch {
	case sub.closed():
		return dst, snap, false, ErrClosed
	case sub.cur.Gen < s.gen:
		return dst, snap, false, ErrReset
	case sub.cur.Gen > s.gen:
		return dst, snap, false, fmt.Errorf("%w: subscription cursor from a later generation", ErrInvalidInput)
	}

	next := min(sub.cur.Next, s.b.nextSeq()) // clamps StartAt beyond Newest+1
	var skip uint64
	if !s.b.empty() && next < s.b.oldest {
		missed := uint64(s.b.oldest - next)
		if sub.onLag == LagFail {
			return dst, snap, false, &LagError{
				Cursor: sub.cur,
				Oldest: Ref{Gen: s.gen, Seq: s.b.oldest},
				Missed: missed,
			}
		}
		skip, next = missed, s.b.oldest
	}
	if !s.b.empty() && next <= s.b.newest {
		n := min(int(s.b.newest-next+1), s.effectiveLimit(limit))
		base := len(dst)
		dst = growEntries(dst, n)
		for i := range n {
			s.copyOut(&dst[base+i], next+Seq(i))
		}
		sub.cur.Next = next + Seq(n)
		sub.skipped += skip
		return dst, snap, true, nil
	}
	sub.cur.Next = next
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
// watermark may be forwarded to a destination series. See
// ExampleSubscription_NextState for a complete derive loop.
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

// TryNext is Next without blocking: it may return zero entries and a nil error.
func (sub *Subscription[T]) TryNext(dst []Entry[T], limit int) ([]Entry[T], error) {
	out, _, _, err := sub.poll(dst, limit)
	return out, err
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
