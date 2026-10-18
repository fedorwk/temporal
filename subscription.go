package temporal

import (
	"context"
	"fmt"
	"iter"
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
	// SetWatermark wakes it, and a read (Read, TryRead, Next, TryNext) also
	// returns, with no records and Update.WatermarkAdvanced set, when the
	// watermark has advanced past the last one reported and no record is
	// pending. Derive loops use it to forward completeness that arrives after
	// the last record.
	WatermarkUpdates bool
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
// acquisition as a read (see Read).
type SourceState struct {
	Gen                  Gen
	Newest               Ref       // newest retained record; zero if the Series is empty
	Watermark            Timestamp // the Series watermark at the read
	MaxObservedEventTime Timestamp // MinTimestamp if nothing was accepted in Gen
}

// Update describes what one read found. It is zero when the read returns an
// error. A read with Records == 0 and !WatermarkAdvanced found nothing new:
// the next change is signalled on Wake.
type Update struct {
	State   SourceState // captured under the same lock as the records
	Records int         // entries appended to dst by this read
	// WatermarkAdvanced reports that State.Watermark is newer than the last
	// watermark this subscription reported (MinTimestamp at Subscribe and
	// after Seek). Each advance is reported once: with the records it came
	// with, or, under WatermarkUpdates, alone.
	WatermarkAdvanced bool
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
	s.sig.add(sub.sub)
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
	s.sig.remove(sub)
}

func (sub *Subscription[T]) closed() bool {
	select {
	case <-sub.sub.done:
		return true
	default:
		return false
	}
}

// poll is one locked step of the §17.2 algorithm and the body of TryRead; err
// is non-nil for ErrClosed, ErrReset, ErrInvalidInput and lag under LagFail.
// Lag is evaluated in the same lock acquisition as the copy, and the cursor
// moves only after a successful copy (S-005).
func (sub *Subscription[T]) poll(dst []Entry[T], limit int) ([]Entry[T], Update, error) {
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
		return dst, Update{}, ErrClosed
	case sub.cur.Gen < s.gen:
		return dst, Update{}, ErrReset
	case sub.cur.Gen > s.gen:
		return dst, Update{}, fmt.Errorf("%w: subscription cursor from a later generation", ErrInvalidInput)
	}

	next := min(sub.cur.Next, s.adm.b.nextSeq()) // clamps StartAt beyond Newest+1
	var skip uint64
	if !s.adm.b.empty() && next < s.adm.b.oldest {
		missed := uint64(s.adm.b.oldest - next)
		if sub.onLag == LagFail {
			return dst, Update{}, &LagError{
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
		adv := snap.Watermark > sub.lastWM
		sub.lastWM = snap.Watermark
		return dst, Update{State: snap, Records: n, WatermarkAdvanced: adv}, nil
	}
	sub.cur.Next = next
	if sub.sub.wmUpdates && snap.Watermark > sub.lastWM {
		sub.lastWM = snap.Watermark
		return dst, Update{State: snap, WatermarkAdvanced: true}, nil
	}
	if s.closed {
		return dst, Update{}, s.closeErr
	}
	return dst, Update{State: snap}, nil
}

// Next blocks until at least one record is available and appends up to
// min(limit, MaxReadBatch) records to dst in ascending Seq order, advancing
// the cursor. It returns early with ctx.Err() if ctx is done (checked first,
// without moving the cursor), ErrClosed after Close or once a closed Series is
// drained (a *ClosedError carrying the cause if the Series was closed with
// CloseWithError), ErrReset after Clear until Seek, and a *LagError (ErrLagged)
// if the cursor fell behind retention under LagFail. With WatermarkUpdates it
// also returns, with no records, when only the watermark moved.
func (sub *Subscription[T]) Next(ctx context.Context, dst []Entry[T], limit int) ([]Entry[T], error) {
	out, _, err := sub.Read(ctx, dst, limit)
	return out, err
}

// Read is Next that also reports what the read found: the state of the source
// Series captured under the same lock as the records, how many records were
// appended, and whether the watermark advanced. It returns once there are
// records or, with WatermarkUpdates, a watermark advance, so Records > 0 ||
// WatermarkAdvanced holds on every nil-error return. It is the building block
// for derived series: after processing a batch whose last record is
// u.State.Newest, every record up to u.State.Watermark has been seen, so the
// watermark may be forwarded to a destination series. A producer usually
// advances the watermark after appending, so subscribe with WatermarkUpdates
// and the loop can forward it at once. See ExampleSubscription_Read for a
// complete derive loop.
func (sub *Subscription[T]) Read(ctx context.Context, dst []Entry[T], limit int) ([]Entry[T], Update, error) {
	for {
		if err := ctx.Err(); err != nil {
			return dst, Update{}, err
		}
		out, u, err := sub.poll(dst, limit)
		switch {
		case err != nil:
			return out, Update{}, err
		case u.Records > 0 || u.WatermarkAdvanced:
			return out, u, nil
		}
		select {
		case <-sub.sub.wake:
		case <-ctx.Done():
			return dst, Update{}, ctx.Err()
		case <-sub.sub.done:
			return dst, Update{}, ErrClosed
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
	out, _, err := sub.poll(dst, limit)
	return out, err
}

// TryRead is Read without blocking: one read step, which may find nothing.
// A loop that waits on several subscriptions calls it after each receive
// from Wake until a call returns Records == 0 && !WatermarkAdvanced (or an
// error); one wake-up may stand for several changes, for example a watermark
// advance (reported first) and a close. After such a call, every record up
// to u.State.Watermark has been read.
func (sub *Subscription[T]) TryRead(dst []Entry[T], limit int) ([]Entry[T], Update, error) {
	return sub.poll(dst, limit)
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

// Wake returns the coalesced wake channel, for select loops around TryRead.
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
