package temporal

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
)

// modelSub is the reference model of one subscription (§17.2, P-07, S8-03).
type modelSub struct {
	cur     Cursor
	onLag   LagPolicy
	skipped uint64
	closed  bool
}

func (m *model) startCursor(from StartFrom) Cursor {
	switch {
	case from.kind == startAt && from.ref.Seq != 0:
		return Cursor{Gen: from.ref.Gen, Next: from.ref.Seq}
	case from.kind == startOldest || from.kind == startAt:
		if len(m.recs) == 0 {
			return Cursor{Gen: m.gen, Next: m.nextSeq}
		}
		return Cursor{Gen: m.gen, Next: m.recs[0].Meta.Ref.Seq}
	default: // unset or StartNext
		return Cursor{Gen: m.gen, Next: m.nextSeq}
	}
}

func (m *model) tryNext(sub *modelSub, limit int) ([]Entry[int], error) {
	switch {
	case sub.closed:
		return nil, ErrClosed
	case sub.cur.Gen < m.gen:
		return nil, ErrReset
	case sub.cur.Gen > m.gen:
		return nil, ErrInvalidInput
	}
	next := min(sub.cur.Next, m.nextSeq)
	var skip uint64
	if len(m.recs) > 0 {
		oldest := m.recs[0].Meta.Ref.Seq
		if next < oldest {
			if sub.onLag == LagFail {
				return nil, &LagError{Cursor: sub.cur, Oldest: Ref{Gen: m.gen, Seq: oldest}, Missed: uint64(oldest - next)}
			}
			skip, next = uint64(oldest-next), oldest
		}
	}
	var out []Entry[int]
	for _, e := range m.recs {
		if e.Meta.Ref.Seq >= next && len(out) < m.limit(limit) {
			out = append(out, e)
		}
	}
	if len(out) > 0 {
		sub.cur.Next = out[len(out)-1].Meta.Ref.Seq + 1
		sub.skipped += skip
		return out, nil
	}
	sub.cur.Next = next
	if m.closed {
		return nil, ErrClosed
	}
	return nil, nil
}

func randomStart(r *rand.Rand, m *model) StartFrom {
	switch r.IntN(5) {
	case 0:
		return StartFrom{}
	case 1:
		return StartNext()
	case 2:
		return StartOldest()
	default:
		return StartAt(randomRef(r, m))
	}
}

// subPair is a real subscription and its model, side by side.
type subPair struct {
	real  *Subscription[int]
	model *modelSub
}

// runSubOp runs one random subscription operation and compares it with the
// model. It returns a non-empty name and a description on mismatch.
func runSubOp(r *rand.Rand, s *Series[int], m *model, subs *[]subPair, seen map[string]int) (string, string) {
	if len(*subs) == 0 || r.IntN(5) == 0 {
		start, lag := randomStart(r, m), LagPolicy(r.IntN(2))
		real, err := s.Subscribe(SubscribeOptions{Start: start, OnLag: lag})
		if err != nil {
			return "Subscribe", err.Error()
		}
		ms := &modelSub{cur: m.startCursor(start), onLag: lag}
		if real.Cursor() != ms.cur {
			return "Subscribe", fmt.Sprintf("start %+v: cursor %+v, want %+v", start, real.Cursor(), ms.cur)
		}
		*subs = append(*subs, subPair{real, ms})
		return "", ""
	}
	p := (*subs)[r.IntN(len(*subs))]
	switch op := r.IntN(10); {
	case op < 7:
		limit := r.IntN(6) - 1
		got, gotErr := p.real.TryNext(nil, limit)
		want, wantErr := m.tryNext(p.model, limit)
		if !subErrMatches(gotErr, wantErr) || !slices.Equal(got, want) && len(got)+len(want) > 0 {
			return "TryNext", fmt.Sprintf("limit %d: got (%v, %v), want (%v, %v)", limit, seqsOf(got), gotErr, seqsOf(want), wantErr)
		}
		switch {
		case errors.Is(gotErr, ErrLagged):
			seen["sub lag"]++
		case errors.Is(gotErr, ErrReset):
			seen["sub reset"]++
		case len(got) > 0 && p.model.skipped > 0:
			seen["sub skip"]++
		}
	case op < 9:
		start := randomStart(r, m)
		gotErr := p.real.Seek(start)
		var wantErr error
		if p.model.closed {
			wantErr = ErrClosed
		} else {
			p.model.cur = m.startCursor(start)
		}
		if !errors.Is(gotErr, wantErr) && !(gotErr == nil && wantErr == nil) {
			return "Seek", fmt.Sprintf("got %v, want %v", gotErr, wantErr)
		}
	default:
		p.real.Close()
		p.model.closed = true
	}
	if p.real.Cursor() != p.model.cur || p.real.Skipped() != p.model.skipped {
		return "subscription state", fmt.Sprintf("cursor %+v skipped %d, want %+v %d",
			p.real.Cursor(), p.real.Skipped(), p.model.cur, p.model.skipped)
	}
	return "", ""
}

// subErrMatches compares subscription errors, including LagError fields.
func subErrMatches(got, want error) bool {
	var wl *LagError
	if errors.As(want, &wl) {
		var gl *LagError
		return errors.As(got, &gl) && *gl == *wl
	}
	return errMatches(got, want)
}

func (m *model) openSubs(subs []subPair) int {
	n := 0
	for _, p := range subs {
		if !p.model.closed {
			n++
		}
	}
	return n
}
