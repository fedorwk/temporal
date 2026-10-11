package temporal

// slotMeta is the per-slot metadata stored next to each value (32 bytes).
// Seq is not stored: it is derived from the slot and the bounds. Gen is
// series-wide and filled in at copy-out.
type slotMeta struct {
	eventTime   Timestamp
	receiveTime Timestamp
	sourceSeq   uint64
	epoch       SourceEpoch
	flags       Flags
}

// ring is the preallocated, Seq-addressed record storage. It has no lock,
// exposes no slices, and knows nothing about time or key indexes. The Series
// owns its bounds and synchronization.
type ring[T any] struct {
	values []T
	metas  []slotMeta
	seqs   []Seq // debug builds only: the Seq stored in each slot
	n      uint64
	mask   uint64 // n-1 when n is a power of two
	pow2   bool
}

func newRing[T any](capacity int) ring[T] {
	n := uint64(capacity)
	r := ring[T]{
		values: make([]T, capacity),
		metas:  make([]slotMeta, capacity),
		n:      n,
		pow2:   n&(n-1) == 0,
	}
	if r.pow2 {
		r.mask = n - 1
	}
	if debugAssert {
		r.seqs = make([]Seq, capacity)
	}
	return r
}

// slot maps a Seq (>= 1) to its slot: (seq-1) mod capacity.
func (r *ring[T]) slot(s Seq) int {
	if r.pow2 {
		return int((uint64(s) - 1) & r.mask)
	}
	return int((uint64(s) - 1) % r.n)
}

// put writes value and metadata for s into its slot, overwriting whatever the
// slot held.
func (r *ring[T]) put(s Seq, v *T, m *slotMeta) {
	i := r.slot(s)
	r.values[i] = *v
	r.metas[i] = *m
	if debugAssert {
		r.seqs[i] = s
	}
}

// value returns the stored value for a retained s.
func (r *ring[T]) value(s Seq) *T {
	i := r.slot(s)
	if debugAssert {
		assertf(r.seqs[i] == s, "ring slot %d holds seq %d, want %d", i, r.seqs[i], s)
	}
	return &r.values[i]
}

// meta returns the stored metadata for a retained s.
func (r *ring[T]) meta(s Seq) *slotMeta {
	i := r.slot(s)
	if debugAssert {
		assertf(r.seqs[i] == s, "ring slot %d holds seq %d, want %d", i, r.seqs[i], s)
	}
	return &r.metas[i]
}

// eventTime returns the stored EventTime for a retained s.
func (r *ring[T]) eventTime(s Seq) Timestamp { return r.meta(s).eventTime }

// zero clears every slot so that references inside T can be collected.
func (r *ring[T]) zero() {
	clear(r.values)
	clear(r.metas)
	if debugAssert {
		clear(r.seqs)
	}
}

// bounds tracks the retained Seq interval of the current generation.
//
//	empty:     oldest == 0 (and newest == 0 unless a test hook moved it)
//	non-empty: 1 <= oldest <= newest, newest-oldest+1 = len <= capacity
type bounds struct {
	oldest Seq
	newest Seq
}

func (b bounds) empty() bool { return b.oldest == 0 }

func (b bounds) len() int {
	if b.empty() {
		return 0
	}
	return int(b.newest - b.oldest + 1)
}

// retained reports whether s is in [oldest, newest].
func (b bounds) retained(s Seq) bool {
	return !b.empty() && b.oldest <= s && s <= b.newest
}

// nextSeq is the Seq the next appended record receives.
func (b bounds) nextSeq() Seq { return b.newest + 1 }

// push assigns nextSeq to a new record. If the ring is full, the oldest record
// is evicted first and returned.
func (b *bounds) push(capacity int) (evicted Seq, didEvict bool) {
	s := b.newest + 1
	switch {
	case b.empty():
		b.oldest = s
	case b.len() == capacity:
		evicted, didEvict = b.oldest, true
		b.oldest++
	}
	b.newest = s
	return evicted, didEvict
}

// reset empties the bounds so that the next Seq is 1.
func (b *bounds) reset() { *b = bounds{} }

// oldestRef and newestRef return the zero Ref when empty.
func (b bounds) oldestRef(g Gen) Ref {
	if b.empty() {
		return Ref{}
	}
	return Ref{Gen: g, Seq: b.oldest}
}

func (b bounds) newestRef(g Gen) Ref {
	if b.empty() {
		return Ref{}
	}
	return Ref{Gen: g, Seq: b.newest}
}
