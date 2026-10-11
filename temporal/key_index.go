package temporal

import (
	"fmt"
	"sync"
)

// Extractor returns the key of a record, or ok=false if the record is not
// indexed. It runs in the prepare phase of Append with Meta populated,
// including the tentative Ref. It must be pure, deterministic and
// non-blocking, must not retain e, and must not call any Series method.
type Extractor[T any, K comparable] func(e *Entry[T]) (key K, ok bool)

// Index is a key index registered with a Series through Config.Indexes. Its
// methods are unexported, so only this package can implement it. Create one
// with NewUniqueIndex or NewMultiIndex. A handle binds to exactly one Series.
//
// All methods except indexName are called with the Series mutex held. The
// typed key of each prepared item stays in the index's own scratch, indexed by
// batch position, so no key is boxed.
type Index[T any] interface {
	indexName() string
	validate() error // ErrInvalidConfig for an unknown policy or nil extractor
	isBound() bool
	bind(s *Series[T], capacity int)

	// prepare extracts the key of the item at batch position pos and decides
	// its outcome without mutating committed state. Rows with Seq < evictBelow
	// count as evicted. It returns *ConflictError under ConflictReject; a
	// panic in the extractor propagates to the caller.
	prepare(e *Entry[T], evictBelow Seq, pos int) (decision, error)
	// beginBatch, stage and endBatch maintain the overlay of keys claimed by
	// earlier items of an AppendBatch (P-02).
	beginBatch()
	stage(pos int, seq Seq)
	endBatch()

	commit(slot int, seq Seq, pos int) // apply the prepared decision for pos
	noteDropped(pos int)               // count a committed drop decided by this index
	evict(slot int, seq Seq)
	reset()
	stats() IndexStats
	checkLocked() error
}

// decisionKind is the prepared outcome of one item for one index.
type decisionKind uint8

const (
	decNotIndexed decisionKind = iota // extractor returned ok=false
	decInsert                         // key free: index the item
	decReplace                        // ConflictReplace: the key moves to the item
	decKeepFirst                      // ConflictKeepFirst: stored but not indexed here
	decDrop                           // ConflictDrop: the item is not stored
)

// decision is non-generic so the Series can combine decisions across indexes.
type decision struct {
	kind     decisionKind
	existing Seq // current holder of the key for Replace, KeepFirst and Drop
}

// bindMu serializes index binding across concurrent New calls (K-003).
var bindMu sync.Mutex

// bitset is a fixed-size set of slot numbers.
type bitset []uint64

func newBitset(n int) bitset    { return make(bitset, (n+63)/64) }
func (b bitset) get(i int) bool { return b[i>>6]&(1<<(uint(i)&63)) != 0 }
func (b bitset) set(i int)      { b[i>>6] |= 1 << (uint(i) & 63) }
func (b bitset) unset(i int)    { b[i>>6] &^= 1 << (uint(i) & 63) }
func (b bitset) clearAll()      { clear(b) }

// keyCore is the state shared by both index kinds: the per-slot stored keys
// and indexed bits (K-006), and the per-batch-position prepare scratch.
type keyCore[T any, K comparable] struct {
	name    string
	fn      Extractor[T, K]
	s       *Series[T]
	keys    []K    // per slot: the key stored at commit
	indexed bitset // per slot: the row is entered in this index
	pkeys   []K    // per batch position: extracted key
	pdec    []decision
}

func (c *keyCore[T, K]) indexName() string { return c.name }

func (c *keyCore[T, K]) validateCore() error {
	if c.fn == nil {
		return fmt.Errorf("%w: index %q has a nil extractor", ErrInvalidConfig, c.name)
	}
	return nil
}
func (c *keyCore[T, K]) isBound() bool { return c.s != nil }

func (c *keyCore[T, K]) bindCore(s *Series[T], capacity int) {
	c.s = s
	c.keys = make([]K, capacity)
	c.indexed = newBitset(capacity)
	c.pkeys = make([]K, 1)
	c.pdec = make([]decision, 1)
}

// extract runs the extractor for position pos, growing the scratch as needed.
// A panic propagates; prepare has mutated nothing at that point.
func (c *keyCore[T, K]) extract(e *Entry[T], pos int) (K, bool) {
	if pos >= len(c.pkeys) {
		c.pkeys = append(c.pkeys, make([]K, pos+1-len(c.pkeys))...)
		c.pdec = append(c.pdec, make([]decision, pos+1-len(c.pdec))...)
	}
	return c.fn(e)
}

// lockedSeries locks the bound Series for a lookup, or returns nil for an
// unbound handle.
func (c *keyCore[T, K]) lockedSeries() *Series[T] {
	if c.s == nil {
		return nil
	}
	c.s.mu.Lock()
	return c.s
}

// UniqueIndex maps each key to at most one retained record.
type UniqueIndex[T any, K comparable] struct {
	keyCore[T, K]
	policy  ConflictPolicy
	m       map[K]Seq
	pending map[K]Seq // keys claimed by earlier items of the current batch
	batch   bool

	conflicts, dropped, replaced, keptFirst uint64
}

// NewUniqueIndex creates a unique index with the given conflict policy. Pass
// it in Config.Indexes; an unknown policy fails New with ErrInvalidConfig.
func NewUniqueIndex[T any, K comparable](name string, fn Extractor[T, K], p ConflictPolicy) *UniqueIndex[T, K] {
	return &UniqueIndex[T, K]{keyCore: keyCore[T, K]{name: name, fn: fn}, policy: p}
}

func (ix *UniqueIndex[T, K]) validate() error {
	if !ix.policy.valid() {
		return fmt.Errorf("%w: index %q has unknown ConflictPolicy %d", ErrInvalidConfig, ix.name, ix.policy)
	}
	return ix.validateCore()
}

func (ix *UniqueIndex[T, K]) bind(s *Series[T], capacity int) {
	ix.bindCore(s, capacity)
	ix.m = make(map[K]Seq)
}

// holder returns the record that holds k for an item whose transaction
// evicts every Seq below evictBelow.
func (ix *UniqueIndex[T, K]) holder(k K, evictBelow Seq) (Seq, bool) {
	if ix.batch {
		if seq, ok := ix.pending[k]; ok {
			return seq, true
		}
	}
	if seq, ok := ix.m[k]; ok && seq >= evictBelow {
		return seq, true
	}
	return 0, false
}

func (ix *UniqueIndex[T, K]) prepare(e *Entry[T], evictBelow Seq, pos int) (decision, error) {
	k, ok := ix.extract(e, pos)
	d := decision{kind: decNotIndexed}
	if ok {
		d.kind = decInsert
		if h, taken := ix.holder(k, evictBelow); taken {
			d.existing = h
			switch ix.policy {
			case ConflictDrop:
				d.kind = decDrop
			case ConflictReplace:
				d.kind = decReplace
			case ConflictKeepFirst:
				d.kind = decKeepFirst
			default:
				ix.conflicts++ // P-04: diagnostic counter moves on a failed append
				return decision{}, &ConflictError{
					Index:    ix.name,
					Key:      fmt.Sprint(k), // error path only (P-18)
					Existing: Ref{Gen: e.Meta.Ref.Gen, Seq: h},
					BatchPos: pos,
				}
			}
		}
	}
	ix.pkeys[pos] = k
	ix.pdec[pos] = d
	return d, nil
}

func (ix *UniqueIndex[T, K]) beginBatch() {
	ix.batch = true
	if ix.pending == nil {
		ix.pending = make(map[K]Seq)
	}
}

func (ix *UniqueIndex[T, K]) stage(pos int, seq Seq) {
	if d := ix.pdec[pos].kind; d == decInsert || d == decReplace {
		ix.pending[ix.pkeys[pos]] = seq
	}
}

func (ix *UniqueIndex[T, K]) endBatch() {
	ix.batch = false
	clear(ix.pending)
}

func (ix *UniqueIndex[T, K]) commit(slot int, seq Seq, pos int) {
	d := ix.pdec[pos]
	var zero K
	switch d.kind {
	case decNotIndexed:
		ix.keys[slot] = zero
		ix.indexed.unset(slot)
		return
	case decKeepFirst:
		ix.keys[slot] = ix.pkeys[pos]
		ix.indexed.unset(slot)
		ix.keptFirst++
		return
	case decReplace:
		if debugAssert {
			assertf(ix.m[ix.pkeys[pos]] == d.existing, "unique %q: replace holder %d, map has %d",
				ix.name, d.existing, ix.m[ix.pkeys[pos]])
		}
		ix.indexed.unset(ix.s.ring.slot(d.existing))
		ix.replaced++
	default:
		if debugAssert {
			_, taken := ix.m[ix.pkeys[pos]]
			assertf(d.kind == decInsert && !taken, "unique %q: commit %v on a taken key", ix.name, d.kind)
		}
	}
	k := ix.pkeys[pos]
	ix.keys[slot] = k
	ix.indexed.set(slot)
	ix.m[k] = seq
}

func (ix *UniqueIndex[T, K]) noteDropped(pos int) {
	if ix.pdec[pos].kind == decDrop {
		ix.dropped++
	}
}

// evict removes the mapping only if it still points to seq (K-008). Replace
// clears the previous holder's indexed bit, so an indexed row always holds its
// key; the comparison is the spec's guard and is asserted in debug builds.
func (ix *UniqueIndex[T, K]) evict(slot int, seq Seq) {
	if ix.indexed.get(slot) {
		k := ix.keys[slot]
		if debugAssert {
			assertf(ix.m[k] == seq, "unique %q: indexed seq %d does not hold its key", ix.name, seq)
		}
		if ix.m[k] == seq {
			delete(ix.m, k)
		}
	}
	var zero K
	ix.keys[slot] = zero
	ix.indexed.unset(slot)
}

func (ix *UniqueIndex[T, K]) reset() {
	clear(ix.m)
	clear(ix.keys)
	ix.indexed.clearAll()
}

func (ix *UniqueIndex[T, K]) stats() IndexStats {
	return IndexStats{
		Name:      ix.name,
		Unique:    true,
		Keys:      len(ix.m),
		Conflicts: ix.conflicts,
		Dropped:   ix.dropped,
		Replaced:  ix.replaced,
		KeptFirst: ix.keptFirst,
	}
}

// Get returns the record mapped to k, or StatusNotFound. An evicted key and a
// never-seen key both return StatusNotFound. An unbound handle returns
// StatusInvalid.
func (ix *UniqueIndex[T, K]) Get(k K) (Entry[T], Status) {
	var e Entry[T]
	s := ix.lockedSeries()
	if s == nil {
		return e, StatusInvalid
	}
	defer s.mu.Unlock()
	seq, ok := ix.m[k]
	if !ok {
		return e, StatusNotFound
	}
	s.copyOut(&e, seq)
	return e, StatusOK
}

// Contains reports whether k is mapped to a retained record.
func (ix *UniqueIndex[T, K]) Contains(k K) bool {
	s := ix.lockedSeries()
	if s == nil {
		return false
	}
	defer s.mu.Unlock()
	_, ok := ix.m[k]
	return ok
}

// Len returns the number of mapped keys.
func (ix *UniqueIndex[T, K]) Len() int {
	s := ix.lockedSeries()
	if s == nil {
		return 0
	}
	defer s.mu.Unlock()
	return len(ix.m)
}

// checkLocked verifies the §14.1 unique-index invariants.
func (ix *UniqueIndex[T, K]) checkLocked() error {
	s := ix.s
	want := 0
	for seq := s.b.oldest; !s.b.empty() && seq <= s.b.newest; seq++ {
		slot := s.ring.slot(seq)
		if !ix.indexed.get(slot) {
			continue
		}
		want++
		if got, ok := ix.m[ix.keys[slot]]; !ok || got != seq {
			return fmt.Errorf("unique %q: indexed seq %d key %v maps to %d (present %v)",
				ix.name, seq, ix.keys[slot], got, ok)
		}
	}
	if len(ix.m) != want {
		return fmt.Errorf("unique %q: %d mappings, %d indexed retained rows", ix.name, len(ix.m), want)
	}
	if n := countUnretainedIndexed(s, ix.indexed); n > 0 {
		return fmt.Errorf("unique %q: %d non-retained slots marked indexed", ix.name, n)
	}
	return nil
}

// countUnretainedIndexed counts indexed bits on slots that hold no retained row.
func countUnretainedIndexed[T any](s *Series[T], indexed bitset) int {
	retained := newBitset(s.cfg.capacity)
	for seq := s.b.oldest; !s.b.empty() && seq <= s.b.newest; seq++ {
		retained.set(s.ring.slot(seq))
	}
	n := 0
	for slot := range s.cfg.capacity {
		if indexed.get(slot) && !retained.get(slot) {
			n++
		}
	}
	return n
}

// postings is a FIFO of ascending Seqs for one key (K-009).
type postings struct {
	seqs []Seq
	head int
}

func (p *postings) len() int     { return len(p.seqs) - p.head }
func (p *postings) front() Seq   { return p.seqs[p.head] }
func (p *postings) back() Seq    { return p.seqs[len(p.seqs)-1] }
func (p *postings) live() []Seq  { return p.seqs[p.head:] }
func (p *postings) push(seq Seq) { p.seqs = append(p.seqs, seq) }

// popFront removes the oldest Seq and compacts once the head passes half the
// backing slice.
func (p *postings) popFront() {
	p.head++
	if p.head > len(p.seqs)/2 {
		n := copy(p.seqs, p.seqs[p.head:])
		p.seqs = p.seqs[:n]
		p.head = 0
	}
}

// MultiIndex maps each key to all retained records with that key, in Seq order.
type MultiIndex[T any, K comparable] struct {
	keyCore[T, K]
	m    map[K]*postings
	free []*postings // recycled empty posting lists
}

// NewMultiIndex creates a non-unique index.
func NewMultiIndex[T any, K comparable](name string, fn Extractor[T, K]) *MultiIndex[T, K] {
	return &MultiIndex[T, K]{keyCore: keyCore[T, K]{name: name, fn: fn}}
}

func (ix *MultiIndex[T, K]) validate() error { return ix.validateCore() }

func (ix *MultiIndex[T, K]) bind(s *Series[T], capacity int) {
	ix.bindCore(s, capacity)
	ix.m = make(map[K]*postings)
}

func (ix *MultiIndex[T, K]) prepare(e *Entry[T], _ Seq, pos int) (decision, error) {
	k, ok := ix.extract(e, pos)
	d := decision{kind: decNotIndexed}
	if ok {
		d.kind = decInsert
	}
	ix.pkeys[pos] = k
	ix.pdec[pos] = d
	return d, nil
}

func (ix *MultiIndex[T, K]) beginBatch()     {}
func (ix *MultiIndex[T, K]) stage(int, Seq)  {}
func (ix *MultiIndex[T, K]) endBatch()       {}
func (ix *MultiIndex[T, K]) noteDropped(int) {}

func (ix *MultiIndex[T, K]) commit(slot int, seq Seq, pos int) {
	if ix.pdec[pos].kind != decInsert {
		var zero K
		ix.keys[slot] = zero
		ix.indexed.unset(slot)
		return
	}
	k := ix.pkeys[pos]
	p := ix.m[k]
	if p == nil {
		p = ix.newPostings()
		ix.m[k] = p
	}
	p.push(seq)
	ix.keys[slot] = k
	ix.indexed.set(slot)
}

func (ix *MultiIndex[T, K]) newPostings() *postings {
	if n := len(ix.free); n > 0 {
		p := ix.free[n-1]
		ix.free = ix.free[:n-1]
		return p
	}
	return &postings{}
}

func (ix *MultiIndex[T, K]) evict(slot int, seq Seq) {
	if ix.indexed.get(slot) {
		k := ix.keys[slot]
		p := ix.m[k]
		if debugAssert {
			assertf(p != nil && p.len() > 0 && p.front() == seq, "multi %q: evict seq %d is not the front", ix.name, seq)
		}
		p.popFront()
		if p.len() == 0 {
			delete(ix.m, k)
			p.seqs, p.head = p.seqs[:0], 0
			ix.free = append(ix.free, p)
		}
	}
	var zero K
	ix.keys[slot] = zero
	ix.indexed.unset(slot)
}

func (ix *MultiIndex[T, K]) reset() {
	clear(ix.m)
	clear(ix.keys)
	clear(ix.free)
	ix.free = ix.free[:0]
	ix.indexed.clearAll()
}

func (ix *MultiIndex[T, K]) stats() IndexStats {
	return IndexStats{Name: ix.name, Keys: len(ix.m)}
}

// Get appends to dst the records with key k in the requested Seq order, up to
// min(limit, MaxReadBatch). Status is OK if any record was returned, otherwise
// NotFound; an unbound handle or an unknown Order returns StatusInvalid.
func (ix *MultiIndex[T, K]) Get(dst []Entry[T], k K, limit int, o Order) ([]Entry[T], ReadInfo) {
	if !o.valid() {
		return dst, ReadInfo{Status: StatusInvalid}
	}
	s := ix.lockedSeries()
	if s == nil {
		return dst, ReadInfo{Status: StatusInvalid}
	}
	defer s.mu.Unlock()
	info := ReadInfo{Status: StatusNotFound, Oldest: s.b.oldestRef(s.gen)}
	p := ix.m[k]
	if p == nil {
		return dst, info
	}
	live := p.live()
	n := min(len(live), s.effectiveLimit(limit))
	base := len(dst)
	dst = growEntries(dst, n)
	for i := range n {
		seq := live[i]
		if o == Descending {
			seq = live[len(live)-1-i]
		}
		s.copyOut(&dst[base+i], seq)
	}
	info.Status, info.N, info.Truncated = StatusOK, n, len(live) > n
	info.LastRef = dst[base+n-1].Meta.Ref
	return dst, info
}

// Latest returns the newest record with key k.
func (ix *MultiIndex[T, K]) Latest(k K) (Entry[T], Status) {
	var e Entry[T]
	s := ix.lockedSeries()
	if s == nil {
		return e, StatusInvalid
	}
	defer s.mu.Unlock()
	p := ix.m[k]
	if p == nil {
		return e, StatusNotFound
	}
	s.copyOut(&e, p.back())
	return e, StatusOK
}

// Count returns the number of retained records with key k.
func (ix *MultiIndex[T, K]) Count(k K) int {
	s := ix.lockedSeries()
	if s == nil {
		return 0
	}
	defer s.mu.Unlock()
	if p := ix.m[k]; p != nil {
		return p.len()
	}
	return 0
}

// Keys returns the number of distinct keys.
func (ix *MultiIndex[T, K]) Keys() int {
	s := ix.lockedSeries()
	if s == nil {
		return 0
	}
	defer s.mu.Unlock()
	return len(ix.m)
}

// checkLocked verifies the §14.1 multi-index invariants.
func (ix *MultiIndex[T, K]) checkLocked() error {
	s := ix.s
	want := map[K][]Seq{}
	for seq := s.b.oldest; !s.b.empty() && seq <= s.b.newest; seq++ {
		if slot := s.ring.slot(seq); ix.indexed.get(slot) {
			want[ix.keys[slot]] = append(want[ix.keys[slot]], seq)
		}
	}
	if len(want) != len(ix.m) {
		return fmt.Errorf("multi %q: %d keys, %d expected", ix.name, len(ix.m), len(want))
	}
	for k, p := range ix.m {
		live := p.live()
		if len(live) == 0 {
			return fmt.Errorf("multi %q: empty posting list for %v", ix.name, k)
		}
		w := want[k]
		if len(w) != len(live) {
			return fmt.Errorf("multi %q: key %v postings %v, want %v", ix.name, k, live, w)
		}
		for i := range w {
			if w[i] != live[i] {
				return fmt.Errorf("multi %q: key %v postings %v, want %v", ix.name, k, live, w)
			}
		}
	}
	if n := countUnretainedIndexed(s, ix.indexed); n > 0 {
		return fmt.Errorf("multi %q: %d non-retained slots marked indexed", ix.name, n)
	}
	return nil
}
