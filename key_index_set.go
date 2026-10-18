package temporal

// keyIndexes is the Series' key indexes as one unit. It runs the per-index
// write protocol (extract and decide, stage for later items of a batch,
// commit, evict, reset) and combines the per-index decisions (Drop wins,
// S6-05), so the write path calls it once per step instead of once per index.
// Each index keeps its typed per-position scratch (S6-01, P-11); the set never
// sees a key. All methods are called with the Series mutex held.
type keyIndexes[T any] struct {
	list      []Index[T]
	conflicts []conflictIndex // the indexes in list whose keys can conflict (unique)
	xe        *Entry[T]       // handed to extractors (S6-07); zeroed after use; nil until bind
	batch     bool            // between beginBatch and endBatch
}

// conflictIndex is the part of the protocol only an index with conflicting
// keys needs: the overlay of keys claimed by earlier items of a batch (P-02)
// and the count of items it dropped.
type conflictIndex interface {
	beginBatch()
	stage(pos int, seq Seq) // the item at pos was not dropped and will get seq
	endBatch()
	noteDropped(pos int) // count a committed drop if this index decided it
}

// keyPlan is the combined prepare decision of every index for one item. The
// zero keyPlan keeps the item and reports no holder.
type keyPlan struct {
	existing Seq   // holder reported in AppendResult.Existing
	by       int32 // 1 + the position of the index that produced existing; 0 for none
	drop     bool  // an index decided Drop
}

func newKeyIndexes[T any](list []Index[T]) keyIndexes[T] {
	x := keyIndexes[T]{list: append([]Index[T](nil), list...)}
	for _, ix := range x.list {
		if c, ok := ix.(conflictIndex); ok {
			x.conflicts = append(x.conflicts, c)
		}
	}
	return x
}

// none reports whether the Series has no key indexes.
func (x *keyIndexes[T]) none() bool { return len(x.list) == 0 }

// bind attaches every index to s. slots maps a Seq to its ring slot.
func (x *keyIndexes[T]) bind(s *Series[T], slots *metaRing, capacity int) {
	if x.none() {
		return
	}
	x.xe = new(Entry[T])
	for _, ix := range x.list {
		ix.bind(s, slots, capacity)
	}
}

// prepare runs P-03 step 6 for the item at batch position pos, which will be
// stored as ref with the series-set flags: each index in Config.Indexes order
// extracts the key and decides, without changing what is committed. Rows below
// evictBelow count as evicted. Any error fails the item; otherwise any Drop
// drops it. The combined decision goes to kp: the reported holder comes from
// the first index that decided Drop, else from the first that decided Replace
// or KeepFirst (S6-05). Inside a batch, a kept item's keys are staged so that
// later items see them. A panic in an extractor propagates; nothing has
// changed then.
func (x *keyIndexes[T]) prepare(v *T, m *InputMeta, flags Flags, ref Ref, evictBelow Seq, pos int, kp *keyPlan) error {
	e := x.xe
	e.Value = *v
	e.Meta = m.meta(flags, ref)
	defer func() { *e = Entry[T]{} }()

	*kp = keyPlan{}
	var dropBy int32
	var dropHolder Seq
	for i, ix := range x.list {
		by := int32(i) + 1
		d, err := ix.prepare(e, evictBelow, pos)
		if err != nil {
			return err
		}
		switch d.kind {
		case decDrop:
			if dropBy == 0 {
				dropBy, dropHolder = by, d.existing
			}
		case decReplace, decKeepFirst:
			if kp.by == 0 {
				kp.by, kp.existing = by, d.existing
			}
		}
	}
	if dropBy != 0 {
		*kp = keyPlan{drop: true, existing: dropHolder, by: dropBy}
		return nil
	}
	if x.batch {
		for _, c := range x.conflicts {
			c.stage(pos, ref.Seq)
		}
	}
	return nil
}

// beginBatch and endBatch bracket AppendBatch's prepare phase: keys of kept
// items stay visible to later items until endBatch.
func (x *keyIndexes[T]) beginBatch() {
	x.batch = true
	for _, c := range x.conflicts {
		c.beginBatch()
	}
}

func (x *keyIndexes[T]) endBatch() {
	x.batch = false
	for _, c := range x.conflicts {
		c.endBatch()
	}
}

// commit applies the prepared decisions for pos to the row stored at slot.
func (x *keyIndexes[T]) commit(slot int, seq Seq, pos int) {
	for _, ix := range x.list {
		ix.commit(slot, seq, pos)
	}
}

// drop records that the prepared item at pos was dropped.
func (x *keyIndexes[T]) drop(pos int) {
	for _, c := range x.conflicts {
		c.noteDropped(pos)
	}
}

// evict removes the row seq, stored at slot, from every index.
func (x *keyIndexes[T]) evict(slot int, seq Seq) {
	for _, ix := range x.list {
		ix.evict(slot, seq)
	}
}

// reset empties every index (Clear).
func (x *keyIndexes[T]) reset() {
	for _, ix := range x.list {
		ix.reset()
	}
}

// name returns the name of the index a keyPlan's by field names.
func (x *keyIndexes[T]) name(by int32) string { return x.list[by-1].indexName() }

// stats returns per-index statistics in Config order.
func (x *keyIndexes[T]) stats() []IndexStats {
	st := make([]IndexStats, len(x.list))
	for i, ix := range x.list {
		st[i] = ix.stats()
	}
	return st
}

// check verifies every index's invariants.
func (x *keyIndexes[T]) check() error {
	for _, ix := range x.list {
		if err := ix.checkLocked(); err != nil {
			return err
		}
	}
	return nil
}
