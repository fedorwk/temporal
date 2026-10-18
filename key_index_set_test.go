package temporal

import (
	"errors"
	"testing"
)

// The key-index set's protocol and decision combining, tested without a
// Series: the indexes are bound to a bare ring slot map.

type keysFixture struct {
	t *testing.T
	x keyIndexes[trade]
}

func newKeysFixture(t *testing.T, list ...Index[trade]) *keysFixture {
	r := newMetaRing(8)
	f := &keysFixture{t: t, x: newKeyIndexes(list)}
	f.x.bind(nil, &r, 8)
	return f
}

func (f *keysFixture) prepare(id int, sym string, seq, evictBelow Seq, pos int) (keyPlan, error) {
	v := trade{ID: id, Symbol: sym}
	m := at(Timestamp(seq))
	var kp keyPlan
	err := f.x.prepare(&v, &m, 0, Ref{Gen: 1, Seq: seq}, evictBelow, pos, &kp)
	return kp, err
}

// store prepares and commits a row at seq (slot seq-1), outside any batch.
func (f *keysFixture) store(id int, sym string, seq Seq) {
	f.t.Helper()
	kp, err := f.prepare(id, sym, seq, 0, 0)
	if err != nil || kp.drop {
		f.t.Fatalf("store %d %q: %+v %v", id, sym, kp, err)
	}
	f.x.commit(int(seq-1), seq, 0)
}

// want checks a plan; by is 1 + the position of the index that reported the
// holder.
func (f *keysFixture) want(got keyPlan, err error, want keyPlan) {
	f.t.Helper()
	if err != nil || got != want {
		f.t.Fatalf("plan %+v err %v, want %+v", got, err, want)
	}
}

// S6-05: Drop wins, and the holder comes from the first index that decided
// Drop, else from the first that decided Replace or KeepFirst.
func TestKeyIndexes_Precedence(t *testing.T) {
	ids := NewUniqueIndex("id", byID, ConflictReplace)
	syms := NewUniqueIndex("sym", bySymbol, ConflictDrop)
	keep := NewUniqueIndex("keep", byID, ConflictKeepFirst)
	multi := NewMultiIndex("multi", bySymbol)
	f := newKeysFixture(t, multi, ids, syms, keep)
	f.store(1, "X", 1)
	f.store(2, "Y", 2)

	kp, err := f.prepare(3, "Z", 3, 0, 0)
	f.want(kp, err, keyPlan{})
	kp, err = f.prepare(1, "Z", 3, 0, 0) // ids Replace, keep KeepFirst: first wins
	f.want(kp, err, keyPlan{existing: 1, by: 2})
	kp, err = f.prepare(1, "Y", 3, 0, 0) // syms Drop beats an earlier Replace
	f.want(kp, err, keyPlan{drop: true, existing: 2, by: 3})
}

func TestKeyIndexes_RejectFailsTheItem(t *testing.T) {
	drop := NewUniqueIndex("drop", bySymbol, ConflictDrop)
	reject := NewUniqueIndex("reject", byID, ConflictReject)
	f := newKeysFixture(t, drop, reject)
	f.store(1, "X", 1)
	_, err := f.prepare(1, "X", 2, 0, 4)
	var ce *ConflictError
	if !errors.As(err, &ce) || ce.Index != "reject" || ce.BatchPos != 4 || ce.Existing.Seq != 1 {
		t.Fatalf("got %v, want a ConflictError from reject at pos 4", err)
	}
	if *f.x.xe != (Entry[trade]{}) {
		t.Fatal("extractor scratch entry not cleared after an error")
	}
}

func TestKeyIndexes_EvictBelowFreesKeys(t *testing.T) {
	f := newKeysFixture(t, NewUniqueIndex("id", byID, ConflictDrop))
	f.store(1, "", 1)
	kp, err := f.prepare(1, "", 2, 1, 0)
	f.want(kp, err, keyPlan{drop: true, existing: 1, by: 1})
	kp, err = f.prepare(1, "", 2, 2, 0) // the transaction evicts seq 1
	f.want(kp, err, keyPlan{})
}

// P-02: inside a batch, kept items claim their keys for later items; dropped
// items claim nothing; endBatch forgets the claims.
func TestKeyIndexes_BatchOverlay(t *testing.T) {
	ids := NewUniqueIndex("id", byID, ConflictDrop)
	syms := NewUniqueIndex("sym", bySymbol, ConflictDrop)
	f := newKeysFixture(t, ids, syms)
	f.store(9, "X", 1)

	f.x.beginBatch()
	kp, err := f.prepare(5, "A", 2, 0, 0) // kept: claims 5 and A
	f.want(kp, err, keyPlan{})
	kp, err = f.prepare(5, "B", 3, 0, 1)
	f.want(kp, err, keyPlan{drop: true, existing: 2, by: 1})
	kp, err = f.prepare(7, "X", 3, 0, 2) // dropped by sym: claims nothing
	f.want(kp, err, keyPlan{drop: true, existing: 1, by: 2})
	kp, err = f.prepare(7, "C", 3, 0, 3)
	f.want(kp, err, keyPlan{})
	f.x.endBatch()

	kp, err = f.prepare(5, "A", 2, 0, 0) // nothing was committed
	f.want(kp, err, keyPlan{})
}

func TestKeyIndexes_DropCountsTheDecidingIndexes(t *testing.T) {
	ids := NewUniqueIndex("id", byID, ConflictDrop)
	syms := NewUniqueIndex("sym", bySymbol, ConflictDrop)
	other := NewUniqueIndex("other", byID, ConflictReplace)
	f := newKeysFixture(t, ids, syms, other, NewMultiIndex("multi", bySymbol))
	f.store(1, "X", 1)
	f.store(2, "Y", 2)
	if kp, err := f.prepare(1, "Z", 3, 0, 0); err != nil || !kp.drop {
		t.Fatalf("%+v %v", kp, err)
	}
	f.x.drop(0)
	st := f.x.stats()
	if st[0].Dropped != 1 || st[1].Dropped != 0 || st[2].Dropped != 0 || st[3].Dropped != 0 {
		t.Fatalf("dropped counts %d %d %d %d", st[0].Dropped, st[1].Dropped, st[2].Dropped, st[3].Dropped)
	}
	if st[0].Keys != 2 || st[3].Keys != 2 {
		t.Fatalf("a drop changed the indexes: %+v", st)
	}
}

func TestKeyIndexes_CommitEvictReset(t *testing.T) {
	ids := NewUniqueIndex("id", byID, ConflictReplace)
	multi := NewMultiIndex("sym", bySymbol)
	f := newKeysFixture(t, ids, multi)
	f.store(1, "X", 1)
	f.store(2, "X", 2)
	if kp, err := f.prepare(1, "X", 3, 0, 0); err != nil || kp.by != 1 || kp.existing != 1 {
		t.Fatalf("%+v %v", kp, err)
	}
	f.x.commit(2, 3, 0) // replace: the key moves to seq 3
	if ids.m[1] != 3 || ids.indexed.get(0) {
		t.Fatalf("replace: map %v, old row still indexed %v", ids.m, ids.indexed.get(0))
	}
	f.x.evict(0, 1) // the replaced row: no mapping to remove
	f.x.evict(1, 2)
	if len(ids.m) != 1 || multi.m["X"].len() != 1 { // X was posted at seqs 1-3
		t.Fatalf("after evict: ids %v, multi X %d", ids.m, multi.m["X"].len())
	}
	f.x.reset()
	if st := f.x.stats(); st[0].Keys != 0 || st[1].Keys != 0 {
		t.Fatalf("after reset: %+v", st)
	}
}

// §14.1: check verifies the indexes against the retained rows it is given.
func TestKeyIndexes_Check(t *testing.T) {
	ids := NewUniqueIndex("id", byID, ConflictDrop)
	multi := NewMultiIndex("sym", bySymbol)
	f := newKeysFixture(t, ids, multi)
	for seq := Seq(1); seq <= 3; seq++ {
		f.store(int(seq), "X", seq)
	}
	if err := f.x.check(bounds{oldest: 1, newest: 3}); err != nil {
		t.Fatal(err)
	}
	if err := f.x.check(bounds{oldest: 2, newest: 3}); err == nil {
		t.Fatal("seq 1 is indexed but not retained: want an error")
	}
	f.x.evict(0, 1)
	if err := f.x.check(bounds{oldest: 2, newest: 3}); err != nil {
		t.Fatal(err)
	}
	if err := f.x.check(bounds{}); err == nil {
		t.Fatal("rows indexed in an empty series: want an error")
	}
	multi.indexed.set(0) // a non-retained slot marked indexed
	if err := f.x.check(bounds{oldest: 2, newest: 3}); err == nil {
		t.Fatal("corrupt multi index: want an error")
	}
}

func TestKeyIndexes_None(t *testing.T) {
	if x := newKeyIndexes[trade](nil); !x.none() {
		t.Fatal("no indexes must give an empty set")
	}
	if x := newKeyIndexes([]Index[trade]{NewMultiIndex("m", bySymbol)}); x.none() || len(x.conflicts) != 0 {
		t.Fatal("a multi index is a member without conflicts")
	}
}
