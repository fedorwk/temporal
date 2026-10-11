package temporal

import "github.com/tidwall/btree"

// btreeTimeIndex is the reference timeIndex: an ordered set of timeKeys in a
// tidwall/btree BTreeG. This is the only file that imports tidwall/btree.
//
// Lookups and scans use one reusable iterator instead of callbacks, so they do
// not allocate closures. Point primitives rely on Seq 0 and math.MaxUint64
// never being assigned: {ts, 0} sorts before and {ts, seqMax} after every
// stored key with EventTime ts.
type btreeTimeIndex struct {
	tr      *btree.BTreeG[timeKey]
	insHint btree.PathHint // inserts cluster at the newest end
	delHint btree.PathHint // evictions cluster at the oldest end
	it      btree.IterG[timeKey]
}

func newBTreeTimeIndex() *btreeTimeIndex {
	return &btreeTimeIndex{
		tr: btree.NewBTreeGOptions(keyLess, btree.Options{NoLocks: true}),
	}
}

func (x *btreeTimeIndex) Set(k timeKey) {
	_, replaced := x.tr.SetHint(k, &x.insHint)
	if debugAssert {
		assertf(!replaced, "time index: inserted key %v already present", k)
	}
}

func (x *btreeTimeIndex) Delete(k timeKey) {
	_, deleted := x.tr.DeleteHint(k, &x.delHint)
	if debugAssert {
		assertf(deleted, "time index: deleted key %v not present", k)
	}
}

func (x *btreeTimeIndex) Apply(del, ins []timeKey) {
	for _, k := range del {
		x.Delete(k)
	}
	for _, k := range ins {
		x.Set(k)
	}
}

func (x *btreeTimeIndex) Clear() {
	x.tr.Clear()
	x.insHint = btree.PathHint{}
	x.delHint = btree.PathHint{}
}

func (x *btreeTimeIndex) Len() int { return x.tr.Len() }

func (x *btreeTimeIndex) Min() (timeKey, bool) { return x.tr.Min() }

func (x *btreeTimeIndex) Max() (timeKey, bool) { return x.tr.Max() }

func (x *btreeTimeIndex) AsOf(ts Timestamp) (timeKey, bool) {
	return x.seekLE(timeKey{eventTime: ts, seq: seqMax})
}

func (x *btreeTimeIndex) Before(ts Timestamp) (timeKey, bool) {
	return x.seekLE(timeKey{eventTime: ts, seq: 0})
}

func (x *btreeTimeIndex) After(ts Timestamp) (timeKey, bool) {
	return x.seekGE(timeKey{eventTime: ts, seq: seqMax})
}

func (x *btreeTimeIndex) AtOrAfter(ts Timestamp) (timeKey, bool) {
	return x.seekGE(timeKey{eventTime: ts, seq: 0})
}

// seekGE returns the smallest key >= k.
func (x *btreeTimeIndex) seekGE(k timeKey) (timeKey, bool) {
	x.it.Init(x.tr, false)
	ok := x.it.Seek(k)
	var r timeKey
	if ok {
		r = x.it.Item()
	}
	x.it.Release()
	return r, ok
}

// seekLE returns the greatest key <= k.
func (x *btreeTimeIndex) seekLE(k timeKey) (timeKey, bool) {
	x.it.Init(x.tr, false)
	ok := x.positionLE(k)
	var r timeKey
	if ok {
		r = x.it.Item()
	}
	x.it.Release()
	return r, ok
}

// positionLE moves the initialized iterator to the greatest key <= k.
func (x *btreeTimeIndex) positionLE(k timeKey) bool {
	if !x.it.Seek(k) {
		return x.it.Last() // every key is < k
	}
	if x.it.Item() == k {
		return true
	}
	return x.it.Prev()
}

func (x *btreeTimeIndex) Ascend(dst []timeKey, start timeKey, to Timestamp, limit int) ([]timeKey, bool) {
	x.it.Init(x.tr, false)
	more := false
	n := 0
	for ok := x.it.Seek(start); ok; ok = x.it.Next() {
		k := x.it.Item()
		if k.eventTime >= to {
			break
		}
		if n == limit {
			more = true
			break
		}
		dst = append(dst, k)
		n++
	}
	x.it.Release()
	return dst, more
}

func (x *btreeTimeIndex) Descend(dst []timeKey, start timeKey, limit int) ([]timeKey, bool) {
	x.it.Init(x.tr, false)
	more := false
	n := 0
	for ok := x.positionLE(start); ok; ok = x.it.Prev() {
		if n == limit {
			more = true
			break
		}
		dst = append(dst, x.it.Item())
		n++
	}
	x.it.Release()
	return dst, more
}
