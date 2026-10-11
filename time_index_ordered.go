package temporal

// orderedTimeIndex is the timeIndex for TimeIndexOrdered. It stores no keys:
// when EventTime never decreases in Seq order, the retained records in Seq
// order are already sorted by (EventTime, Seq), so every lookup is a binary
// search over the ring's metadata. The index tracks only which Seqs it holds.
//
// Mutations must follow the Series' FIFO pattern: inserts carry the next Seq
// with an EventTime no lower than the previous insert, and deletes remove the
// oldest key. New guarantees the ordering by requiring LateReject with zero
// tolerance; debug builds assert both rules.
type orderedTimeIndex struct {
	r     *metaRing
	first Seq // oldest indexed Seq; meaningful when n > 0
	n     int
	last  timeKey // newest key ever inserted since Clear, for debug checks
}

func newOrderedTimeIndex(r *metaRing) *orderedTimeIndex {
	return &orderedTimeIndex{r: r}
}

// key returns the i-th smallest key, 0 <= i < n.
func (x *orderedTimeIndex) key(i int) timeKey {
	s := x.first + Seq(i)
	return timeKey{eventTime: x.r.eventTime(s), seq: s}
}

func (x *orderedTimeIndex) Set(k timeKey) {
	if debugAssert {
		assertf(x.n == 0 || k.seq == x.first+Seq(x.n), "ordered index: insert %v, want seq %d", k, x.first+Seq(x.n))
		assertf(x.last.seq == 0 || keyLess(x.last, k), "ordered index: insert %v after %v", k, x.last)
		assertf(x.r.eventTime(k.seq) == k.eventTime, "ordered index: insert %v, ring EventTime %d", k, x.r.eventTime(k.seq))
	}
	if x.n == 0 {
		x.first = k.seq
	}
	x.n++
	x.last = k
}

func (x *orderedTimeIndex) Delete(k timeKey) {
	if debugAssert {
		assertf(x.n > 0 && k.seq == x.first, "ordered index: delete %v, oldest seq %d (len %d)", k, x.first, x.n)
	}
	x.first++
	x.n--
}

// Apply moves the interval bounds in O(1); debug builds check every key.
func (x *orderedTimeIndex) Apply(del, ins []timeKey) {
	if debugAssert {
		for _, k := range del {
			x.Delete(k)
		}
		for _, k := range ins {
			x.Set(k)
		}
		return
	}
	x.first += Seq(len(del))
	x.n -= len(del)
	if len(ins) > 0 {
		if x.n == 0 {
			x.first = ins[0].seq
		}
		x.n += len(ins)
		x.last = ins[len(ins)-1]
	}
}

func (x *orderedTimeIndex) Clear() { *x = orderedTimeIndex{r: x.r} }

func (x *orderedTimeIndex) Len() int { return x.n }

func (x *orderedTimeIndex) Min() (timeKey, bool) { return x.at(0) }

func (x *orderedTimeIndex) Max() (timeKey, bool) { return x.at(x.n - 1) }

// at returns key(i), or false when i is out of range.
func (x *orderedTimeIndex) at(i int) (timeKey, bool) {
	if i < 0 || i >= x.n {
		return timeKey{}, false
	}
	return x.key(i), true
}

// The point primitives rely on Seq 0 and seqMax never being assigned, as in
// btreeTimeIndex.

func (x *orderedTimeIndex) AsOf(ts Timestamp) (timeKey, bool) {
	return x.at(x.searchGT(timeKey{eventTime: ts, seq: seqMax}) - 1)
}

func (x *orderedTimeIndex) Before(ts Timestamp) (timeKey, bool) {
	return x.at(x.searchGE(timeKey{eventTime: ts, seq: 0}) - 1)
}

func (x *orderedTimeIndex) After(ts Timestamp) (timeKey, bool) {
	return x.at(x.searchGT(timeKey{eventTime: ts, seq: seqMax}))
}

func (x *orderedTimeIndex) AtOrAfter(ts Timestamp) (timeKey, bool) {
	return x.at(x.searchGE(timeKey{eventTime: ts, seq: 0}))
}

// searchGE returns the position of the smallest key >= k, or n if none.
// Queries near the newest record (the common live case) are answered by the
// first comparison.
func (x *orderedTimeIndex) searchGE(k timeKey) int {
	if x.n == 0 || keyLess(x.key(x.n-1), k) {
		return x.n
	}
	lo, hi := 0, x.n-1 // key(hi) >= k
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if keyLess(x.key(mid), k) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// searchGT returns the position of the smallest key > k, or n if none.
func (x *orderedTimeIndex) searchGT(k timeKey) int {
	if x.n == 0 || !keyLess(k, x.key(x.n-1)) {
		return x.n
	}
	lo, hi := 0, x.n-1 // key(hi) > k
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if keyLess(k, x.key(mid)) {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

func (x *orderedTimeIndex) Ascend(dst []timeKey, start timeKey, to Timestamp, limit int) ([]timeKey, bool) {
	n := 0
	for i := x.searchGE(start); i < x.n; i++ {
		k := x.key(i)
		if k.eventTime >= to {
			break
		}
		if n == limit {
			return dst, true
		}
		dst = append(dst, k)
		n++
	}
	return dst, false
}

func (x *orderedTimeIndex) Descend(dst []timeKey, start timeKey, limit int) ([]timeKey, bool) {
	n := 0
	for i := x.searchGT(start) - 1; i >= 0; i-- {
		if n == limit {
			return dst, true
		}
		dst = append(dst, x.key(i))
		n++
	}
	return dst, false
}
