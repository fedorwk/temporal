package temporal

import "fmt"

// sliceTimeIndex is the test-only reference timeIndex: a sorted []timeKey with
// linear insert and delete, and every primitive written as a plain linear scan
// over its predicate (no pivot keys).
type sliceTimeIndex struct {
	keys []timeKey
}

var _ timeIndex = (*sliceTimeIndex)(nil)

func (x *sliceTimeIndex) find(k timeKey) int {
	for i, have := range x.keys {
		if have == k {
			return i
		}
	}
	return -1
}

func (x *sliceTimeIndex) Set(k timeKey) {
	if x.find(k) >= 0 {
		panic(fmt.Sprintf("slice index: duplicate key %v", k))
	}
	i := 0
	for i < len(x.keys) && keyLess(x.keys[i], k) {
		i++
	}
	x.keys = append(x.keys, timeKey{})
	copy(x.keys[i+1:], x.keys[i:])
	x.keys[i] = k
}

func (x *sliceTimeIndex) Delete(k timeKey) {
	i := x.find(k)
	if i < 0 {
		panic(fmt.Sprintf("slice index: missing key %v", k))
	}
	x.keys = append(x.keys[:i], x.keys[i+1:]...)
}

func (x *sliceTimeIndex) Apply(del, ins []timeKey) {
	for _, k := range del {
		x.Delete(k)
	}
	for _, k := range ins {
		x.Set(k)
	}
}

func (x *sliceTimeIndex) Clear() { x.keys = nil }

func (x *sliceTimeIndex) Len() int { return len(x.keys) }

// last returns the greatest key satisfying pred, first the smallest.
func (x *sliceTimeIndex) last(pred func(timeKey) bool) (timeKey, bool) {
	for i := len(x.keys) - 1; i >= 0; i-- {
		if pred(x.keys[i]) {
			return x.keys[i], true
		}
	}
	return timeKey{}, false
}

func (x *sliceTimeIndex) first(pred func(timeKey) bool) (timeKey, bool) {
	for _, k := range x.keys {
		if pred(k) {
			return k, true
		}
	}
	return timeKey{}, false
}

func (x *sliceTimeIndex) AsOf(ts Timestamp) (timeKey, bool) {
	return x.last(func(k timeKey) bool { return k.eventTime <= ts })
}

func (x *sliceTimeIndex) Before(ts Timestamp) (timeKey, bool) {
	return x.last(func(k timeKey) bool { return k.eventTime < ts })
}

func (x *sliceTimeIndex) After(ts Timestamp) (timeKey, bool) {
	return x.first(func(k timeKey) bool { return k.eventTime > ts })
}

func (x *sliceTimeIndex) AtOrAfter(ts Timestamp) (timeKey, bool) {
	return x.first(func(k timeKey) bool { return k.eventTime >= ts })
}

func (x *sliceTimeIndex) Min() (timeKey, bool) { return x.first(func(timeKey) bool { return true }) }

func (x *sliceTimeIndex) Max() (timeKey, bool) { return x.last(func(timeKey) bool { return true }) }

func (x *sliceTimeIndex) Ascend(dst []timeKey, start timeKey, to Timestamp, limit int) ([]timeKey, bool) {
	n := 0
	for _, k := range x.keys {
		if keyLess(k, start) || k.eventTime >= to {
			continue
		}
		if n == limit {
			return dst, true
		}
		dst = append(dst, k)
		n++
	}
	return dst, false
}

func (x *sliceTimeIndex) Descend(dst []timeKey, start timeKey, limit int) ([]timeKey, bool) {
	n := 0
	for i := len(x.keys) - 1; i >= 0; i-- {
		k := x.keys[i]
		if keyLess(start, k) {
			continue
		}
		if n == limit {
			return dst, true
		}
		dst = append(dst, k)
		n++
	}
	return dst, false
}
