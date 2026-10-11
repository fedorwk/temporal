package temporal

import (
	"testing"
	"unsafe"
)

// P-16: slot metadata stays at 32 bytes.
func TestSlotMeta_Size(t *testing.T) {
	if got := unsafe.Sizeof(slotMeta{}); got != 32 {
		t.Fatalf("sizeof(slotMeta) = %d, want 32", got)
	}
}

// §12.1, §12.2: the ring and bounds keep exactly the newest Capacity records,
// each in its own slot, and evict in Seq order.
func TestRing_PushRetainEvict(t *testing.T) {
	for _, capacity := range []int{1, 2, 3, 4, 5, 6, 7, 8, 10, 1000, 1024} {
		r := newRing[int](capacity)
		var b bounds
		if !b.empty() || b.len() != 0 || b.nextSeq() != 1 {
			t.Fatalf("cap %d: fresh bounds %+v", capacity, b)
		}

		total := 5 * capacity
		for i := 1; i <= total; i++ {
			s := b.nextSeq()
			if s != Seq(i) {
				t.Fatalf("cap %d: nextSeq = %d, want %d", capacity, s, i)
			}
			prevOldest := b.oldest
			evicted, didEvict := b.push(capacity)
			wantEvict := i > capacity
			if didEvict != wantEvict {
				t.Fatalf("cap %d seq %d: didEvict = %v", capacity, i, didEvict)
			}
			if didEvict && evicted != prevOldest {
				t.Fatalf("cap %d seq %d: evicted %d, previous oldest %d", capacity, i, evicted, prevOldest)
			}
			v := i * 10
			r.put(s, &v, &slotMeta{eventTime: Timestamp(i), flags: FlagGap})

			if want := min(i, capacity); b.len() != want {
				t.Fatalf("cap %d seq %d: len %d, want %d", capacity, i, b.len(), want)
			}
			if b.newest != s || b.oldest != Seq(max(1, i-capacity+1)) {
				t.Fatalf("cap %d seq %d: bounds %+v", capacity, i, b)
			}
		}

		// Every retained Seq maps to a distinct slot and still holds its data.
		used := make(map[int]Seq, capacity)
		for s := b.oldest; s <= b.newest; s++ {
			slot := r.slot(s)
			if prev, dup := used[slot]; dup {
				t.Fatalf("cap %d: seqs %d and %d share slot %d", capacity, prev, s, slot)
			}
			used[slot] = s
			if *r.value(s) != int(s)*10 || r.eventTime(s) != Timestamp(s) || r.meta(s).flags != FlagGap {
				t.Fatalf("cap %d: seq %d holds value %d meta %+v", capacity, s, *r.value(s), *r.meta(s))
			}
		}
		if len(used) != capacity {
			t.Fatalf("cap %d: %d slots used", capacity, len(used))
		}
		if b.retained(b.oldest-1) || b.retained(b.newest+1) || b.retained(0) {
			t.Fatalf("cap %d: retained() true outside bounds %+v", capacity, b)
		}
	}
}

func TestRing_SlotFormula(t *testing.T) {
	for _, capacity := range []int{1, 3, 7, 8, 16, 1000} {
		r := newRing[byte](capacity)
		for s := Seq(1); s <= Seq(4*capacity); s++ {
			if got, want := r.slot(s), int((uint64(s)-1)%uint64(capacity)); got != want {
				t.Fatalf("cap %d: slot(%d) = %d, want %d", capacity, s, got, want)
			}
		}
		if got, want := r.slot(seqMax-1), int((uint64(seqMax-1)-1)%uint64(capacity)); got != want {
			t.Fatalf("cap %d: slot(max-1) = %d, want %d", capacity, got, want)
		}
	}
}

func TestBounds_EmptyAndReset(t *testing.T) {
	var b bounds
	if b.retained(0) || b.retained(1) {
		t.Fatal("empty bounds retain something")
	}
	if !b.oldestRef(1).IsZero() || !b.newestRef(1).IsZero() {
		t.Fatal("empty bounds return non-zero refs")
	}
	b.push(4)
	b.push(4)
	if got := b.oldestRef(2); got != (Ref{Gen: 2, Seq: 1}) {
		t.Fatalf("oldestRef = %+v", got)
	}
	if got := b.newestRef(2); got != (Ref{Gen: 2, Seq: 2}) {
		t.Fatalf("newestRef = %+v", got)
	}
	b.reset()
	if !b.empty() || b.len() != 0 || b.nextSeq() != 1 {
		t.Fatalf("after reset %+v", b)
	}
}

// §12.1: zero() releases references held inside T.
func TestRing_ZeroClearsReferences(t *testing.T) {
	type rec struct {
		p   *int
		buf []byte
	}
	r := newRing[rec](3)
	var b bounds
	for i := range 3 {
		x := i
		s := b.nextSeq()
		b.push(3)
		r.put(s, &rec{p: &x, buf: []byte{1, 2}}, &slotMeta{eventTime: 5})
	}
	r.zero()
	for i := range r.values {
		if r.values[i].p != nil || r.values[i].buf != nil || r.metas[i] != (slotMeta{}) {
			t.Fatalf("slot %d not zeroed: %+v %+v", i, r.values[i], r.metas[i])
		}
	}
}
