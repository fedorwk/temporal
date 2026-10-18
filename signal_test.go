package temporal

import "testing"

// Who each change wakes, and coalescing, tested on the module alone.

func newTestSubscriber(wmUpdates bool) *subscriber {
	return &subscriber{wake: make(chan struct{}, 1), done: make(chan struct{}), wmUpdates: wmUpdates}
}

func woken(sub *subscriber) bool {
	select {
	case <-sub.wake:
		return true
	default:
		return false
	}
}

func closedChan(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestChangeSignal_WhoWakes(t *testing.T) {
	cases := []struct {
		name               string
		report             func(*changeSignal)
		plain, wm, waiters bool
	}{
		{"records", (*changeSignal).records, true, true, false},
		{"watermark", (*changeSignal).watermark, false, true, true},
		{"closed", (*changeSignal).closed, true, true, true},
	}
	for _, c := range cases {
		var sig changeSignal
		plain, wm := newTestSubscriber(false), newTestSubscriber(true)
		sig.add(plain)
		sig.add(wm)
		wait := sig.watermarkWait()
		c.report(&sig)
		if got := woken(plain); got != c.plain {
			t.Errorf("%s: plain subscriber woken %v, want %v", c.name, got, c.plain)
		}
		if got := woken(wm); got != c.wm {
			t.Errorf("%s: WatermarkUpdates subscriber woken %v, want %v", c.name, got, c.wm)
		}
		if got := closedChan(wait); got != c.waiters {
			t.Errorf("%s: watermark waiters woken %v, want %v", c.name, got, c.waiters)
		}
	}
}

func TestChangeSignal_Coalesces(t *testing.T) {
	var sig changeSignal
	sub := newTestSubscriber(true)
	sig.add(sub)
	sig.records()
	sig.watermark()
	sig.records()
	if !woken(sub) {
		t.Fatal("no wake-up")
	}
	if woken(sub) {
		t.Fatal("wake-ups did not coalesce")
	}
}

func TestChangeSignal_WatermarkWaitChannel(t *testing.T) {
	var sig changeSignal
	sig.watermark() // nobody waits: nothing to close
	a, b := sig.watermarkWait(), sig.watermarkWait()
	if a != b {
		t.Fatal("waiters do not share one channel")
	}
	sig.records()
	if closedChan(a) {
		t.Fatal("records woke watermark waiters")
	}
	sig.watermark()
	if !closedChan(a) {
		t.Fatal("watermark advance did not wake waiters")
	}
	if c := sig.watermarkWait(); c == a || closedChan(c) {
		t.Fatal("a woken channel was reused")
	}
	sig.watermark() // the new channel is closed once, not twice
	sig.closed()
}

func TestChangeSignal_AddRemove(t *testing.T) {
	var sig changeSignal
	a, b := newTestSubscriber(false), newTestSubscriber(false)
	sig.add(a)
	sig.add(b)
	before := sig.subs.Load()
	sig.remove(a)
	if sig.count() != 1 || len(*before) != 2 {
		t.Fatalf("count %d, old list %d: remove must copy", sig.count(), len(*before))
	}
	sig.records()
	if woken(a) || !woken(b) {
		t.Fatal("removed subscriber woken, or remaining one not")
	}
	sig.remove(a) // absent: no-op
	if sig.count() != 1 {
		t.Fatalf("count %d", sig.count())
	}
}
