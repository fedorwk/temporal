package temporal

import (
	"slices"
	"sync"
	"sync/atomic"
)

// subscriber is a subscription's notification endpoint.
type subscriber struct {
	wake      chan struct{} // buffer 1: coalesced wake-ups
	done      chan struct{} // closed by Subscription.Close
	wmUpdates bool          // woken by watermark advances; immutable
	closeOnce sync.Once
}

// changeSignal decides who a change wakes. Writers report the change with one
// call after releasing the Series mutex, so nobody is woken under the lock
// (C-005); the calls never block.
//
//	records    records were committed, or Clear  every subscription
//	watermark  the watermark advanced             WaitWatermark callers, subscriptions with WatermarkUpdates
//	closed     the Series closed                  WaitWatermark callers, every subscription
//
// A subscription's wake channel holds one pending wake-up, so wake-ups
// coalesce. WaitWatermark callers share one channel, created by the first
// waiter and closed by the next watermark or closed report.
type changeSignal struct {
	subs atomic.Pointer[[]*subscriber] // copy-on-write; read without a lock

	waiting atomic.Bool   // wmWait is non-nil; lets wakeWaiters skip mu when nobody waits
	mu      sync.Mutex    // guards wmWait
	wmWait  chan struct{} // closed to wake WaitWatermark callers; nil when none wait

	dbgFired atomic.Uint64 // reported changes, counted in temporaldebug builds only
}

// add registers sub. Calls to add and remove must be serialized (the Series
// holds its mutex).
func (c *changeSignal) add(sub *subscriber) {
	var list []*subscriber
	if p := c.subs.Load(); p != nil {
		list = slices.Clone(*p)
	}
	list = append(list, sub)
	c.subs.Store(&list)
}

// remove unregisters sub, serialized like add.
func (c *changeSignal) remove(sub *subscriber) {
	p := c.subs.Load()
	if p == nil {
		return
	}
	list := slices.DeleteFunc(slices.Clone(*p), func(x *subscriber) bool { return x == sub })
	c.subs.Store(&list)
}

// count returns the number of registered subscribers.
func (c *changeSignal) count() int {
	if p := c.subs.Load(); p != nil {
		return len(*p)
	}
	return 0
}

// watermarkWait returns the channel that the next watermark or closed report
// closes. The caller holds the Series mutex and has read the watermark and the
// closed state under it: a writer that changes either afterwards reports only
// after it acquires and releases that mutex, so its report finds this channel.
func (c *changeSignal) watermarkWait() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.wmWait == nil {
		c.wmWait = make(chan struct{})
		c.waiting.Store(true)
	}
	return c.wmWait
}

// records reports committed records or a Clear.
func (c *changeSignal) records() {
	if debugAssert {
		c.dbgFired.Add(1)
	}
	c.wakeSubscribers(false)
}

// watermark reports a watermark advance.
func (c *changeSignal) watermark() {
	if debugAssert {
		c.dbgFired.Add(1)
	}
	c.wakeWaiters()
	c.wakeSubscribers(true)
}

// closed reports that the Series closed.
func (c *changeSignal) closed() {
	if debugAssert {
		c.dbgFired.Add(1)
	}
	c.wakeWaiters()
	c.wakeSubscribers(false)
}

// wakeWaiters wakes every WaitWatermark caller.
func (c *changeSignal) wakeWaiters() {
	if c.waiting.Load() {
		c.takeWaiters()
	}
}

// takeWaiters closes and forgets the WaitWatermark channel, if any, and
// clears the waiting flag. The channel is closed after mu is released.
func (c *changeSignal) takeWaiters() {
	c.mu.Lock()
	wake := c.wmWait
	c.wmWait = nil
	c.waiting.Store(false)
	c.mu.Unlock()
	if wake != nil {
		close(wake)
	}
}

// wakeSubscribers wakes every subscription, or with wmOnly only those with
// WatermarkUpdates. A full wake buffer means a wake-up is already pending.
func (c *changeSignal) wakeSubscribers(wmOnly bool) {
	p := c.subs.Load()
	if p == nil {
		return
	}
	for _, sub := range *p {
		if wmOnly && !sub.wmUpdates {
			continue
		}
		select {
		case sub.wake <- struct{}{}:
		default:
		}
	}
}
