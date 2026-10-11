package temporal

import (
	"flag"
	"math/rand/v2"
	"runtime"
	"testing"
	"time"
)

var (
	itersFlag = flag.Int("temporal.iters", 0, "iterations for randomized tests (0 = per-test default)")
	seedFlag  = flag.Uint64("temporal.seed", 0, "seed for randomized tests (0 = time-based)")
)

// iters scales a randomized test's default iteration count: -temporal.iters
// overrides it; -short and the race detector reduce it.
func iters(def int) int {
	if *itersFlag > 0 {
		return *itersFlag
	}
	if testing.Short() {
		def /= 10
	}
	if raceEnabled {
		def /= 4
	}
	return max(def, 1)
}

// newRand returns a deterministic generator and logs its seed so a failing run
// can be reproduced with -temporal.seed.
func newRand(t testing.TB) (*rand.Rand, uint64) {
	t.Helper()
	seed := *seedFlag
	if seed == 0 {
		seed = uint64(time.Now().UnixNano())
	}
	t.Logf("seed %d (rerun with -temporal.seed=%d)", seed, seed)
	return rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), seed
}

// mustNew creates a Series or fails the test.
func mustNew[T any](t testing.TB, cfg Config[T]) *Series[T] {
	t.Helper()
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// mustAppend appends or fails the test.
func mustAppend[T any](t testing.TB, s *Series[T], v T, m InputMeta) AppendResult {
	t.Helper()
	res, err := s.Append(v, m)
	if err != nil {
		t.Fatalf("Append(%v, %+v): %v", v, m, err)
	}
	return res
}

// mustInvariants fails the test if CheckInvariants reports a violation.
func mustInvariants[T any](t testing.TB, s *Series[T]) {
	t.Helper()
	if err := s.CheckInvariants(); err != nil {
		t.Fatalf("CheckInvariants: %v", err)
	}
}

// at is shorthand for an InputMeta with only EventTime set.
func at(ts Timestamp) InputMeta { return InputMeta{EventTime: ts} }

// allModes lists every TimeIndexMode for mode-independence tests.
var allModes = []TimeIndexMode{TimeIndexNone, TimeIndexBTree}

// skipAllocTest skips allocation assertions where counts are not meaningful:
// under the race detector and in temporaldebug builds (assertions box args).
func skipAllocTest(t *testing.T) {
	t.Helper()
	if raceEnabled || debugAssert {
		t.Skip("allocation counts are not meaningful under -race or temporaldebug")
	}
}

// allocsPerOp runs f once to warm up, then measures runs calls, and returns
// the exact average number of heap allocations per call. Unlike
// testing.AllocsPerRun it does not truncate, so one allocation per call shows.
// It reports the minimum of three measurements: an allocation made by f
// appears in every measurement, while a stray runtime allocation does not.
func allocsPerOp(runs int, f func()) float64 {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	f()
	best := -1.0
	for range 3 {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for range runs {
			f()
		}
		runtime.ReadMemStats(&after)
		n := float64(after.Mallocs-before.Mallocs) / float64(runs)
		if best < 0 || n < best {
			best = n
		}
	}
	return best
}

// panicValue runs f and returns the value it panicked with, or nil.
func panicValue(f func()) (v any) {
	defer func() { v = recover() }()
	f()
	return nil
}
