package temporal

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Benchmarks for §22. The default set covers the mandatory §22.3 comparison
// and a practical subset of the §22.1 matrix; TEMPORAL_BENCH_FULL=1 enables
// the full matrix (larger capacities, every value size, every disorder level).
//
// Run: ./scripts/bench.sh (benchstat None vs BTree) or
//      go test -run '^$' -bench . ./temporal

var benchFull = os.Getenv("TEMPORAL_BENCH_FULL") == "1"

// Fixed-size values for the T-size dimension.
type (
	val16  [2]uint64
	val32  [4]uint64
	val64  [8]uint64
	val128 [16]uint64
	val256 [32]uint64
)

// tick is a 32-byte trade-like record used by most benchmarks.
type tick struct {
	ID    uint64
	Price int64
	Qty   int64
	Side  uint8
}

// eventTimes generates n event times with the given disorder: the fraction of
// records that arrive late (up to 1000 ns behind), or a reverse burst every
// 1000 records when disorder < 0. ties is the fraction of records sharing the
// previous timestamp.
func eventTimes(n int, disorder, ties float64, seed uint64) []Timestamp {
	r := rand.New(rand.NewPCG(seed, 1))
	out := make([]Timestamp, n)
	now := Timestamp(1_760_000_000_000_000_000)
	for i := range out {
		switch {
		case i > 0 && r.Float64() < ties:
			out[i] = out[i-1]
			continue
		case disorder < 0 && i%1000 >= 900: // adversarial reverse burst
			out[i] = now - Timestamp(i%1000-899)*10
			continue
		case disorder > 0 && r.Float64() < disorder:
			out[i] = now - Timestamp(1+r.IntN(1000))
			continue
		}
		now += Timestamp(1 + r.IntN(100))
		out[i] = now
	}
	return out
}

func modeName(m TimeIndexMode) string {
	if m == TimeIndexBTree {
		return "BTree"
	}
	return "None"
}

func reportRecords(b *testing.B, records int) {
	b.ReportMetric(float64(records)/b.Elapsed().Seconds(), "records/s")
}

// --- §22.3 mandatory comparison -------------------------------------------

func BenchmarkCompare_Append(b *testing.B) {
	for _, mode := range allModes {
		b.Run("mode="+modeName(mode), func(b *testing.B) {
			s := mustNew(b, Config[tick]{Capacity: 1 << 16, TimeIndex: mode})
			times := eventTimes(1<<16, 0.01, 0.01, 1)
			for i := range 1 << 16 { // fill so every append evicts
				s.Append(tick{ID: uint64(i)}, at(times[i]))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.Append(tick{ID: uint64(i)}, at(times[i&(1<<16-1)]))
			}
			reportRecords(b, b.N)
		})
	}
}

func BenchmarkCompare_AppendBatch(b *testing.B) {
	for _, mode := range allModes {
		for _, size := range []int{8, 64, 256, 1024} {
			b.Run(fmt.Sprintf("mode=%s/batch=%d", modeName(mode), size), func(b *testing.B) {
				s := mustNew(b, Config[tick]{Capacity: 1 << 16, TimeIndex: mode})
				times := eventTimes(1<<16, 0.01, 0.01, 2)
				items := make([]Input[tick], size)
				dst := make([]AppendResult, 0, size)
				k := 0
				fill := func() {
					for j := range items {
						items[j] = Input[tick]{Value: tick{ID: uint64(k)}, Meta: at(times[k&(1<<16-1)])}
						k++
					}
				}
				for range (1 << 16) / size {
					fill()
					dst, _ = s.AppendBatch(dst[:0], items)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					fill()
					b.StartTimer()
					dst, _ = s.AppendBatch(dst[:0], items)
				}
				reportRecords(b, b.N*size)
			})
		}
	}
}

func BenchmarkCompare_ReadFrom(b *testing.B) {
	for _, mode := range allModes {
		b.Run("mode="+modeName(mode), func(b *testing.B) {
			s := mustNew(b, Config[tick]{Capacity: 1 << 16, TimeIndex: mode})
			times := eventTimes(1<<16, 0.01, 0.01, 3)
			for i := range 1 << 16 {
				s.Append(tick{ID: uint64(i)}, at(times[i]))
			}
			dst := make([]Entry[tick], 0, 256)
			oldest := s.OldestRef()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				from := Ref{Gen: oldest.Gen, Seq: oldest.Seq + Seq((i*256)&(1<<15))}
				dst, _ = s.ReadFrom(dst[:0], from, 256)
			}
			reportRecords(b, b.N*256)
		})
	}
}

func BenchmarkCompare_SubscriptionNext(b *testing.B) {
	for _, mode := range allModes {
		b.Run("mode="+modeName(mode), func(b *testing.B) {
			s := mustNew(b, Config[tick]{Capacity: 1 << 16, TimeIndex: mode})
			times := eventTimes(1<<16, 0.01, 0.01, 4)
			for i := range 1 << 16 {
				s.Append(tick{ID: uint64(i)}, at(times[i]))
			}
			sub := mustSubscribe(b, s, SubscribeOptions{Start: StartOldest()})
			dst := make([]Entry[tick], 0, 256)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if i%((1<<16)/256) == 0 {
					b.StopTimer()
					sub.Seek(StartOldest())
					b.StartTimer()
				}
				dst, _ = sub.Next(ctx, dst[:0], 256)
			}
			reportRecords(b, b.N*256)
		})
	}
}

func BenchmarkCompare_AsOf(b *testing.B) {
	for _, mode := range allModes {
		b.Run("mode="+modeName(mode), func(b *testing.B) {
			s := mustNew(b, Config[tick]{Capacity: 1 << 16, TimeIndex: mode})
			times := eventTimes(1<<16, 0.01, 0.01, 5)
			for i := range 1 << 16 {
				s.Append(tick{ID: uint64(i)}, at(times[i]))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.AsOf(times[(i*7919)&(1<<16-1)])
			}
			reportRecords(b, b.N)
		})
	}
}

func BenchmarkCompare_Range(b *testing.B) {
	for _, mode := range allModes {
		b.Run("mode="+modeName(mode), func(b *testing.B) {
			s := mustNew(b, Config[tick]{Capacity: 1 << 16, TimeIndex: mode})
			times := eventTimes(1<<16, 0.01, 0.01, 6)
			for i := range 1 << 16 {
				s.Append(tick{ID: uint64(i)}, at(times[i]))
			}
			sorted := slices.Clone(times)
			slices.Sort(sorted)
			dst := make([]Entry[tick], 0, 256)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				j := (i * 7919) & (1<<15 - 1)
				dst, _ = s.Range(dst[:0], sorted[j], sorted[j+256], 256)
			}
			reportRecords(b, b.N*256)
		})
	}
}

// --- §22.1 matrix (practical subset; full behind TEMPORAL_BENCH_FULL) -----

// BenchmarkMatrix_AppendDisorder measures B-tree ingestion under event-time
// disorder and ties (negative disorder = reverse burst).
func BenchmarkMatrix_AppendDisorder(b *testing.B) {
	disorders := []float64{0, 0.01, 0.1, -1}
	ties := []float64{0, 0.1}
	if benchFull {
		disorders = []float64{0, 0.001, 0.01, 0.1, -1}
		ties = []float64{0, 0.01, 0.1, 1}
	}
	for _, d := range disorders {
		for _, tie := range ties {
			b.Run(fmt.Sprintf("disorder=%g/ties=%g", d, tie), func(b *testing.B) {
				s := mustNew(b, Config[tick]{Capacity: 1 << 16, TimeIndex: TimeIndexBTree})
				times := eventTimes(1<<17, d, tie, 7)
				for i := range 1 << 16 {
					s.Append(tick{}, at(times[i]))
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					s.Append(tick{}, at(times[(1<<16+i)&(1<<17-1)]))
				}
				reportRecords(b, b.N)
			})
		}
	}
}

func benchAppendSize[T any](b *testing.B, mode TimeIndexMode, capacity int, v T) {
	s := mustNew(b, Config[T]{Capacity: capacity, TimeIndex: mode})
	times := eventTimes(1<<16, 0.01, 0, 8)
	for i := range capacity {
		s.Append(v, at(times[i&(1<<16-1)]))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Append(v, at(times[i&(1<<16-1)]))
	}
	reportRecords(b, b.N)
}

// BenchmarkMatrix_AppendValueSize covers the T-size and capacity dimensions,
// including a non-power-of-two capacity.
func BenchmarkMatrix_AppendValueSize(b *testing.B) {
	capacities := []int{1 << 10, 1 << 16, 100_000}
	if benchFull {
		capacities = append(capacities, 1<<20, 10_000_000)
	}
	for _, mode := range allModes {
		for _, capacity := range capacities {
			prefix := fmt.Sprintf("mode=%s/cap=%d", modeName(mode), capacity)
			b.Run(prefix+"/T=16B", func(b *testing.B) { benchAppendSize(b, mode, capacity, val16{}) })
			b.Run(prefix+"/T=64B", func(b *testing.B) { benchAppendSize(b, mode, capacity, val64{}) })
			b.Run(prefix+"/T=256B", func(b *testing.B) { benchAppendSize(b, mode, capacity, val256{}) })
			if benchFull {
				b.Run(prefix+"/T=32B", func(b *testing.B) { benchAppendSize(b, mode, capacity, val32{}) })
				b.Run(prefix+"/T=128B", func(b *testing.B) { benchAppendSize(b, mode, capacity, val128{}) })
			}
		}
	}
}

// BenchmarkMatrix_AppendIndexes covers the key-index dimension with high- and
// low-cardinality keys.
func BenchmarkMatrix_AppendIndexes(b *testing.B) {
	type setup struct {
		name string
		idx  func() []Index[tick]
	}
	setups := []setup{
		{"none", func() []Index[tick] { return nil }},
		{"unique", func() []Index[tick] {
			return []Index[tick]{NewUniqueIndex("id", func(e *Entry[tick]) (uint64, bool) { return e.Value.ID, true }, ConflictDrop)}
		}},
		{"unique+multi(low)", func() []Index[tick] {
			return []Index[tick]{
				NewUniqueIndex("id", func(e *Entry[tick]) (uint64, bool) { return e.Value.ID, true }, ConflictDrop),
				NewMultiIndex("side", func(e *Entry[tick]) (uint8, bool) { return e.Value.Side, true }),
			}
		}},
		{"unique+multi(high)", func() []Index[tick] {
			return []Index[tick]{
				NewUniqueIndex("id", func(e *Entry[tick]) (uint64, bool) { return e.Value.ID, true }, ConflictDrop),
				NewMultiIndex("px", func(e *Entry[tick]) (int64, bool) { return e.Value.Price, true }),
			}
		}},
	}
	for _, mode := range allModes {
		for _, st := range setups {
			b.Run(fmt.Sprintf("mode=%s/idx=%s", modeName(mode), st.name), func(b *testing.B) {
				s := mustNew(b, Config[tick]{Capacity: 1 << 16, TimeIndex: mode, Indexes: st.idx()})
				times := eventTimes(1<<16, 0.01, 0.01, 9)
				id := uint64(0)
				next := func() tick {
					id++
					return tick{ID: id, Price: int64(id % 5000), Side: uint8(id % 2)}
				}
				for i := range 1 << 16 {
					s.Append(next(), at(times[i]))
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					s.Append(next(), at(times[i&(1<<16-1)]))
				}
				reportRecords(b, b.N)
			})
		}
	}
}

// BenchmarkMatrix_Queries covers the query dimension at several batch sizes,
// with preallocated and nil dst.
func BenchmarkMatrix_Queries(b *testing.B) {
	u := NewUniqueIndex("id", func(e *Entry[tick]) (uint64, bool) { return e.Value.ID, true }, ConflictReject)
	mi := NewMultiIndex("side", func(e *Entry[tick]) (uint8, bool) { return e.Value.Side, true })
	s := mustNew(b, Config[tick]{Capacity: 1 << 16, TimeIndex: TimeIndexBTree, Indexes: []Index[tick]{u, mi}})
	times := eventTimes(1<<16, 0.01, 0.01, 10)
	for i := range 1 << 16 {
		s.Append(tick{ID: uint64(i + 1), Side: uint8(i % 2)}, at(times[i]))
	}
	sorted := slices.Clone(times)
	slices.Sort(sorted)
	pick := func(i int) Timestamp { return sorted[(i*7919)&(1<<15-1)+1000] }
	newest := s.NewestRef()

	point := map[string]func(i int){
		"Get":       func(i int) { s.Get(Ref{Gen: newest.Gen, Seq: newest.Seq - Seq(i&1023)}) },
		"Latest":    func(int) { s.Latest() },
		"AsOf":      func(i int) { s.AsOf(pick(i)) },
		"Before":    func(i int) { s.Before(pick(i)) },
		"After":     func(i int) { s.After(pick(i)) },
		"UniqueGet": func(i int) { u.Get(uint64(1 + (i*7919)&(1<<16-1))) },
	}
	for name, fn := range point {
		b.Run("point/"+name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				fn(i)
			}
		})
	}

	sizes := []int{1, 8, 64, 256, 4096}
	if benchFull {
		sizes = []int{1, 8, 64, 256, 1024, 4096}
	}
	for _, n := range sizes {
		for _, prealloc := range []bool{true, false} {
			var dst []Entry[tick]
			if prealloc {
				dst = make([]Entry[tick], 0, n)
			}
			reset := func() []Entry[tick] {
				if prealloc {
					return dst[:0]
				}
				return nil
			}
			batch := map[string]func(i int){
				"ReadFrom": func(i int) { dst, _ = s.ReadFrom(reset(), Ref{Gen: newest.Gen, Seq: newest.Seq - Seq(n)}, n) },
				"Range":    func(i int) { dst, _ = s.Range(reset(), pick(i), MaxTimestamp-1, n) },
				"PrevN":    func(i int) { dst, _ = s.PrevN(reset(), pick(i), n) },
				"NextN":    func(i int) { dst, _ = s.NextN(reset(), pick(i), n) },
				"MultiGet": func(i int) { dst, _ = mi.Get(reset(), uint8(i%2), n, Descending) },
			}
			for name, fn := range batch {
				b.Run(fmt.Sprintf("batch/%s/n=%d/prealloc=%v", name, n, prealloc), func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						fn(i)
					}
					reportRecords(b, b.N*n)
				})
			}
		}
	}

	b.Run("scan/Entries/16K", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			seq, _ := s.Entries(sorted[0], sorted[1<<14])
			for range seq {
			}
		}
		reportRecords(b, b.N*(1<<14))
	})
}

// BenchmarkMatrix_ContendedAppend measures multi-goroutine Append contention.
func BenchmarkMatrix_ContendedAppend(b *testing.B) {
	for _, mode := range allModes {
		b.Run("mode="+modeName(mode), func(b *testing.B) {
			s := mustNew(b, Config[tick]{Capacity: 1 << 16, TimeIndex: mode})
			var ts atomic.Int64
			ts.Store(1)
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					s.Append(tick{}, at(Timestamp(ts.Add(1))))
				}
			})
			reportRecords(b, b.N)
		})
	}
}

// --- §22.2 write latency under readers ------------------------------------

// BenchmarkWriteLatency reports p50/p99 Append latency (ns) while concurrent
// readers copy batches, by reader count, batch read size and mode.
func BenchmarkWriteLatency(b *testing.B) {
	readerCounts := []int{1, 2, 8, 32}
	readSizes := []int{64, 4096}
	for _, mode := range allModes {
		for _, readers := range readerCounts {
			for _, n := range readSizes {
				b.Run(fmt.Sprintf("mode=%s/readers=%d/read=%d", modeName(mode), readers, n), func(b *testing.B) {
					benchWriteLatency(b, mode, readers, n)
				})
			}
		}
	}
}

func benchWriteLatency(b *testing.B, mode TimeIndexMode, readers, readSize int) {
	s := mustNew(b, Config[tick]{Capacity: 1 << 16, TimeIndex: mode, MaxReadBatch: readSize})
	times := eventTimes(1<<16, 0.01, 0.01, 11)
	for i := range 1 << 16 {
		s.Append(tick{}, at(times[i]))
	}
	var stop atomic.Bool
	var wg sync.WaitGroup
	for r := range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dst := make([]Entry[tick], 0, readSize)
			for i := 0; !stop.Load(); i++ {
				if mode == TimeIndexBTree && r%2 == 1 {
					dst, _ = s.Range(dst[:0], times[i&(1<<15-1)], MaxTimestamp-1, readSize)
				} else {
					dst, _ = s.ReadFrom(dst[:0], Ref{}, readSize)
				}
			}
		}()
	}
	samples := make([]time.Duration, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		s.Append(tick{}, at(times[i&(1<<16-1)]))
		samples[i] = time.Since(start)
	}
	b.StopTimer()
	stop.Store(true)
	wg.Wait()
	slices.Sort(samples)
	b.ReportMetric(float64(samples[len(samples)/2].Nanoseconds()), "p50-ns")
	b.ReportMetric(float64(samples[len(samples)*99/100].Nanoseconds()), "p99-ns")
}

// --- §22.2 memory per retained row ----------------------------------------

// TestMemoryPerRow reports heap bytes per retained row by mode and index
// configuration (run with -v; skipped in -short).
func TestMemoryPerRow(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("memory report: skipped in -short and -race")
	}
	const capacity = 1 << 18
	configs := []struct {
		name string
		cfg  func() Config[tick]
	}{
		{"None", func() Config[tick] { return Config[tick]{Capacity: capacity} }},
		{"BTree", func() Config[tick] { return Config[tick]{Capacity: capacity, TimeIndex: TimeIndexBTree} }},
		{"None+unique", func() Config[tick] {
			return Config[tick]{Capacity: capacity, Indexes: []Index[tick]{
				NewUniqueIndex("id", func(e *Entry[tick]) (uint64, bool) { return e.Value.ID, true }, ConflictReject),
			}}
		}},
		{"None+multi", func() Config[tick] {
			return Config[tick]{Capacity: capacity, Indexes: []Index[tick]{
				NewMultiIndex("px", func(e *Entry[tick]) (int64, bool) { return e.Value.Price, true }),
			}}
		}},
	}
	heap := func() uint64 {
		runtime.GC()
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	t.Logf("value type: 32-byte tick; slot metadata: 32 bytes")
	for _, c := range configs {
		before := heap()
		s := mustNew(t, c.cfg())
		for i := range capacity * 2 {
			s.Append(tick{ID: uint64(i), Price: int64(i % 5000)}, at(Timestamp(i)))
		}
		after := heap()
		t.Logf("%-12s %6.1f bytes/row", c.name, float64(after-before)/capacity)
		runtime.KeepAlive(s)
	}
}

// T-009: sustained ingestion keeps the heap bounded after warm-up.
func TestSustainedIngestionHeapBounded(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("sustained ingestion: skipped in -short and -race")
	}
	ids := NewUniqueIndex("id", func(e *Entry[tick]) (uint64, bool) { return e.Value.ID, true }, ConflictDrop)
	s := mustNew(t, Config[tick]{Capacity: 1 << 16, TimeIndex: TimeIndexBTree, Indexes: []Index[tick]{ids}})
	sub := mustSubscribe(t, s, SubscribeOptions{Start: StartOldest(), OnLag: LagSkip})
	times := eventTimes(1<<16, 0.01, 0.01, 12)
	buf := make([]Entry[tick], 0, 1024)
	inuse := func() uint64 {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapInuse
	}
	ingest := func(n int, base int) {
		for i := range n {
			id := uint64(base + i)
			s.Append(tick{ID: id}, at(times[int(id)&(1<<16-1)]+Timestamp(id)))
			if i%512 == 0 {
				buf, _ = sub.TryNext(buf[:0], 1024)
			}
		}
	}
	ingest(1<<18, 0) // warm-up: fills the ring, index maps and B-tree
	warm := inuse()
	var peak uint64
	for round := range 8 {
		ingest(1<<18, (round+1)<<18)
		peak = max(peak, inuse())
	}
	t.Logf("HeapInuse after warm-up %d KiB, peak over 2M more appends %d KiB", warm>>10, peak>>10)
	if peak > warm+warm/2+8<<20 {
		t.Fatalf("heap grew from %d to %d bytes during steady-state ingestion", warm, peak)
	}
}
