package temporal

import (
	"hash/fnv"
	"math/rand/v2"
	"testing"
)

// byteSource is a rand.Source that replays the fuzzer's bytes, so mutations of
// the input map to changed decisions (op choice, sizes, timestamps, keys).
// Once the bytes run out it continues with a PCG seeded from them; a source of
// constant zeros would make rand.IntN's rejection loop spin.
type byteSource struct {
	data []byte
	pos  int
	rest *rand.PCG
}

func newByteSource(data []byte) *byteSource {
	h := fnv.New64a()
	h.Write(data)
	seed := h.Sum64()
	return &byteSource{data: data, rest: rand.NewPCG(seed, seed^0x5851f42d4c957f2d)}
}

func (b *byteSource) Uint64() uint64 {
	if b.pos+8 > len(b.data) {
		return b.rest.Uint64()
	}
	var v uint64
	for _, c := range b.data[b.pos : b.pos+8] {
		v = v<<8 | uint64(c)
	}
	b.pos += 8
	return v
}

// FuzzSeries (T-003, T-010) decodes the input into a random scenario
// (capacity 1..8, either mode, 0-2 key indexes with random policies, random
// late tolerance, gating and SourceSeq policy) and an operation sequence
// covering Append, AppendBatch, every Seq, point and batch query, Continue with
// stale, foreign and future cursors, key lookups, Subscribe/TryNext/Seek/Close,
// Clear, Close and SetWatermark. Every result is compared with the reference
// model and CheckInvariants runs after each operation.
func FuzzSeries(f *testing.F) {
	seeds := rand.New(rand.NewPCG(7, 11))
	for _, n := range []int{0, 8, 64, 256, 1024, 4096} {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(seeds.Uint64())
		}
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := rand.New(newByteSource(data))
		mode := allModes[r.IntN(len(allModes))]
		steps := 1 + min(len(data)/8, 600)
		runSeqModel(t, r, mode, 0, steps, map[Status]int{}, map[string]int{})
	})
}

// TestModel_LongRuns (T-002) drives long overwrite-heavy runs at every small
// capacity in both modes, so the ring wraps many times per generation.
func TestModel_LongRuns(t *testing.T) {
	r, _ := newRand(t)
	for _, mode := range allModes {
		for run := range iters(40) {
			runSeqModel(t, r, mode, run, 3000, map[Status]int{}, map[string]int{})
		}
	}
}
