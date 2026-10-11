//go:build !temporaldebug

package temporal

// debugAssert enables internal consistency assertions (build tag temporaldebug).
const debugAssert = false

// assertf is a no-op in normal builds. Guard call sites with
// `if debugAssert { ... }` so that arguments are not evaluated.
func assertf(bool, string, ...any) {}
