//go:build temporaldebug

package temporal

import "fmt"

// debugAssert enables internal consistency assertions (build tag temporaldebug).
const debugAssert = true

// assertf panics with a formatted message when cond is false. Guard call sites
// with `if debugAssert { ... }` so that arguments are not evaluated, and not
// boxed into interfaces, in normal builds.
func assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf("temporal: assertion failed: "+format, args...))
	}
}
