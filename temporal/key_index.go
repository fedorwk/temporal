package temporal

// Index is a key index registered with a Series through Config.Indexes. Its
// methods are unexported, so only this package can implement it. Use
// NewUniqueIndex or NewMultiIndex to create one.
type Index[T any] interface {
	indexName() string
}
