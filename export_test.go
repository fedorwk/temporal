package temporal

// testSetNewest moves the next Seq of an empty Series to n+1, for
// ErrSeqExhausted tests. It panics if the Series is not empty.
func (s *Series[T]) testSetNewest(n Seq) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.adm.b.empty() {
		panic("testSetNewest on a non-empty series")
	}
	s.adm.b.newest = n
}

// testSetGen sets the generation, for ErrGenExhausted tests.
func (s *Series[T]) testSetGen(g Gen) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gen = g
}
