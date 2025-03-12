package kvdb

// Store represents the data store
type Store struct {
	maxTxnId uint64
	data     map[string]string
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

// Put  ...
func (s *Store) Put(key, value string) {
	s.data[key] = value
}

// Delete ...
func (s *Store) Delete(key string) {
	delete(s.data, key)
}

func (s *Store) Get(key string) (string, bool) {
	value, exists := s.data[key]
	return value, exists
}

func (s *Store) GetMaxTxnId() uint64 {
	return s.maxTxnId
}
func (s *Store) SetMaxTxnId(id uint64) {
	s.maxTxnId = id
}
