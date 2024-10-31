package kvproject

import (
	"bitcask-go/index"
	"bytes"
)

// User-facing iterator
type Iterator struct {
	indexIter index.Iterator
	db *DB
	options IteratorOptions
}

// Initialize iterator
func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		db: db,
		indexIter: indexIter,
		options: opts,
	}
}

// Return to the beginning of the iterator ie. the first data
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

// According to the parameter key, find the first key which is bigger or smaller than it
// Start iterate from here
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

// Go to the next key
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

// Iterate over all the keys?
func (it *Iterator) Valid() bool{
	return it.indexIter.Valid()
}

// Key of the current iteration place
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// Value of the current iteration place
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByPosition(logRecordPos)
}

// Close iterator, release related resources
func (it *Iterator) Close() {
	it.indexIter.Close()
}

// Find the next key with the specified prefix
func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}

	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if prefixLen <= len(key) && bytes.Equal(it.options.Prefix, key[:prefixLen]) {
			break
		}
	}
}
