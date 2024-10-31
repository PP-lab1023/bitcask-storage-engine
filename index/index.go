package index

import (
	"bitcask-go/data"
	"bytes"

	"github.com/google/btree"
)

// Abstract indexer interface
// set it as interface to use various data structure
type Indexer interface {
	// Put a key-value into keydir
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get a value from key in keydir
	Get(key []byte) *data.LogRecordPos

	// Delete a key-value from keydir
	Delete(key []byte) bool

	// Amount of data in the indexer
	Size() int

	// Get iterator
	Iterator(reverse bool) Iterator
}

type IndexType = int8
const (
	// BTree index
	Btree IndexType = iota + 1

	// Adaptive radix tree
	ART
)

// Initialize index according to the IndexType
func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		// todo
		return nil
	default:
		panic("unsupported index type")
	}
}

// Type Item in BTree is an interface
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

// Implement Less method in Item in BTree
func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1 // bi.(*Item) is an assertion
}

// Universal index iterator (for btree or other data structure)
type Iterator interface {
	// Return to the beginning of the iterator ie. the first data
	Rewind()

	// According to the parameter key, find the first key which is bigger or smaller than it
	// Start iterate from here
	Seek(key []byte)

	// Go to the next key
	Next()

	// Iterate over all the keys?
	Valid() bool

	// Key of the current iteration place
	Key() []byte

	// Value of the current iteration place
	Value() *data.LogRecordPos

	// Close iterator, release related resources
	Close()
}