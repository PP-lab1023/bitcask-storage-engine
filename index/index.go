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
