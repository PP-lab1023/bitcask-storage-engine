package index

import (
	"bitcask-go/data"
	"sync"

	"github.com/google/btree"
)

// BTree index
// encapsulation of BTree storage created by Google
// https://github.com/google/btree
type BTree struct {
	tree *btree.BTree

	// It is said in BTree function:
	// "Write operations are not safe for concurrent mutation by multiple goroutines,
	// but Read operations are."
	// So create a lock for concurrent write
	lock *sync.RWMutex
}

// Initialize BTree
func NewBTree() *BTree {
	return &BTree{
		// Google btree needs an initialezed parameter to control the number of leaves
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock() // Add lock before write
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	return oldItem != nil
}
