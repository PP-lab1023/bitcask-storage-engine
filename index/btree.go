package index

import (
	"bitcask-go/data"
	"bytes"
	"sort"
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

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reverse)
}

// BTree iterator
type btreeIterator struct {
	// Iterator creates an array to temporarily save keys
	// currIndex is the place of the iterator in the array
	crrIndex int

	// Reverse iterate?
	reverse bool

	// Items saved in Btree
	values []*Item
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	// This anonymous function defines what to do to each element in the tree
	saveValues := func(it btree.Item) bool {
		// it.(*Item) 
		// (it) is an interface. This grammar means turning (it) to *Item
		// If (it) is actually an instance of *Item, the conversion will succeed; otherwise, trigger a panic.
		// In btree, each element in the tree is stored as the btree.Item interface type. 
		// When traversing the tree, elements are passed as btree.Item types. 
		// If we know these elements are actually of type *Item, 
		// we can use a type assertion to convert them to *Item in order to access the fields and methods of *Item.
		values[idx] = it.(*Item)
		idx++
		// return false means terminating iteration
		return true
	}

	if reverse {
		tree.Descend(saveValues)
	} else{
		tree.Ascend(saveValues)
	}
	
	return &btreeIterator{
		crrIndex: 0,
		reverse: reverse,
		values: values,
	}
}

func(bti *btreeIterator) Rewind() {
	bti.crrIndex = 0
}

func(bti *btreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.crrIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		bti.crrIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}

func(bti *btreeIterator) Next() {
	bti.crrIndex += 1
}

func(bti *btreeIterator) Valid() bool {
	return bti.crrIndex < len(bti.values)
}

func(bti *btreeIterator) Key() []byte {
	return bti.values[bti.crrIndex].key
}

func(bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.crrIndex].pos
}

func(bti *btreeIterator) Close() {
	bti.values = nil
}
