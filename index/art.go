package index

import (
	"bitcask-go/data"
	"bytes"
	"sort"
	"sync"

	goart "github.com/plar/go-adaptive-radix-tree"
)

// Adaptive radix tree
// https://github.com/plar/go-adaptive-radix-tree

type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

// Initialize an AdaptiveRadixTree
func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	oldValue, _ := art.tree.Insert(key, pos)
	art.lock.Unlock()
	if oldValue == nil {
		return nil 
	}
	return oldValue.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.Lock()
	defer art.lock.Unlock()

	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	// value is an interface. Must be transferred to LogRecordPos
	return value.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()
	oldValue, deleted := art.tree.Delete(key)
	art.lock.Unlock()
	if oldValue == nil {
		return nil, false
	}
	return oldValue.(*data.LogRecordPos), deleted
}

func (art *AdaptiveRadixTree) Size() int {
	art.lock.Lock()
	size := art.tree.Size()
	art.lock.Unlock()
	return size
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.Lock()
	defer art.lock.Unlock()
	return newARTIterator(art.tree, reverse)
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

// ART iterator
type artIterator struct {
	// Iterator creates an array to temporarily save keys
	// currIndex is the place of the iterator in the array
	crrIndex int

	// Reverse iterate?
	reverse bool

	// Items saved in ART
	values []*Item
}

func newARTIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int

	if reverse {
		// Store data in reverse
		idx = tree.Size() - 1
	}

	values := make([]*Item, tree.Size())
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	tree.ForEach(saveValues)
	
	return &artIterator{
		crrIndex: 0,
		reverse: reverse,
		values: values,
	}
}

func(ai *artIterator) Rewind() {
	ai.crrIndex = 0
}

func(ai *artIterator) Seek(key []byte) {
	if ai.reverse {
		ai.crrIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) <= 0
		})
	} else {
		ai.crrIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) >= 0
		})
	}
}

func(ai *artIterator) Next() {
	ai.crrIndex += 1
}

func(ai *artIterator) Valid() bool {
	return ai.crrIndex < len(ai.values)
}

func(ai *artIterator) Key() []byte {
	return ai.values[ai.crrIndex].key
}

func(ai *artIterator) Value() *data.LogRecordPos {
	return ai.values[ai.crrIndex].pos
}

func(ai *artIterator) Close() {
	ai.values = nil
}
