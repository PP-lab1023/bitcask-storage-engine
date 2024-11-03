package index

import (
	"bitcask-go/data"
	"path/filepath"

	"go.etcd.io/bbolt"
)

const bptreeIndexFileName = "bptree-index"
var indexBucketName = []byte("bitcask-index")

// b+ tree index
// go.etcd.io/bbolt
type BPlusTree struct {
	// bbolt itself is a storage engine
	// bbolt support Concurrent reading and writing. No need to add lock
	tree *bbolt.DB
}

// Initialize BPlusTree
func NewBPlusTree(dirPath string, syncWrite bool) *BPlusTree {
	opts := bbolt.DefaultOptions
	opts.NoSync = !syncWrite
	// B+ tree stores indexes on disk
	// So it needs a filepath to open
	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, opts)
	if err != nil {
		panic("failed to open bptree")
	}

	// Because bbolt itself is a database
	// The method Update supports transaction.  
	// Create a bucket. This database uses bucket to put in data
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create bucket in bptree")
	}
	return &BPlusTree{tree: bptree}
}

func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) bool {
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to put value in bptree")
	}
	return true
}

func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos

	// Method View can only be used to read
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		panic("failed to get value in bptree")
	}
	return pos
}

func (bpt *BPlusTree) Delete(key []byte) bool {
	var ok = false
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if value := bucket.Get(key); len(value) != 0 {
			ok = true
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete value in bptree")
	}
	return ok
}

func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get size in bptree")
	}
	return size
}

func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBptreeIterator(bpt.tree, reverse)
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

// B+ tree iterator
type bptreeIterator struct {
	tx *bbolt.Tx
	cursor *bbolt.Cursor
	reverse bool
	curKey []byte
	curVal []byte
}

func newBptreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a transaction")
	}
	bpi := &bptreeIterator{
		tx: tx,
		cursor: tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	// If not call Rewind(), the curKey is empty. When call Valid(), it goes wrong
	bpi.Rewind()
	return bpi
}

func (bpi *bptreeIterator) Rewind() {
	if bpi.reverse {
		bpi.curKey, bpi.curVal = bpi.cursor.Last()
	} else {
		bpi.curKey, bpi.curVal = bpi.cursor.First()
	}
}

func (bpi *bptreeIterator) Seek(key []byte) {
	bpi.curKey, bpi.curVal = bpi.cursor.Seek(key)
}

func (bpi *bptreeIterator) Next() {
	if bpi.reverse {
		bpi.curKey, bpi.curVal = bpi.cursor.Prev()
	} else {
		bpi.curKey, bpi.curVal = bpi.cursor.Next()
	}
}

func (bpi *bptreeIterator) Valid() bool {
	return len(bpi.curKey) != 0
}

func (bpi *bptreeIterator) Key() []byte {
	return bpi.curKey
}

func (bpi *bptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bpi.curVal)
}

func (bpi *bptreeIterator) Close() {
	// Submit the temporary transaction
	// Read-only transactions must be rolled back and not committed.(Written in the comment of Rollback())
	_ = bpi.tx.Rollback()
}