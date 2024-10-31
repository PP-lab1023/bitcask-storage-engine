package kvproject

import (
	"bitcask-go/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-1")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	t.Log(iterator.Valid())
	assert.Equal(t, false, iterator.Valid())
}

func TestDB_Iterator_One_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-2")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(10), utils.GetTestKey(10))
	assert.Nil(t, err)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	t.Log(iterator.Valid())
	assert.Equal(t, true, iterator.Valid())
	t.Log(string(iterator.Key()))
	assert.Equal(t, utils.GetTestKey(10), iterator.Key())
	val, err := iterator.Value()
	assert.Nil(t, err)
	assert.Equal(t, utils.GetTestKey(10), val)
}

func TestDB_Iterator_Many_Values(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-3")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("annde"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("ax"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("vggh"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("zsffgg"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("ggds"), utils.RandomValue(10))
	assert.Nil(t, err)

	// Iterate
	Iter1 := db.NewIterator(DefaultIteratorOptions)
	for Iter1.Rewind(); Iter1.Valid(); Iter1.Next() {
		t.Log("key = ", string(Iter1.Key()))
		assert.NotNil(t, Iter1.Key())
	}
	Iter1.Rewind()
	for Iter1.Seek([]byte("c")); Iter1.Valid(); Iter1.Next() {
		t.Log("key = ", string(Iter1.Key()))
		assert.NotNil(t, Iter1.Key())
	}

	// Reverse iterate
	IterOpts1 := DefaultIteratorOptions
	IterOpts1.Reverse = true
	Iter2 := db.NewIterator(IterOpts1)
	for Iter2.Rewind(); Iter2.Valid(); Iter2.Next() {
		t.Log("key = ", string(Iter2.Key()))
		assert.NotNil(t, Iter2.Key())
	}
	Iter2.Rewind()
	for Iter2.Seek([]byte("c")); Iter2.Valid(); Iter2.Next() {
		t.Log("key = ", string(Iter2.Key()))
		assert.NotNil(t, Iter2.Key())
	}

	// Have prefix
	IterOpts2 := DefaultIteratorOptions
	IterOpts2.Prefix = []byte("ann")
	Iter3 := db.NewIterator(IterOpts2)
	for Iter3.Rewind(); Iter3.Valid(); Iter3.Next() {
		t.Log("key = ", string(Iter3.Key()))
		assert.NotNil(t, Iter3.Key())
	}

}

