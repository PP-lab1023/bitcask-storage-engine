package index

import (
	"bitcask-go/data"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBPlusTree_Put(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-put")
	_ = os.MkdirAll(path, os.ModePerm)

	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	res1 := tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	assert.Nil(t, res1)
	res2 := tree.Put([]byte("fs"), &data.LogRecordPos{Fid: 123, Offset: 999})
	assert.Nil(t, res2)
	res3 := tree.Put([]byte("gg"), &data.LogRecordPos{Fid: 123, Offset: 999})
	assert.Nil(t, res3)

	res4 := tree.Put([]byte("gg"), &data.LogRecordPos{Fid: 124, Offset: 99})
	assert.Equal(t, res4.Fid, uint32(123))
	assert.Equal(t, res4.Offset, int64(999))
}

func TestBPlusTree_Get(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-get")
	_ = os.MkdirAll(path, os.ModePerm)

	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	pos1 := tree.Get([]byte("not exist"))
	t.Log(pos1)
	assert.Nil(t, pos1)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	pos2 := tree.Get([]byte("aac"))
	t.Log(pos2)
	assert.NotNil(t, pos2)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 13, Offset: 999})
	pos3 := tree.Get([]byte("aac"))
	t.Log(pos3)
	assert.NotNil(t, pos3)
}

func TestBPlusTree_Delete(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-delete")
	_ = os.MkdirAll(path, os.ModePerm)

	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	res1, ok1 := tree.Delete([]byte("not exist"))
	assert.Nil(t, res1)
	assert.False(t, ok1)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	res2, ok2 := tree.Delete([]byte("aac"))
	assert.True(t, ok2)
	assert.Equal(t, uint32(123), res2.Fid)
	assert.Equal(t, int64(999), res2.Offset)

	pos1 := tree.Get([]byte("aac"))
	assert.Nil(t, pos1)
}

func TestBPlusTree_Size(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-size")
	_ = os.MkdirAll(path, os.ModePerm)

	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	t.Log(tree.Size())
	assert.Equal(t, tree.Size(), 0)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("fs"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("gg"), &data.LogRecordPos{Fid: 123, Offset: 999})
	t.Log(tree.Size())
	assert.Equal(t, tree.Size(), 3)
}

func TestBPlusTree_Iterator(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-Iterator")
	_ = os.MkdirAll(path, os.ModePerm)

	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("fs"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("gg"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("zds"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("ghh"), &data.LogRecordPos{Fid: 123, Offset: 999})

	iter1 := tree.Iterator(false)
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		t.Log(string(iter1.Key()))
		assert.NotNil(t, iter1.Key())
		assert.NotNil(t, iter1.Value())
	}

	iter2 := tree.Iterator(true)
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		t.Log(string(iter2.Key()))
		assert.NotNil(t, iter2.Key())
		assert.NotNil(t, iter2.Value())
	}
}