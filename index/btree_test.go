package index

import (
	"bitcask-go/data"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)

	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	t.Log(res3)
	assert.Equal(t, res3.Fid, uint32(1))
	assert.Equal(t, res3.Offset, int64(2))
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	// Test if Get works after changing the value of a key
	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.Equal(t, res3.Fid, uint32(1))
	assert.Equal(t, res3.Offset, int64(2))
	
	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)
	res2, ok1 := bt.Delete(nil)
	assert.Equal(t, res2.Fid, uint32(1))
	assert.Equal(t, res2.Offset, int64(100))
	assert.True(t, ok1)

	res3 := bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 22, Offset: 33})
	assert.Nil(t, res3)
	res4, ok2 := bt.Delete([]byte("aaa"))
	assert.True(t, ok2)
	assert.Equal(t, res4.Fid, uint32(22))
	assert.Equal(t, res4.Offset, int64(33))
}

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBTree()

	// 1. BTree is empty
	iter1 := bt1.Iterator(false)
	t.Log(iter1.Valid())
	
	// 2. BTree is not empty
	bt1.Put([]byte("code"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	t.Log(iter2.Key())
	assert.NotNil(t, iter2.Key())
	t.Log(iter2.Value())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, iter2.Valid(), false)

	// 3. BTree has many data
	bt1.Put([]byte("afsd"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("kg"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("vbf"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		t.Log("key = ", string(iter3.Key()))
	}

	// 4. Reversed
	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		t.Log("key = ", string(iter4.Key()))
	}

	// 5. Test Seek()
	iter5 := bt1.Iterator(false)
	for iter5.Seek([]byte("cc")); iter5.Valid(); iter5.Next() {
		t.Log("key = ", string(iter5.Key()))
	}
	
	// 6. Reversed Seek()
	iter6 := bt1.Iterator(true)
	for iter6.Seek([]byte("e")); iter6.Valid(); iter6.Next() {
		t.Log("key = ", string(iter6.Key()))
	}
}
