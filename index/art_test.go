package index

import (
	"bitcask-go/data"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()
	res1 := art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Nil(t, res1)
	res2 := art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Nil(t, res2)
	res3 := art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Nil(t, res3)

	res4 := art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 2, Offset: 13})
	t.Log(res4)
	assert.Equal(t, res4.Fid, uint32(1))
	assert.Equal(t, res4.Offset, int64(12))
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	pos1:= art.Get([]byte("key-1"))
	t.Log(pos1)
	assert.NotNil(t, pos1)

	pos2 := art.Get([]byte("key-10"))
	assert.Nil(t, pos2)

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 13})
	pos3:= art.Get([]byte("key-1"))
	t.Log(pos3)
	assert.NotNil(t, pos3)
	
}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewART()

	res1, ok1 := art.Delete([]byte("not exist"))
	assert.Nil(t, res1)
	assert.False(t, ok1)

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	res2, ok2 := art.Delete([]byte("key-1"))
	assert.True(t, ok2)
	t.Log(res2)
	assert.Equal(t, uint32(1), res2.Fid)
	assert.Equal(t, int64(12), res2.Offset)

	pos1:= art.Get([]byte("key-1"))
	t.Log(pos1)
	assert.Nil(t, pos1)
}

func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewART()

	t.Log(art.Size())
	assert.Equal(t, art.Size(), 0)
	
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 12})
	t.Log(art.Size())
	assert.Equal(t, art.Size(), 3)
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()
	art.Iterator(false)

	art.Put([]byte("code"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("asfjsl"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("jjvb"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("bsfjl"), &data.LogRecordPos{Fid: 1, Offset: 12})

	iter1 := art.Iterator(false)
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		t.Log(string(iter1.Key()))
		assert.NotNil(t, iter1.Key())
		assert.NotNil(t, iter1.Value())
	}

	iter2 := art.Iterator(true)
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		t.Log(string(iter2.Key()))
		assert.NotNil(t, iter2.Key())
		assert.NotNil(t, iter2.Value())
	}
}