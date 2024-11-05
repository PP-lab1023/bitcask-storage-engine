package redis

import (
	kvproject "bitcask-go"
	"bitcask-go/utils"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRedisDataStructure_Del_Type(t *testing.T) {
	opts := kvproject.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
	opts.DirPath = dir
	rds, err := NewReisDataStructure(opts)
	assert.Nil(t, err)

	// Del()
	err = rds.Del(utils.GetTestKey(11))
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)

	// Type()
	typ, err := rds.Type(utils.GetTestKey(1))
	assert.Nil(t, err)
	t.Log(typ)
	assert.Equal(t, String, typ)

	err = rds.Del(utils.GetTestKey(1))
	assert.Nil(t, err)

	_, err = rds.Get(utils.GetTestKey(1))
	t.Log(err)
	assert.Equal(t, kvproject.ErrKeyNotFound, err)
}

// ================ String data structure ================
func TestRedisDataStructure_Get(t *testing.T) {
	opts := kvproject.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
	opts.DirPath = dir
	rds, err := NewReisDataStructure(opts)
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)
	err = rds.Set(utils.GetTestKey(2), time.Second * 5, utils.RandomValue(100))
	assert.Nil(t, err)

	val1, err := rds.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	t.Log(string(val1))
	assert.NotNil(t, val1)

	val2, err := rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	t.Log(string(val2))
	assert.NotNil(t, val2)

	_, err = rds.Get(utils.GetTestKey(3))
	assert.Equal(t, kvproject.ErrKeyNotFound, err)
}

// ================ Hash data structure ================
func TestRedisDataStructure_HGet(t *testing.T) {
	opts := kvproject.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
	opts.DirPath = dir
	rds, err := NewReisDataStructure(opts)
	assert.Nil(t, err)

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.True(t, ok1)
	assert.Nil(t, err)
	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(200))
	assert.False(t, ok2)
	assert.Nil(t, err)
	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), utils.RandomValue(100))
	assert.True(t, ok3)
	assert.Nil(t, err)

	val1, err := rds.HGet(utils.GetTestKey(1), []byte("field1"))
	t.Log(string(val1))
	assert.Nil(t, err)
	val2, err := rds.HGet(utils.GetTestKey(1), []byte("field2"))
	t.Log(string(val2))
	assert.Nil(t, err)

	val3, err := rds.HGet(utils.GetTestKey(1), []byte("field-not-exist"))
	t.Log(string(val3), err)
}

func TestRedisDataStructure_HDel(t *testing.T) {
	opts := kvproject.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
	opts.DirPath = dir
	rds, err := NewReisDataStructure(opts)
	assert.Nil(t, err)

	del1, err := rds.HDel(utils.GetTestKey(200), nil)
	t.Log(del1, err)

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.True(t, ok1)
	assert.Nil(t, err)
	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), utils.RandomValue(100))
	assert.True(t, ok3)
	assert.Nil(t, err)

	del2, err := rds.HDel(utils.GetTestKey(1), []byte("field2"))
	t.Log(del2, err)
}