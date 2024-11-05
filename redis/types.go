package redis

import (
	kvproject "bitcask-go"
	"encoding/binary"
	"errors"
	"time"
)

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

type RedisDataStructure struct {
	db *kvproject.DB
}

type redisDataType = byte
const (
	String redisDataType = iota
	Hash
	Set
	List
	ZSet
)

// Initialize redis service
func NewReisDataStructure(options kvproject.Options) (*RedisDataStructure, error) {
	// The same as open a database
	db, err := kvproject.Open(options)
	if err != nil {
		return nil, err
	}
	return &RedisDataStructure{db: db}, nil
}

// ================ String data structure ================
func (rds *RedisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	// Encode value: type + expire + payload
	buf := make([]byte, binary.MaxVarintLen64 + 1)
	buf[0] = String

	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)

	encValue := make([]byte, index + len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	// Use interface to write in
	return rds.db.Put(key, encValue)
}

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	// Decode

	// dataType is not right
	dataType := encValue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}

	// Data has expired
	var index = 1
	expire, n := binary.Varint(encValue[index:])
	index += n
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}

	return encValue[index:], nil
}

