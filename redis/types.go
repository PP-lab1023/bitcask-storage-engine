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

// ================ Hash data structure ================
func (rds *RedisDataStructure) HSet(key, field, value []byte) (bool, error) {
	// Find metadata, if not exist(ie. the hash has not been created before), create
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	// Create key of the data put in hash
	hk := &hashInternalKey{
		key: key,
		version: meta.version,
		field: field,
	}
	encKey := hk.encode()

	// Find if key exists(ie. Has the key been put in hash before?)
	// If not exists, return true, otherwise false
	var exist = true
	if _, err = rds.db.Get(encKey); err == kvproject.ErrKeyNotFound {
		exist = false
	}

	wb := rds.db.NewWriteBatch(kvproject.DefaultWriteBatchOptions)
	// If not exist, need to do some updates
	if !exist {
		meta.size++
		// Update hash's metadata
		_ = wb.Put(key, meta.encode())
	}
	// Even if the key exists, its field may be changed
	_ = wb.Put(encKey, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		// There is no data in this hash
		return nil, nil
	}
	
	hk := &hashInternalKey{
		key: key,
		version: meta.version,
		field: field,
	}
	encKey := hk.encode()

	return rds.db.Get(encKey)
}

func (rds *RedisDataStructure) HDel(key, field []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		// There is no data in this hash
		return false, nil
	}

	hk := &hashInternalKey{
		key: key,
		version: meta.version,
		field: field,
	}
	encKey := hk.encode()

	var exist = true
	if _, err = rds.db.Get(encKey); err == kvproject.ErrKeyNotFound {
		exist = false
	}

	if exist {
		wb := rds.db.NewWriteBatch(kvproject.DefaultWriteBatchOptions)
		meta.size--
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}

	return exist, nil
}

// ================ Set data structure ================
func (rds *RedisDataStructure) SAdd(key, member[]byte) (bool, error){
	// Find metadata
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	// Construct key for member
	sk := &setInternalKey{
		key: key,
		version: meta.version,
		member: member,
	}

	var ok = false
	if _, err = rds.db.Get(sk.encode()); err == kvproject.ErrKeyNotFound {
		// The member not exist
		wb := rds.db.NewWriteBatch(kvproject.DefaultWriteBatchOptions)
		meta.size++
		_ = wb.Put(key, meta.encode())
		_ = wb.Put(sk.encode(), nil)
		if err = wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}

	return ok, nil
}

func (rds *RedisDataStructure) SIsmember(key, member[]byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		// There's no member in the set
		return false, nil
	}

	// Construct key for member
	sk := &setInternalKey{
		key: key,
		version: meta.version,
		member: member,
	}

	_, err = rds.db.Get(sk.encode())
	if err != nil && err != kvproject.ErrKeyNotFound {
		return false, err
	}
	if err == kvproject.ErrKeyNotFound {
		return false, nil
	}
	return true, nil
}

func (rds *RedisDataStructure) SRem(key, member[]byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		// There's no member in the set
		return false, nil
	}

	// Construct key for member
	sk := &setInternalKey{
		key: key,
		version: meta.version,
		member: member,
	}

	if _, err = rds.db.Get(sk.encode()); err == kvproject.ErrKeyNotFound {
		// The member not exist
		return false, nil
	} 

	// Update
	wb := rds.db.NewWriteBatch(kvproject.DefaultWriteBatchOptions)
	meta.size--
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(sk.encode())
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}