package redis

import (
	"bitcask-go"
	"bitcask-go/utils"
	"bytes"
	"encoding/binary"
	"errors"
	"strconv"
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
func NewRedisDataStructure(options kvproject.Options) (*RedisDataStructure, error) {
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
		
		buf := sk.encode()	
		_ = wb.Put(buf, nil)
		

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

func (rds *RedisDataStructure) SMembers(key []byte) ([][]byte, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		// There's no member in the set
		return nil, nil
	}

	// Construct prefix for member
	sk := &setInternalKey{
		key: key,
		version: meta.version,
	}
	opt := kvproject.DefaultIteratorOptions
	opt.Prefix = bytes.TrimRight(sk.encode(), "\x00")

	// Iterate over to find all the members
	var members [][]byte
	it := rds.db.NewIterator(opt)
	for it.Rewind(); it.Valid(); it.Next() {
		key = it.Key()
		
		// Skip key and version
		member := key[len(sk.key) + 8:]
		members = append(members, member)
	}
	return members, nil
}

// ================ List data structure ================
func (rds *RedisDataStructure) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	// Find metadata
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return 0, err
	}

	// Construct key for member
	lk := &listInternalKey{
		key: key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail 
	}

	// Update
	wb := rds.db.NewWriteBatch(kvproject.DefaultWriteBatchOptions)
	meta.size++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}
	_ = wb.Put(key, meta.encode())
	_ = wb.Put(lk.encode(), element)
	if err = wb.Commit(); err != nil {
		return 0, err
	}
	
	return meta.size, nil
}

func (rds *RedisDataStructure) LPush(key, element[]byte) (uint32, error){
	return rds.pushInner(key, element, true)
}

func (rds *RedisDataStructure) RPush(key, element[]byte) (uint32, error){
	return rds.pushInner(key, element, false)
}

func (rds *RedisDataStructure) popInner(key []byte, isLeft bool) ([]byte, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		// There's no member in the set
		return nil, nil
	}

	// Construct key for member
	lk := &listInternalKey{
		key: key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1 
	}

	element, err := rds.db.Get(lk.encode())
	if err != nil {
		return nil, err
	}

	// Update meta
	meta.size--
	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}
	if err = rds.db.Put(key, meta.encode()); err != nil {
		return nil, err
	}

	return element, nil
}

func (rds *RedisDataStructure) LPop(key []byte) ([]byte, error){
	return rds.popInner(key, true)
}

func (rds *RedisDataStructure) RPop(key []byte) ([]byte, error){
	return rds.popInner(key, false)
}

// ================ ZSet data structure ================
func (rds *RedisDataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return false, err
	}

	// Construct key for member
	zk := &zsetInternalKey{
		key: key,
		version: meta.version,
		score: score,
		member: member,
	}

	var exist = true
	value, err := rds.db.Get(zk.encode())
	if err != nil && err != kvproject.ErrKeyNotFound {
		// Something goes wrong
		return false, err
	}
	if err == kvproject.ErrKeyNotFound {
		exist = false
	}
	if exist {
		// The member exists and its score doesn't change
		if score == utils.Float64FromBytes(value) {
			return false, nil
		}
	}

	// Update
	wb := rds.db.NewWriteBatch(kvproject.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	} else {
		oldKey := &zsetInternalKey{
			key: key,
			version: meta.version,
			member: member,
			score: utils.Float64FromBytes(value),
		}
		// Must delete the old key, or this old key will be found when iterating
		_ = wb.Delete(oldKey.encode())
	}
	_ = wb.Put(zk.encode(), utils.Float64ToBytes(score))
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

func (rds *RedisDataStructure) ZScore(key []byte, member []byte) (float64, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		// Don't support negative score
		return -1, err
	}
	if meta.size == 0 {
		return -1, nil
	}

	// Construct key for member
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	value, err := rds.db.Get(zk.encode())
	if err != nil {
		return -1, err
	}

	return utils.Float64FromBytes(value), nil
}

func (rds *RedisDataStructure) ZPopmax(key []byte) ([]byte, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		// There's no member in the set
		return nil, nil
	}
	// Construct prefix for member
	zk := &zsetInternalKey{
		key: key,
		version: meta.version,
	}
	opt := kvproject.DefaultIteratorOptions
	opt.Prefix = bytes.TrimRight(zk.encode(), "\x00")

	// Iterate over to find the member with the biggest score
	it := rds.db.NewIterator(opt)
	var score = -1
	var member []byte
	for it.Rewind(); it.Valid(); it.Next() {
		buf, err := it.Value()
		if err != nil {
			return nil, err
		}

		val, err := strconv.Atoi(string(buf))
		if err != nil {
			return nil, err
		}

		if val > score {
			score = val
			key = it.Key()
			// Skip key and version
			member = key[len(zk.key) + 8:]
		}
	}
	return member, nil
}

