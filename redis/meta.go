package redis

import (
	"encoding/binary"
	"math"
)

const (
	maxMetadataSize = 1 + binary.MaxVarintLen64 * 2 + binary.MaxVarintLen32
	extraListMetadataSize = binary.MaxVarintLen64 * 2

	initialListMark = math.MaxUint64 / 2
)



type metaData struct {
	dataType byte 
	expire int64
	version int64
	size uint32		// Number of keys

	// Especially for List
	head uint64
	tail uint64
}

func (md *metaData) encode() []byte {
	var size = maxMetadataSize
	if md.dataType == List {
		size += extraListMetadataSize
	}
	buf := make([]byte, size)

	buf[0] = md.dataType
	var index = 1
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutVarint(buf[index:], int64(md.size))

	if md.dataType == List {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}

	return buf[:index]
}

func decodeMetadata(buf []byte) *metaData {
	dataType := buf[0]

	var index = 1
	expire, n := binary.Varint(buf[index:])
	index += n
	version, n := binary.Varint(buf[index:])
	index += n
	size, n := binary.Varint(buf[index:])
	index += n

	var head uint64 = 0
	var tail uint64 = 0
	if dataType == List {
		head, n = binary.Uvarint(buf[index:])
		index += n
		tail, _ = binary.Uvarint(buf[index:])
	}

	return &metaData{
		dataType: dataType,
		expire: expire,
		version: version,
		size: uint32(size),
		head: head,
		tail: tail,
	}
}

type hashInternalKey struct {
	key []byte
	version int64
	field []byte
}

func (hk *hashInternalKey) encode() []byte {
	buf := make([]byte, len(hk.key) + len(hk.field) + 8)
	var index = 0
	// key
	copy(buf[index : index + len(hk.key)], hk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index : index + 8], uint64(hk.version))
	index += 8

	// field
	copy(buf[index:], hk.field)

	return buf
}


type setInternalKey struct {
	key []byte
	version int64
	member []byte
}

func (sk *setInternalKey) encode() []byte {
	buf := make([]byte, len(sk.key) + len(sk.member) + 8)
	var index = 0
	// key
	copy(buf[index : index + len(sk.key)], sk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index : index + 8], uint64(sk.version))
	index += 8

	// member
	copy(buf[index:], sk.member)
	
	return buf
}

type listInternalKey struct {
	key []byte
	version int64
	index uint64
}

func (lk *listInternalKey) encode() []byte {
	buf := make([]byte, len(lk.key) + 8 + 8)
	var index = 0
	// key
	copy(buf[index : index + len(lk.key)], lk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index : index + 8], uint64(lk.version))
	index += 8

	// index
	binary.LittleEndian.PutUint64(buf[index:], uint64(lk.version))
	
	return buf
}