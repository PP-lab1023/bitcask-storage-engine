package redis

import (
	kvproject "bitcask-go"
	"errors"
	"time"
)

func (rds *RedisDataStructure) Del(key []byte) error {
	return rds.db.Delete(key)
}

func (rds *RedisDataStructure) Type(key []byte) (redisDataType, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return 0, err
	}
	if len(encValue) == 0 {
		return 0, errors.New("value is null")
	}

	// The first byte is type
	return encValue[0], nil
}

func (rds *RedisDataStructure) findMetadata(key []byte, dataType redisDataType) (*metaData, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && err != kvproject.ErrKeyNotFound {
		return nil, err
	}

	var meta *metaData
	var exist = true
	if err == kvproject.ErrKeyNotFound {
		// Need to be initialized
		exist = false
	} else {
		meta = decodeMetadata(metaBuf)
		// Judge data type
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}
		// Judge expire time
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	if !exist {
		meta = &metaData{
			dataType: dataType,
			expire: 0,
			version: time.Now().UnixNano(),
			size: 0,
		}
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}

	return meta, nil
}
