package kvproject

import (
	"bitcask-go/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

// For the data written in not by transaction, give them a specific seqNo
const nonTransactionSeqNo uint64 = 0

var txnFinKey = []byte("txn-fin")

// Guarantee atomicity of writing data
type WriteBatch struct {
	options WriteBatchOptions
	mu *sync.Mutex
	db *DB
	pendingWrites map[string]*data.LogRecord  // Save data wrriten in by user
}

// Initialize write batch
func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		options: opts,
		mu: new(sync.Mutex),
		db: db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// Temporarily store logRecord
	logRecord := &data.LogRecord{Key: key, Value: value}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// Data doesn't exist
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	// Temporarily store logRecord
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}



// Commit transaction, store data to disk in batches and update the in-memory index
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}

	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	// Guarantee serialization of transaction commits
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	// Get current transaction serial number
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)
	
	// Write data to the data file
	positions := make(map[string]*data.LogRecordPos)
	for _, record := range wb.pendingWrites {
		// Because the function has already added lock at the beginning, there is no need here
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key: logRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type: record.Type,
		})
		if err != nil {
			return err
		}
		positions[string(record.Key)] = logRecordPos
	}

	// A signal of the completion of the transaction
	finishedRecord := &data.LogRecord{
		Key: logRecordKeyWithSeq(txnFinKey, seqNo),
		Type: data.LogRecordFinished,
	}
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}
	
	// Determine whether to persist based on the option
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// All the data of the transaction have been written to the data file
	// Update the in-memory indexer
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		if record.Type == data.LogRecordNormal {
			wb.db.index.Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDeleted {
			wb.db.index.Delete(record.Key)
		}
	}

	// Clear pendingWrites to enable next commit
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

// Encode key and seqNo
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq, seqNo)

	encKey := make([]byte, n + len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}

// Get key and transaction serial number
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}