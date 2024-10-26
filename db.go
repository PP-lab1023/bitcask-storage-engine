package kvproject

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"sync"
)

// Database-oriented interface
type DB struct {
	options Options
	mu *sync.RWMutex
	activeFile *data.DataFile 				// Current active file, can be written in
	olderFiles map[uint32]*data.DataFile    // Old files, can only be read
	index index.Indexer
}

// Write key/value, key cannot be nil
func (db *DB) Put(key []byte, value []byte) error{
	// Judge if the key is valid
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// Build struct LogRecord 
	logRecord := &data.LogRecord{
		Key: key,
		Value: value,
		Type: data.LogRecordNormal, 
	}

	// Append to the current active file
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// Renew in-memory index
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}

	return nil

}

// Get value according to key
func (db *DB) Get(key []byte) ([]byte, error) {
	// Add read lock
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Judge the validation of key
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// Get corresponding index from in-memory
	logRecordPos := db.index.Get(key)
	// If key is not in the in-memory index, key doesn't exist
	if logRecordPos == nil {
		return nil, ErrKeyNotFind
	}

	// Find data file according to file id
	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}

	// Data file is nil
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// Read corresponding data according to offset
	logRecord, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	// Judge if the key is deleted
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFind
	}

	return logRecord.Value, nil
}

// append logRecord to active file
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error){
	db.mu.Lock()
	defer db.mu.Unlock()

	// Judge if current active file exists
	// because when there is no write to the database, there is no active file
	// If not exists, initialize
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// Encode logRecord
	encRecord, size := data.EncodelogRecord(logRecord)

	// If written data reaches the limit of the active file,
	// then close the active file and open a new file
	if db.activeFile.WriteOff + size > db.options.DataFileSize {
		// Make data in files persistent from in-memory to disk
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// Make current active file transfer to old files
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// Open new file
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// Write data to the file
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// Determine if it is necessary to make data persistent according to user's option
	if db.options.SyncWrite {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// Construct LogRecordPos and return 
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff}
	return pos, nil
}

// Set current active file
// Must have lock when using this method
func (db *DB) setActiveDataFile() error{
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		// It means there are files that have been opened
		initialFileId = db.activeFile.FileId + 1
	}

	// Open new file
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}