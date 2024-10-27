package kvproject

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Database-oriented interface
type DB struct {
	options Options
	mu *sync.RWMutex
	fileIds []int                           // Can only be used when loading index, cannot be used or updated in other places
	activeFile *data.DataFile 				// Current active file, can be written in
	olderFiles map[uint32]*data.DataFile    // Old files, can only be read
	index index.Indexer
}

// Open bitcask storage engine instance
func Open(options Options) (*DB, error) {
	// Verify the configuration items passed in by the user
	if err := checkOptions(options); err != nil {
		return nil, err
	}
	
	// Judge if DirPath exists
	// If not exits, construct
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// Initialize DB instance
	db := &DB{
		options: options,
		mu: new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index: index.NewIndexer(options.IndexType),
	}

	// Load data file
	// These are files to be appended (log files)
	// Actually, log files are data files. They are the same thing.
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// Load index from data files
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}

	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be positive")
	}

	return nil
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

// Delete corresponding data according to the key
func (db *DB) Delete(key []byte) error{
	// Judge validation of the key
	if len(key) ==  0{
		return ErrKeyIsEmpty
	}

	// Examine if the key exists
	// If not exist, there is no need to write this log record
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// The key exists, construct corresponding log record
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// Delete the key in the in-memory index
	ok := db.index.Delete(key)
	if !ok {
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
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
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

// Load data files from disk
func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	// Iterate over all the files in the directory
	// Find all the files ended with .data
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			// Parse file name
			// Data file name is like 0001.data
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			if err != nil {
				// There are some files in the directory with .data suffix but its name is not a number
				// This is not allowed
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	// Sort id, load in order from small to large
	sort.Ints(fileIds)
	db.fileIds = fileIds

	// Iterate over all the fileIds, Open corresponding data file
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}

		if i == len(fileIds) - 1 {
			// The last one, which means it's current active file
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

// Load index from data files
// Use fileIds to iterate over all the records in files
func (db *DB) loadIndexFromDataFiles() error {
	// No files, which means the database is empty
	if len(db.fileIds) == 0 {
		return nil
	}
	
	// Iterate over all the fileIds
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		// Iterate over all the records in the file
		var offset uint64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset) 
			if err != nil {
				// There are two possibilities:
				// 1. Something go wrong, just return the error
				// 2. Reach the end of the file. This is normal. Should get out of the loop
				if err == io.EOF {
					break
				}
				return err
			}
			// Construct in-memory index
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset}
			if logRecord.Type == data.LogRecordDeleted {
				db.index.Delete(logRecord.Key)
			} else {
				db.index.Put(logRecord.Key, logRecordPos)
			}

			// Update offset
			offset += size
		}

		// If it is current active file
		// Update writeoff
		if i == len(db.fileIds) - 1 {
			db.activeFile.WriteOff = offset
		}
	}
	return nil
}

