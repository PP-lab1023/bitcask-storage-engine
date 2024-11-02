package kvproject

import (
	"bitcask-go/data"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirName = "-merge"
	mergeFinishedKey = "merge-finished"
)

// Clear invalid data and create hint file
func (db *DB) Merge() error {
	// If database is empty, return
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()

	// If merge is in progress, return error
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeIsInProgress
	}
	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// Make the current active file persistant 
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	// Transfer current active file into oler file
	db.olderFiles[db.activeFile.FileId] = db.activeFile
	// Open a new active file
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return err
	}
	// Record the latest file not merged
	nonMergeFileId := db.activeFile.FileId

	// Get all the files which need merge
	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	// Now the lock can be released. Then user can write in new data
	// The newly wrriten in data won't influence the data to be merged
	db.mu.Unlock()

	// Sort merge files, merge in turn 
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	mergePath := db.getMergePath()
	// If this mergePath exists, it means merge has happened before
	// Delete the merge directory before
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	// Newly build a merge directory
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}
	// Open a new database instance
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	// If merge of the whole file is not successful, there is no need to sync
	// Sync will be controlled in the code below
	mergeOptions.SyncWrite = false     
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}

	// Open hint file
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	// Iterate over all the data files which need to be processed
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			realKey, _ := parseLogRecordKey(logRecord.Key)

			// Compare with key's log record position in index (this must be the latest)
			logRecordPos := db.index.Get(realKey)
			if logRecordPos != nil && logRecordPos.Fid == dataFile.FileId && logRecordPos.Offset == offset {
				// This logRecord is latest.
				// Should be written to the current active file
				// Because this data is valid. There is no need to write its seqNo to the current active file
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				// Write the pos to the hint file
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}

			}
			offset += size
		}	
	}

	// Make files persistent
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}

	// Write the file which indicates the end of merge
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinRecord := &data.LogRecord{
		Key: []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}
	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}

	return nil
}

// eg : /tmp/bitcask  ->   /tmp/bitcask-merge
func (db *DB) getMergePath() string {
	// func Clean make sure the format of DirPath is correct
	// func Dir get the parent directory of DirPath
	dir := path.Dir(path.Clean(db.options.DirPath))
	// Get the name of the database directory
	// For the example above, it gets bitcask
	base := path.Base(db.options.DirPath)
	return filepath.Join(dir, base + mergeDirName)
}

// Load merge directory
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		// If not exist, return
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// Find the file which indicates the end of merge
	var mergeFinished bool
	var mergeFileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		} 
		// All the files in the merge directory are in mergeFileNames, 
		// including the hint file and the MergeFinishedFile
		// They will be moved to replace those merged old files
		mergeFileNames = append(mergeFileNames, entry.Name())
		
	}

	if !mergeFinished {
		// Merge didn't complete
		return nil
	}

	// Merge completed. Need to delete the merged old files and move the new one in.
	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.options.DirPath, fileId)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}
	for _, fileName := range mergeFileNames {
		// Hint file is also moved to destPath
		scrPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(db.options.DirPath, fileName)
		if err := os.Rename(scrPath, destPath); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}

	// Because there is only one data, the offset is 0
	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}

	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}

	return uint32(nonMergeFileId), nil
}

func (db *DB) loadIndexFromHintFile() error {
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	// The hint file exists
	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}

	// Read indexes in the file
	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		pos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, pos)
		offset += size
	}
	return nil
}