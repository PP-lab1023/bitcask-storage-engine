package data

import "bitcask-go/fio"

// Data files
type DataFile struct {
	FileId uint32			
	WriteOff uint64			// The place where the file is written to 
	IoManager fio.IOManager     // Management of IO read and write
}

// Open new file
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) Write(buf []byte) error {
	return nil
}

func (df *DataFile) Sync() error {
	return nil
}

func (df *DataFile) ReadLogRecord(offset uint64) (*LogRecord, error) {
	return nil, nil
}
