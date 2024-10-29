package data

import (
	"bitcask-go/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

const DataFileNameSuffix = ".data"
var (
	ErrInvalidCRC = errors.New("invalid crc value, log record may be corrupted")
)

// Data files
type DataFile struct {
	FileId uint32			
	WriteOff int64			// The place where the file is written to 
	IoManager fio.IOManager     // Management of IO read and write
}

// Open new file
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	// Format fileId as an integer and pad it with 0 on the left to ensure the final generated string is 9 digits in length
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId) + DataFileNameSuffix) 

	// Initialize IOManager
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId: fileId,
		WriteOff: 0,
		IoManager: ioManager,
	}, nil
}

// 
func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

// Read LogRecord accrording to the offset
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	// A special case:
	// When deleting data, a log record will be appended to the file
	// If this data is the last one, and it's very small, even smaller than maxLogRecordHeaderSize
	// we should not use maxLogRecordHeaderSize to read, because this will cause EOF
	// Instead, just read to the end of the file
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset + maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	// Read header
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	header, headerSize := decodeLogRecordHeader(headerBuf)

	// Header not read
	// The file reading finishes
	if header == nil {
		return nil, 0, io.EOF
	}

	// Information in header are all 0
	// The file reading finishes
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// Get the length of key and value
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize

	// Read the actual key and value saved by user
	logRecord := &LogRecord{Type: header.recordType}
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize + valueSize, offset + headerSize)
		if err != nil {
			return nil, 0, err
		}

		// Get key and value
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	// Check validation of the data 
	// headerBuf is the longest length. headerSize is the real length
	// crc32.Size = 4
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size: headerSize]) 
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, recordSize, nil
}

func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IoManager.Read(b, offset)
	return b, err
}
