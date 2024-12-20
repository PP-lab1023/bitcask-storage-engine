package data

import (
	"bitcask-go/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

const (
	DataFileNameSuffix = ".data"
	HintFileName = "hint-index"
	MergeFinishedFileName = "merge.finished"
	SeqNoFileName = "seq-no"
)


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
func OpenDataFile(dirPath string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	// Format fileId as an integer and pad it with 0 on the left to ensure the final generated string is 9 digits in length
	fileName := GetDataFileName(dirPath, fileId)
	return newOpenFile(fileName, fileId, ioType)
}

// Open hint index file
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newOpenFile(fileName, 0, fio.StandardFIO)
}

// Open the file which indicates the end of merge
func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newOpenFile(fileName, 0, fio.StandardFIO)
}

// Open the file which saves seqNo
func OpenSeqNoFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, SeqNoFileName)
	return newOpenFile(fileName, 0, fio.StandardFIO)
}

func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId) + DataFileNameSuffix) 
}

func newOpenFile(fileName string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	// Initialize IOManager
	ioManager, err := fio.NewIOManager(fileName, ioType)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId: fileId,
		WriteOff: 0,
		IoManager: ioManager,
	}, nil
}


func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

// Write index information to hint file
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key: key,
		Value: EncodeLogRecordPos(pos),
	}
	encRecord, _ := EncodeLogRecord(record)
	return df.Write(encRecord)
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

func (df *DataFile) SetIOManager(driPath string, ioTpye fio.FileIOType) error {
	if err := df.IoManager.Close(); err != nil {
		return err
	}
	ioManager, err := fio.NewIOManager(GetDataFileName(driPath, df.FileId), ioTpye)
	if err != nil {
		return err
	}
	df.IoManager = ioManager
	return nil
}
