package data

import (
	"encoding/binary"
	"hash/crc32"
)

// A signal of whether this data means deletion
type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordFinished
)

// crc type(deleted?) keySize valueSize
//  4 +      1       +   5   +    5    = 15
// keySize and valueSize are changeable
const maxLogRecordHeaderSize = binary.MaxVarintLen32 * 2 + 5

// Log of writing to the file
// Data are appended to the file, like log
type LogRecord struct{
	Key []byte
	Value []byte
	Type LogRecordType // Judge whether this data means deletion
}

// Index of data on RAM (in-memory)
// describe data's postion on disk (ie. keydir)
type LogRecordPos struct {
	Fid    uint32
	Offset int64
}

// Header of LogRecord
type logRecordHeader struct {
	crc uint32                  // Check value 
	recordType LogRecordType    // Deletion?
	keySize uint32              // length of key
	valueSize uint32            // length of value
}

// Temporarily saved data related to transaction
type TransactionRecord struct {
	Record *LogRecord
	Pos *LogRecordPos
}

// Encode LogRecord, return a byte array and its length
// LogRecord is a sturct, convert it to the corresponding storage format in the data file
// *---------*---------*---------*---------*---------*---------*
// |   crc   |  type   | keySize |valueSize|   key   |  value  |
// *---------*---------*---------*---------*---------*---------*
//      4         1       max 5     max 5    variant    variant
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64){
	// Initialize a header
	header := make([]byte, maxLogRecordHeaderSize)

	// Crc can only be calculated after the following bytes are determined
	// So skip the first 4 bytes
	header[4] = logRecord.Type
	var index = 5   // The next byte will be placed at byte[5]
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	// index is the actual length of header 
	// size is the length of encoded logRecord
	var size = index + len(logRecord.Key) + len(logRecord.Value)

	// This is the []byte that will be returned
	encBytes := make([]byte, size)

	// Copy header to encByte, just copy its actual length
	copy(encBytes[:index], header[:index])
	// Copy key and value
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index + len(logRecord.Key):], logRecord.Value)

	// Calculate crc
	crc := crc32.ChecksumIEEE(encBytes[4:])
	// Main platforms use little endian
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}

// Decode header of the byte slice 
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if len(buf) <= 4 {
		// Header's length is even smaller than crc. Something went wrong
		return nil, 0
	}

	header := &logRecordHeader{
		crc: binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	var index = 5
	// Get key size
	// Varint has a way to indicate the end of the number when encoding
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	// Get value size
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

// The second parameter doesn't contain crc itself
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(header)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)
	return crc
}

