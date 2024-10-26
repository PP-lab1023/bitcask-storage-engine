package data

// A signal of whether this data means deletion
type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

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
	Offset uint64
}

// Encode LogRecord, return a byte array and its length
// LogRecord is a sturct, but method in io_manager.go use bytes to read and write
func EncodelogRecord(logRecord *LogRecord) ([]byte, uint64){
	return nil, 0
}

