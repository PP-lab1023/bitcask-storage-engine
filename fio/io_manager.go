package fio

const DataFilePerm = 0644

// Abstract IOManager interface
// Enable different IO types
// But only standard file IO for now
type IOManager interface {
	// Read corresponding data from a specific place in the file
	Read([]byte, int64) (int, error)

	// Write bytes to the file
	Write([]byte) (int, error)

	// make data from in-memory buffer persistent to disk
	Sync() error

	// CLose the file
	Close() error
}