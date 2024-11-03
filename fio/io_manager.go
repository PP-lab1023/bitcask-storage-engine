package fio

const DataFilePerm = 0644

type FileIOType = byte
const (
	// Standard file IO
	StandardFIO FileIOType = iota

	// Memory file mapping
	MemoryMap
)

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

	// Get size of the file 
	Size() (int64, error)
}

// Initialize IOManager, only support standard FileIO
func NewIOManager(filename string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFIO:
		return NewFileIOManager(filename)
	case MemoryMap:
		return NewMMapIOManager(filename)
	default:
		panic("unsupported IO type")
	}
	
}

