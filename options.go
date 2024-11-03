package kvproject

import "os"

// Some parameters given by user
type Options struct {
	// The place of written files
	DirPath string 

	// The limit of active file
	DataFileSize int64

	// Is it necessary to immediately make data persistent after every write?
	SyncWrite bool

	// How many bytes are written in total before being persisted?
	BytesPerSync uint

	// Index type
	IndexType IndexerType

	// Use mmap when start up?
	MMapAtStartUp bool

	// Threshold for data file merging
	DataFileMergeRatio float32
}

type IndexerType = int8
const (
	// BTree index
	Btree IndexerType = iota + 1

	// Adaptive radix tree
	ART

	// B+ tree, store indexes on the disk
	BPlusTree
)

var DefaultOptions = Options{
	DirPath: os.TempDir(),
	DataFileSize: 256 * 1024 *1024,  //256MB
	SyncWrite: false,
	BytesPerSync: 0,
	IndexType: Btree,
	MMapAtStartUp: true,
	DataFileMergeRatio: 0.5,
}

// Options of iterator
type IteratorOptions struct {
	// Traverse the keys whose prefix is the specified value. Empty by default.
	Prefix []byte

	// Reversed? Default is false
	Reverse bool
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix: nil,
	Reverse: false,
}

// Options for batch writing
type WriteBatchOptions struct {
	// The maximum amount of data in a batch
	MaxBatchNum uint 

	// Persist when committing a transaction?
	SyncWrites bool
}

var DefaultWriteBatchOptions = WriteBatchOptions {
	MaxBatchNum: 10000,
	SyncWrites: true,
}