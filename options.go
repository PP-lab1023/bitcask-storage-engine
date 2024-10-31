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

	// Index type
	IndexType IndexerType
}

type IndexerType = int8
const (
	// BTree index
	Btree IndexerType = iota + 1

	// Adaptive radix tree
	ART
)

var DefaultOptions = Options{
	DirPath: os.TempDir(),
	DataFileSize: 256 * 1024 *1024,  //256MB
	SyncWrite: false,
	IndexType: Btree,
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