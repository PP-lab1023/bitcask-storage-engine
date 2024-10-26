package kvproject

// Some parameters given by user
type Options struct {
	// The place of written files
	DirPath string 

	// The limit of active file
	DataFileSize uint64

	// Is it necessary to immediately make data persistent after every write?
	SyncWrite bool
}