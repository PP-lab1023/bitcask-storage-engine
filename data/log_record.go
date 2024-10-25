package data

// Index of data on RAM (in-memory)
// describe data's postion on disk (ie. keydir)
type LogRecordPos struct {
	Fid    uint32
	Offset int64
}
