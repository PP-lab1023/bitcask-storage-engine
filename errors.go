package kvproject

import "errors"

var (
	ErrKeyIsEmpty = errors.New("the key is empty")
	ErrIndexUpdateFailed = errors.New("failed to update index")
	ErrKeyNotFound = errors.New("key not found in database")
	ErrDataFileNotFound = errors.New("data file is not found")
	ErrDataDirectoryCorrupted = errors.New("the database directory maybe corrupted")
	ErrExceedMaxBatchNum = errors.New("exceed the max batch num")
	ErrMergeIsInProgress = errors.New("merge is in progress, try again later")
	ErrDatabaseIsInUse = errors.New("the database directory is in use")
	ErrMergeRatioUnreached = errors.New("the merge ratio does not reach the threshold")
	ErrNoEnoughSpaceForMerge = errors.New("no enough disk space for merge")
)