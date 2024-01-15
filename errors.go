package mewdb

import "errors"

var (
	ErrKeyNotFound = errors.New("mewdb: key not found")

	ErrKeyIsEmpty = errors.New("mewdb: key is empty")

	ErrDatabaseIsUsing = errors.New("mewdb: create flock file error, database is using")

	ErrDatabaseIsClosed = errors.New("mewdb: database is closed")

	ErrMergeIsRunning = errors.New("mewdb: merge is running")
)
