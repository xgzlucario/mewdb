package mewdb

import "errors"

var (
	ErrKeyNotFound = errors.New("mewdb: key not found")

	ErrKeyIsEmpty = errors.New("mewdb: key is empty")

	ErrDatabaseIsUsing = errors.New("mewdb: database is using")
)
