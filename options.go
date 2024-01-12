package mewdb

import (
	"errors"
	"time"
)

type Options struct {
	// Dir path if the db storage path.
	DirPath string

	MergeInterval time.Duration
}

// checkOptions checks the validity of the options.
func checkOptions(option Options) error {
	if option.DirPath == "" {
		return errors.New("invalid dir path")
	}
	return nil
}
