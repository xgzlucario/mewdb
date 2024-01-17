package mewdb

import (
	"errors"
	"time"
)

var DefaultOptions = Options{
	DirPath:       "data",
	MergeInterval: 30 * time.Minute,
}

// Options represents the configuration for mewdb.
type Options struct {
	// DirPath is the database storage path.
	DirPath string

	// MergeInterval
	MergeInterval time.Duration
}

// checkOptions checks the validity of the options.
func checkOptions(option Options) error {
	if option.DirPath == "" {
		return errors.New("invalid dir path")
	}
	if option.MergeInterval <= 0 {
		return errors.New("invalid merge interval")
	}
	return nil
}
