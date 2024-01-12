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
	// Dir path if the db storage path.
	DirPath string

	// MErgeInterval
	MergeInterval time.Duration
}

// checkOptions checks the validity of the options.
func checkOptions(option Options) error {
	if option.DirPath == "" {
		return errors.New("invalid dir path")
	}
	return nil
}
