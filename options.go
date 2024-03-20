package mewdb

import (
	"errors"
	"log/slog"
)

const (
	CronExprEveryHour   = "0 0 0/1 * * ?"
	CronExprEveryMinute = "0 0/1 * * * ?"
)

var DefaultOptions = Options{
	DirPath:       "mewdb",
	MergeCronExpr: CronExprEveryHour,
	Logger:        slog.Default(),
}

// Options represents the configuration for mewdb.
type Options struct {
	// DirPath is the database storage path.
	DirPath string

	// MergeCronExpr
	MergeCronExpr string

	// Logger
	Logger *slog.Logger
}

// checkOptions checks the validity of the options.
func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("invalid dir path")
	}
	if options.Logger == nil {
		return errors.New("invalid logger")
	}
	return nil
}
