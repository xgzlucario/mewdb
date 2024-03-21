package mewdb

import (
	"errors"
	"log/slog"
)

const (
	KB = 1024
	MB = 1024 * KB
	GB = 1024 * MB

	CronExprEveryHour   = "0 0 0/1 * * ?"
	CronExprEveryMinute = "0 0/1 * * * ?"
)

var DefaultOptions = Options{
	DirPath:        "mewdb",
	MergeCronExpr:  CronExprEveryHour,
	Logger:         slog.Default(),
	SegmentSize:    GB,
	SegmentFileExt: ".SEG",
}

type Options struct {
	// DirPath is the database storage path.
	DirPath string

	MergeCronExpr string

	Logger *slog.Logger

	SegmentSize    int64
	SegmentFileExt string
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("invalid dir path")
	}
	if options.Logger == nil {
		return errors.New("invalid logger")
	}
	return nil
}
