package mewdb

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/rosedblabs/wal"
)

// Wal is write ahead log for mewdb.
type Wal struct {
	options *Options
	log     *wal.WAL
}

// openWal create WAL files by dirPath.
func openWal(options Options) (*Wal, error) {
	walopt := wal.DefaultOptions
	walopt.DirPath = options.DirPath
	walopt.SegmentFileExt = options.SegmentFileExt
	walopt.SegmentSize = options.SegmentSize

	// open from wal.
	log, err := wal.Open(walopt)
	if err != nil {
		return nil, err
	}

	return &Wal{&options, log}, nil
}

func (l *Wal) Write(data []byte) (Keydir, error) {
	return l.log.Write(data)
}

func (l *Wal) Read(keydir Keydir) ([]byte, error) {
	return l.log.Read(keydir)
}

// walker is the callback function for Iter.
type walker func(keydir Keydir, data []byte)

// Iter iterate datas between segmentStart and segmentEnd, this is a internal function.
func (l *Wal) Iter(segmentStart, segmentEnd uint32, f walker) error {
	if segmentEnd < segmentStart {
		panic("bug: segmentEnd is less than segmentStart")
	}
	reader := l.log.NewReaderWithMax(segmentEnd)
	for reader.CurrentSegmentId() < segmentStart {
		reader.SkipCurrentSegment()
	}
	for {
		data, keydir, err := reader.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		f(keydir, data)
	}
	return nil
}

func (l *Wal) Sync() error {
	return l.log.Sync()
}

func (l *Wal) Close() error {
	return l.log.Close()
}

func (l *Wal) ActiveSegmentID() uint32 {
	return l.log.ActiveSegmentID()
}

func (l *Wal) OpenNewActiveSegment() error {
	return l.log.OpenNewActiveSegment()
}

func (l *Wal) SegmentFileName(segId uint32) string {
	return fmt.Sprintf("%09d%s", segId, l.options.SegmentFileExt)
}

// RemoveOldSegments remove all segments which is less than maxSegmentID.
func (l *Wal) RemoveOldSegments(maxSegmentID uint32) error {
	maxSegmentName := l.SegmentFileName(maxSegmentID)

	return filepath.WalkDir(l.options.DirPath, func(path string, file os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := file.Name()

		if strings.HasSuffix(name, l.options.SegmentFileExt) && name <= maxSegmentName {
			os.Remove(path)
		}
		return nil
	})
}
