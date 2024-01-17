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
	dirPath        string
	segmentFileExt string
	log            *wal.WAL
}

// openWal create WAL files by dirPath.
func openWal(dirPath, segmentFileExt string) (*Wal, error) {
	options := wal.DefaultOptions
	options.DirPath = dirPath
	options.SegmentFileExt = segmentFileExt

	// open from wal.
	log, err := wal.Open(options)
	if err != nil {
		return nil, err
	}
	return &Wal{
		segmentFileExt: options.SegmentFileExt,
		dirPath:        dirPath,
		log:            log,
	}, err
}

// Write
func (l *Wal) Write(data []byte) (Keydir, error) {
	return l.log.Write(data)
}

// Read
func (l *Wal) Read(keydir Keydir) ([]byte, error) {
	return l.log.Read(keydir)
}

// walker is the callback function for Iter.
type walker func(keydir Keydir, data []byte)

// iterInternal iterate datas between segmentStart and segmentEnd, this is a internal function.
func (l *Wal) iterInternal(segmentStart, segmentEnd uint32, f walker) error {
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

// Iter iterate all data.
func (l *Wal) Iter(f walker) error {
	return l.iterInternal(0, l.log.ActiveSegmentID(), f)
}

// IterWithMax iterate all data with max segment id.
func (l *Wal) IterWithMax(segId uint32, f walker) error {
	return l.iterInternal(0, segId, f)
}

// IterWithSegment iterate all data with segment id.
func (l *Wal) IterWithSegment(segId uint32, f walker) error {
	return l.iterInternal(segId, segId, f)
}

// Sync
func (l *Wal) Sync() error {
	return l.log.Sync()
}

// Close
func (l *Wal) Close() error {
	return l.log.Close()
}

// ActiveSegmentID
func (l *Wal) ActiveSegmentID() uint32 {
	return l.log.ActiveSegmentID()
}

// OpenNewActiveSegment
func (l *Wal) OpenNewActiveSegment() error {
	return l.log.OpenNewActiveSegment()
}

// SegmentFileName
func (l *Wal) SegmentFileName(segId uint32) string {
	return fmt.Sprintf("%09d%s", segId, l.segmentFileExt)
}

// RemoveOldSegments remove all segments which is less than maxSegmentID.
func (l *Wal) RemoveOldSegments(maxSegmentID uint32) error {
	maxSegmentName := l.SegmentFileName(maxSegmentID)

	return filepath.WalkDir(l.dirPath, func(path string, file os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := file.Name()

		if strings.HasSuffix(name, l.segmentFileExt) && name <= maxSegmentName {
			os.Remove(path)
		}
		return nil
	})
}
