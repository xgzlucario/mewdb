package mewdb

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/rosedblabs/wal"
)

// Wal is write ahead log for mewdb.
type Wal struct {
	dirPath    string
	segmentExt string
	log        *wal.WAL
}

// openWal create WAL files by dirPath.
func openWal(dirPath string) (*Wal, error) {
	options := wal.DefaultOptions
	options.DirPath = dirPath
	options.SegmentFileExt = ".SEG"
	// open from wal.
	log, err := wal.Open(options)
	if err != nil {
		return nil, err
	}
	return &Wal{
		segmentExt: options.SegmentFileExt,
		dirPath:    dirPath,
		log:        log,
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

// Iter iterate all data in wal.
func (l *Wal) Iter(f func(Keydir, []byte)) error {
	return l.IterWithMax(l.log.ActiveSegmentID(), f)
}

// Iter iterate all data in wal with max segment id.
func (l *Wal) IterWithMax(segId uint32, f func(Keydir, []byte)) error {
	reader := l.log.NewReaderWithMax(segId)
	for {
		val, position, err := reader.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		f(position, val)
	}
	return nil
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

// RemoveOldSegments remove all segments which is less than maxSegmentID.
func (l *Wal) RemoveOldSegments(maxSegmentID uint32) error {
	maxSegmentName := fmt.Sprintf("%09d%s", maxSegmentID, l.segmentExt)

	filepath.WalkDir(l.dirPath, func(path string, file os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if file.Name() <= maxSegmentName {
			os.Remove(path)
		}
		return nil
	})
	return nil
}
