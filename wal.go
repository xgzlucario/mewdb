package mewdb

import (
	"io"

	"github.com/rosedblabs/wal"
)

// Wal is write ahead log for mewdb.
type Wal struct {
	log *wal.WAL
}

// openWal create WAL files by dirPath.
func openWal(dirPath string) (*Wal, error) {
	options := wal.DefaultOptions
	options.DirPath = dirPath
	// open from wal.
	log, err := wal.Open(options)
	if err != nil {
		return nil, err
	}
	return &Wal{log: log}, err
}

// Write
func (l *Wal) Write(record *LogRecord) (Keydir, error) {
	return l.log.Write(record.encode())
}

// Read
func (l *Wal) Read(keydir Keydir) ([]byte, error) {
	return l.log.Read(keydir)
}

// Iter iterate all data in wal.
func (l *Wal) Iter(f func(Keydir, []byte)) error {
	reader := l.log.NewReader()
	record := new(LogRecord)

	for {
		val, position, err := reader.Next()
		if err == io.EOF {
			break
		}
		record.decode(val)
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
