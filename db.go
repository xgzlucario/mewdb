package mewdb

import (
	"path"
	"sync"

	"github.com/gofrs/flock"
)

const (
	noTTL     = 0
	timeCarry = 1e9

	fileLockName = "flock"
)

// DB
type DB struct {
	sync.RWMutex
	flock *flock.Flock

	dataFiles *Wal
	hintFiles *Wal

	index   *Index
	options *Options

	mergeC chan struct{}
}

// Open
func Open(options Options) (db *DB, err error) {
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// create file lock, prevent multiple processes from using the same db directory.
	fileLock := flock.New(fileLockName)
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	// init db instance.
	db = &DB{
		index:   NewIndex(),
		flock:   fileLock,
		options: &options,
	}

	// open data files.
	db.dataFiles, err = openWal(options.DirPath)
	if err != nil {
		return nil, err
	}

	// open hint files.
	db.hintFiles, err = openWal(path.Join(options.DirPath, "hint"))
	if err != nil {
		return nil, err
	}

	// load index from WAL.
	if err := db.loadIndexFromWAL(); err != nil {
		return nil, err
	}

	db.mergeC = make(chan struct{}, 1)

	return db, nil
}

// Put
func (db *DB) Put(key, value []byte) error {
	return db.PutWithTTL(key, value, noTTL)
}

// PutWithTTL
func (db *DB) PutWithTTL(key, value []byte, nanosec int64) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// write WAL first.
	keydir, err := db.dataFiles.Write(&LogRecord{
		Timestamp: uint32(nanosec / timeCarry),
		KeySize:   uint32(len(key)),
		Key:       key,
		Value:     value,
	})
	if err != nil {
		return err
	}
	// update index.
	db.index.Set(key, keydir)

	return nil
}

// Get
func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	keydir, ok := db.index.Get(key)
	if !ok {
		return nil, ErrKeyNotFound
	}

	// read data from disk.
	data, err := db.dataFiles.Read(keydir)
	if err != nil {
		return nil, err
	}

	// decode record.
	logRecord := new(LogRecord)
	logRecord.decode(data)

	return logRecord.Value, nil
}

// Close
func (db *DB) Close() error {
	db.Lock()
	defer db.Unlock()

	// close data files.
	if err := db.dataFiles.Close(); err != nil {
		return err
	}
	if err := db.hintFiles.Close(); err != nil {
		return err
	}
	// close file lock.
	if err := db.flock.Close(); err != nil {
		return err
	}
	close(db.mergeC)

	return nil
}

// loadIndexFromWAL
func (db *DB) loadIndexFromWAL() error {
	var record = new(LogRecord)

	return db.dataFiles.Iter(func(keydir Keydir, val []byte) {
		record.decode(val)
		db.index.SetTx(record.Key, keydir, record.TTL())
	})
}
