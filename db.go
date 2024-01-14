package mewdb

import (
	"path"

	"github.com/gofrs/flock"
)

const (
	noTTL     = 0
	timeCarry = 1e9

	fileLockName = "FLOCK"
	hintFileName = "HINT"
)

// DB represents a MEWDB database instance built on BITCASK model.
// See https://en.wikipedia.org/wiki/Bitcask for more details.
type DB struct {
	flock     *flock.Flock
	dataFiles *Wal     // data files save key-value by log-structured storage.
	hintFiles *Wal     // hint files store the key and keydir for fast startup.
	index     *Index   // index keeps all the keys in memory.
	options   *Options // database options.
	mergeC    chan struct{}
}

// Open a database with the specified options.
// If the database directory does not exist, it will be created automatically.
func Open(options Options) (db *DB, err error) {
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// open data files.
	dataFiles, err := openWal(options.DirPath)
	if err != nil {
		return nil, err
	}

	// create file lock, prevent multiple processes from using the same db directory.
	fileLock := flock.New(path.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	// init db instance.
	db = &DB{
		index:     NewIndex(),
		flock:     fileLock,
		options:   &options,
		dataFiles: dataFiles,
	}

	// open hint files.
	db.hintFiles, err = openWal(path.Join(options.DirPath, "hint"))
	if err != nil {
		return nil, err
	}

	// load index from hintfiles.
	if err := db.loadIndexFromHint(); err != nil {
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
	record := &LogRecord{
		Timestamp: uint32(nanosec / timeCarry),
		Key:       key,
		Value:     value,
	}
	keydir, err := db.dataFiles.Write(record.encode())
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

	// find index on memory.
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
	record := new(LogRecord)
	record.decode(data)

	return record.Value, nil
}

// Close
func (db *DB) Close() error {
	// close data files.
	if err := db.dataFiles.Close(); err != nil {
		return err
	}

	// close hint files.
	if err := db.hintFiles.Close(); err != nil {
		return err
	}

	// release file lock.
	if err := db.flock.Close(); err != nil {
		return err
	}
	close(db.mergeC)

	return nil
}

// loadIndexFromWAL
func (db *DB) loadIndexFromWAL() error {
	record := new(LogRecord)

	return db.dataFiles.Iter(func(keydir Keydir, val []byte) {
		record.decode(val)
		db.index.SetTx(record.Key, keydir, record.TTL())
	})
}

// loadIndexFromHint
func (db *DB) loadIndexFromHint() error {
	record := new(HintRecord)

	return db.hintFiles.Iter(func(_ Keydir, bytes []byte) {
		record.decode(bytes)
		db.index.Set(record.Key, record.Keydir)
	})
}

// Merge
func (db *DB) Merge() error {
	select {
	case db.mergeC <- struct{}{}:
		return db.doMerge()
	default:
		return ErrMergeIsRunning
	}
}
