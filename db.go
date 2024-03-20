package mewdb

import (
	"log/slog"
	"path"
	"sync"
	"sync/atomic"

	"github.com/gofrs/flock"
	"github.com/robfig/cron/v3"
)

const (
	noTTL     = 0
	timeCarry = 1e9

	fileLockName = "FLOCK"
	hintFileExt  = ".HINT"
	dataFileExt  = ".SEG"
)

// DB represents a MEWDB database instance built on BITCASK model.
// See https://en.wikipedia.org/wiki/Bitcask for more details.
type DB struct {
	mu        sync.Mutex
	flock     *flock.Flock
	dataFiles *Wal // data files save key-value by log-structured storage.
	// hintFiles *Wal // hint files store the key and keydir for fast startup.
	index     *Index
	options   *Options
	closed    atomic.Bool
	mergeLock sync.Mutex
	cron      *cron.Cron // cron scheduler for auto merge task.
	log       *slog.Logger
}

// Open a database with the specified options.
// If the database directory does not exist, it will be created automatically.
func Open(options Options) (db *DB, err error) {
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	options.Logger.Info("mewdb is starting")

	// open data files.
	dataFiles, err := openWal(options.DirPath, dataFileExt)
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
		log:       options.Logger,
	}

	// load index from WAL.
	if err := db.loadIndexFromWAL(); err != nil {
		return nil, err
	}

	// start backend cron job.
	db.cron = cron.New(cron.WithSeconds())
	if len(options.MergeCronExpr) > 0 {
		if _, err = db.cron.AddFunc(options.MergeCronExpr, func() {
			db.Merge()
		}); err != nil {
			return nil, err
		}
	}
	db.cron.Start()

	db.log.Info("mewdb is ready to go")

	return db, nil
}

// Put
func (db *DB) Put(key, val []byte) error {
	return db.PutWithTTL(key, val, noTTL)
}

// PutWithTTL
func (db *DB) PutWithTTL(key, val []byte, nanosec int64) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// write WAL first.
	record := &LogRecord{
		Timestamp: uint32(nanosec / timeCarry),
		Key:       key,
		Value:     val,
	}
	keydir, err := db.dataFiles.Write(record.encode())
	if err != nil {
		return err
	}
	// update index.
	db.index.Set(key, keydir, nanosec)

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

// Delete
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// write WAL first.
	record := &LogRecord{
		Key:   key,
		Value: []byte{},
	}
	_, err := db.dataFiles.Write(record.encode())
	if err != nil {
		return err
	}

	// update index.
	db.index.Delete(key)

	return nil
}

// Close
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed.Load() {
		return ErrDatabaseIsClosed
	}

	// close data files.
	if err := db.dataFiles.Close(); err != nil {
		return err
	}

	// close hint files.
	// if err := db.hintFiles.Close(); err != nil {
	// 	return err
	// }

	// release file lock.
	if err := db.flock.Close(); err != nil {
		return err
	}

	db.cron.Stop()

	db.closed.Store(true)

	return nil
}

// loadIndexFromWAL
func (db *DB) loadIndexFromWAL() error {
	record := new(LogRecord)

	return db.dataFiles.Iter(func(keydir Keydir, data []byte) {
		record.decode(data)
		// fmt.Println(string(record.Key), keydir, len(record.Value))
		db.index.Set(record.Key, keydir, record.TTL())
	})
}

// loadIndexFromHint
// func (db *DB) loadIndexFromHint() error {
// 	record := new(HintRecord)

// 	return db.hintFiles.Iter(func(_ Keydir, data []byte) {
// 		record.decode(data)
// 		db.index.Set(record.Key, record.Keydir)
// 	})
// }

// Merge
func (db *DB) Merge() error {
	if db.closed.Load() {
		return ErrDatabaseIsClosed
	}
	if db.mergeLock.TryLock() {
		return db.doMerge()
	}
	return ErrMergeIsRunning
}
