package mewdb

import (
	"fmt"
	"io"
	"log/slog"
	"path"
	"sync"

	"github.com/rosedblabs/wal"
)

const (
	noTTL     = 0
	timeCarry = 1e9
)

// DB
type DB struct {
	sync.RWMutex

	dataFiles *wal.WAL
	hintFiles *wal.WAL

	index *Index
	opt   *Option

	log *slog.Logger

	mergeC chan struct{}
}

// Open
func Open(opt *Option) (db *DB, err error) {
	db = &DB{
		opt: opt,
		log: slog.Default(),
	}

	// open data files.
	walOptions := wal.DefaultOptions
	walOptions.DirPath = opt.DirPath

	db.dataFiles, err = wal.Open(walOptions)
	if err != nil {
		return nil, err
	}

	// open hint files.
	walOptions = wal.DefaultOptions
	walOptions.DirPath = path.Join(opt.DirPath, "hint")

	db.hintFiles, err = wal.Open(walOptions)
	if err != nil {
		return nil, err
	}

	// load index from WAL.
	db.index = NewIndex()
	if err := db.loadIndexFromWAL(); err != nil {
		return nil, err
	}

	db.mergeC = make(chan struct{}, 1)

	db.log.Info("mewdb is ready to go.")

	return db, nil
}

// Put
func (db *DB) Put(key, value []byte) error {
	return db.PutWithTTL(key, value, noTTL)
}

// PutWithTTL
func (db *DB) PutWithTTL(key, value []byte, nanosec int64) error {
	// encode log record.
	logRecord := LogRecord{
		Timestamp: uint32(nanosec / timeCarry),
		KeySize:   uint32(len(key)),
		Key:       key,
		Value:     value,
	}
	value = logRecord.encode()

	// write WAL.
	position, err := db.dataFiles.Write(value)
	if err != nil {
		return err
	}

	// update index.
	db.index.Set(key, Keydir{position})

	return nil
}

// Get
func (db *DB) Get(key []byte) ([]byte, error) {
	keydir, ok := db.index.Get(key)
	if !ok {
		return nil, ErrKeyNotFound
	}

	// read data from disk.
	data, err := db.dataFiles.Read(keydir.ChunkPosition)
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

	if err := db.dataFiles.Close(); err != nil {
		return err
	}

	if err := db.hintFiles.Close(); err != nil {
		return err
	}

	close(db.mergeC)

	return nil
}

// loadIndexFromWAL
func (db *DB) loadIndexFromWAL() error {
	var keydir Keydir
	var logRecord = new(LogRecord)

	for reader := db.dataFiles.NewReader(); ; reader.Next() {
		position := reader.CurrentChunkPosition()
		keydir = Keydir{position}

		// read data from disk.
		data, err := db.dataFiles.Read(position)
		if err == io.EOF {
			break

		} else if err != nil {
			panic(err)
		}
		logRecord.decode(data)

		db.index.SetTx(logRecord.Key, keydir, logRecord.TTL())
	}

	db.log.Info(fmt.Sprintf("load index from WAL: %d", db.index.Len()))

	return nil
}

// loadIndexFromHint
func (db *DB) loadIndexFromHint() error {
	return nil
}
