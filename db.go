package mewdb

import (
	"hash/crc32"
	"sync"

	"github.com/rosedblabs/wal"
	cache "github.com/xgzlucario/GigaCache"
)

var (
	crctable = crc32.MakeTable(crc32.Castagnoli)
)

// DB
type DB struct {
	sync.RWMutex

	dataFiles *wal.WAL
	hintFiles *wal.WAL

	index *Index
	opt   *Option

	mergeC chan struct{}
}

// Open
func Open(opt *Option) (db *DB, err error) {
	db = &DB{
		opt: opt,
	}

	walOptions := wal.DefaultOptions
	walOptions.DirPath = opt.DirPath

	db.dataFiles, err = wal.Open(walOptions)
	if err != nil {
		return nil, err
	}

	db.hintFiles, err = wal.Open(walOptions)
	if err != nil {
		return nil, err
	}

	db.index = NewIndex()
	db.mergeC = make(chan struct{}, 1)

	return db, nil
}

// Put
func (db *DB) Put(key string, value []byte) error {
	// encode record.
	record := Record{
		Timestamp:   cache.GetSec(),
		KeySize:     uint32(len(key)),
		ValueSize:   uint32(len(value)),
		InternalKey: 0,
		Key:         []byte(key),
		Value:       value,
	}
	value = record.encode()

	// write record to WAL and get position.
	position, err := db.dataFiles.Write(value)
	if err != nil {
		return err
	}

	// update keydir to index.
	keydir := Keydir{
		FileId: 0,
		Offset: uint32(position.ChunkOffset),
		Size:   uint32(position.ChunkSize),
	}
	db.index.Put(key, keydir)

	return nil
}
