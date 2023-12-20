package mewdb

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/rosedblabs/wal"
	cache "github.com/xgzlucario/GigaCache"
)

var (
	order = binary.LittleEndian

	keydirSize = int(unsafe.Sizeof(Keydir{}))
)

// Index
type Index struct {
	m *cache.GigaCache
}

// Keydir
type Keydir struct {
	*wal.ChunkPosition
}

// NewIndex
func NewIndex() *Index {
	return &Index{
		m: cache.New(),
	}
}

// Get
func (i *Index) Get(key string) (keydir Keydir, ok bool) {
	value, _, ok := i.m.Get(key)
	if !ok {
		return
	}
	if len(value) != keydirSize {
		panic(fmt.Errorf("bug: invalid value length: %d", len(value)))
	}

	keydir = Keydir{
		wal.DecodeChunkPosition(value),
	}

	return keydir, true
}

// Put
func (i *Index) Put(key string, keydir Keydir) {
	i.m.Set(key, keydir.Encode())
}

// Scan
func (i *Index) Scan(f func(key []byte, keydir Keydir) bool) {
	var keydir Keydir

	i.m.Scan(func(key, value []byte, ts int64) bool {
		if len(value) != keydirSize {
			panic(fmt.Errorf("bug: invalid value length: %d", len(value)))
		}
		keydir = Keydir{
			wal.DecodeChunkPosition(value),
		}

		return f(key, keydir)
	})
}
