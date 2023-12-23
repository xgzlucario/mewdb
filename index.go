package mewdb

import (
	"fmt"
	"unsafe"

	"github.com/rosedblabs/wal"
	cache "github.com/xgzlucario/GigaCache"
)

var (
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
		m: cache.New(cache.DefaultOption),
	}
}

// Get
func (i *Index) Get(key []byte) (keydir Keydir, ok bool) {
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

// Set
func (i *Index) Set(key []byte, keydir Keydir) {
	i.m.Set(key, keydir.Encode())
}

// SetTx
func (i *Index) SetTx(key []byte, keydir Keydir, ttl int64) {
	i.m.SetTx(key, keydir.Encode(), ttl)
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

// Len
func (i *Index) Len() int {
	return int(i.m.Stat().Len)
}
