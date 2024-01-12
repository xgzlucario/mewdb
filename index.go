package mewdb

import (
	"unsafe"

	"github.com/rosedblabs/wal"
	cache "github.com/xgzlucario/GigaCache"
)

// Index
type Index struct {
	m *cache.GigaCache
}

// NewIndex
func NewIndex() *Index {
	return &Index{
		m: cache.New(cache.DefaultOption),
	}
}

type Keydir = *wal.ChunkPosition

// Get
func (i *Index) Get(key []byte) (keydir Keydir, ok bool) {
	val, _, ok := i.m.Get(b2s(key))
	if !ok {
		return
	}
	keydir = wal.DecodeChunkPosition(val)
	return keydir, true
}

// Set
func (i *Index) Set(key []byte, keydir Keydir) {
	i.m.Set(b2s(key), keydir.Encode())
}

// SetTx
func (i *Index) SetTx(key []byte, keydir Keydir, ttl int64) {
	i.m.SetTx(b2s(key), keydir.Encode(), ttl)
}

// Scan
func (i *Index) Scan(f func(key []byte, keydir Keydir) bool) {
	i.m.Scan(func(key, val []byte, ts int64) bool {
		return f(key, wal.DecodeChunkPosition(val))
	})
}

// Len
func (i *Index) Len() int {
	return int(i.m.Stat().Len)
}

// GC
func (i *Index) GC() {
	i.m.Migrate()
}

// b2s converts byte slice to string unsafe.
func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
