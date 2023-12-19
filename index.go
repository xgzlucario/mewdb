package mewdb

import (
	"encoding/binary"
	"fmt"
	"unsafe"

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
	FileId uint32
	Offset uint32
	Size   uint32
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
		FileId: order.Uint32(value[0:4]),
		Offset: order.Uint32(value[4:8]),
		Size:   order.Uint32(value[8:12]),
	}

	return keydir, true
}

// Put
func (i *Index) Put(key string, keydir Keydir) {
	value := make([]byte, keydirSize)
	order.PutUint32(value[0:4], keydir.FileId)
	order.PutUint32(value[4:8], keydir.Offset)
	order.PutUint32(value[8:12], keydir.Size)

	i.m.Set(key, value)
}

// Scan
func (i *Index) Scan(f func(key []byte, keydir Keydir) bool) {
	var keydir Keydir

	i.m.Scan(func(key, value []byte, ts int64) bool {
		if len(value) != keydirSize {
			panic(fmt.Errorf("bug: invalid value length: %d", len(value)))
		}
		keydir.FileId = order.Uint32(value[0:4])
		keydir.Offset = order.Uint32(value[4:8])
		keydir.Size = order.Uint32(value[8:12])

		return f(key, keydir)
	})
}
