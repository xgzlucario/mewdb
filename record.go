package mewdb

import (
	"encoding/binary"

	"github.com/klauspost/compress/zstd"
	cache "github.com/xgzlucario/GigaCache"
)

var (
	order = binary.LittleEndian

	encoder, _    = zstd.NewWriter(nil)
	decoder, _    = zstd.NewReader(nil)
	compressRatio = 4

	bpool = cache.NewBufferPool()
)

type Type byte

const (
	typeVal Type = iota
	typeDel
	typeZstdCompress
)

func toType(vlen int) Type {
	if vlen >= 1024 {
		return typeZstdCompress
	}
	return typeVal
}

// LogRecord is the mewdb data record format on disk.
type LogRecord struct {
	Type
	Timestamp uint32
	Key       []byte
	Value     []byte
}

func varintSize[T uint64 | int](v T) (n int) {
	for ; v > 0; n++ {
		v >>= 7
	}
	return
}

// encode
func (r *LogRecord) encode() []byte {
	buf := bpool.Get(1 + 4 + varintSize(len(r.Key)) + len(r.Key) + len(r.Value))[:0]

	// type
	buf = append(buf, byte(r.Type))

	// timestamp
	buf = order.AppendUint32(buf, r.Timestamp)

	// key
	buf = binary.AppendUvarint(buf, uint64(len(r.Key)))
	buf = append(buf, r.Key...)

	// value
	switch r.Type {
	case typeZstdCompress:
		preAlloc := bpool.Get(len(r.Value) / compressRatio)
		return append(buf, encoder.EncodeAll(r.Value, preAlloc)...)

	case typeVal:
		return append(buf, r.Value...)
	}
	return buf
}

// decodeKey
func (r *LogRecord) decodeKey(buf []byte) (index int) {
	// type
	r.Type = Type(buf[index])
	index++

	// timestamp
	r.Timestamp = order.Uint32(buf[index:])
	index += 4

	// keySize
	keySize, n := binary.Uvarint(buf[index:])
	index += n

	// key
	r.Key = buf[index : index+int(keySize)]
	index += int(keySize)

	return
}

// decodeAll
func (r *LogRecord) decodeAll(buf []byte) {
	index := r.decodeKey(buf)

	switch r.Type {
	case typeZstdCompress:
		r.Value, _ = decoder.DecodeAll(buf[index:], bpool.Get(len(buf) * compressRatio)[:0])

	case typeVal:
		r.Value = buf[index:]
	}
}

// TTL
func (r *LogRecord) TTL() int64 {
	return int64(r.Timestamp) * timeCarry
}
