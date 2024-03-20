package mewdb

import (
	"encoding/binary"

	"github.com/klauspost/compress/zstd"
)

var (
	order = binary.LittleEndian

	encoder, _ = zstd.NewWriter(nil)
	decoder, _ = zstd.NewReader(nil)
)

type Type byte

const (
	TypeVal Type = iota
	TypeDel
	TypeZstdCompress
)

func toType(vlen int) Type {
	if vlen >= 1024 {
		return TypeZstdCompress
	}
	return TypeVal
}

// LogRecord is the mewdb data record format on disk.
type LogRecord struct {
	Type
	Timestamp uint32
	Key       []byte
	Value     []byte
}

// encode
func (r *LogRecord) encode() []byte {
	buf := make([]byte, 0, 1+4+binary.MaxVarintLen32+len(r.Key)+len(r.Value))

	// type
	buf = append(buf, byte(r.Type))

	// timestamp
	buf = order.AppendUint32(buf, r.Timestamp)

	// key
	buf = binary.AppendUvarint(buf, uint64(len(r.Key)))
	buf = append(buf, r.Key...)

	// value
	switch r.Type {
	case TypeZstdCompress:
		preAlloc := make([]byte, 0, len(r.Value)/4)
		return append(buf, encoder.EncodeAll(r.Value, preAlloc)...)

	case TypeVal:
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
	case TypeZstdCompress:
		r.Value, _ = decoder.DecodeAll(buf[index:], nil)

	case TypeVal:
		r.Value = buf[index:]

	default:
		return
	}
}

// TTL
func (r *LogRecord) TTL() int64 {
	return int64(r.Timestamp) * timeCarry
}
