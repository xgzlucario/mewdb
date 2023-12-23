package mewdb

import (
	"encoding/binary"
	"unsafe"

	"github.com/rosedblabs/wal"
)

const (
	maxVarLen32 = binary.MaxVarintLen32
)

var (
	chunkPositionSize = int(unsafe.Sizeof(wal.ChunkPosition{}))
)

// LogRecord is the data format in disk.
type LogRecord struct {
	Timestamp uint32
	KeySize   uint32
	Key       []byte
	Value     []byte
}

// encode
func (r *LogRecord) encode() []byte {
	buf := make([]byte, 0, maxVarLen32+maxVarLen32+len(r.Key)+len(r.Value))

	buf = binary.AppendUvarint(buf, uint64(r.Timestamp))
	buf = binary.AppendUvarint(buf, uint64(r.KeySize))
	buf = append(buf, r.Key...)
	buf = append(buf, r.Value...)

	return buf
}

// decode
func (r *LogRecord) decode(buf []byte) {
	var index int
	// timestamp
	timestamp, n := binary.Uvarint(buf[index:])
	r.Timestamp = uint32(timestamp)
	index += n
	// keySize
	keySize, n := binary.Uvarint(buf[index:])
	r.KeySize = uint32(keySize)
	index += n
	// key
	r.Key = buf[index : index+int(keySize)]
	index += int(keySize)
	// value
	r.Value = buf[index:]
}

// TTL
func (r *LogRecord) TTL() int64 {
	return int64(r.Timestamp) * timeCarry
}

// IndexRecord is the index data format in disk.
type IndexRecord struct {
	KeySize  uint32
	Key      []byte
	Position *wal.ChunkPosition
}

// encode
func (r *IndexRecord) encode() []byte {
	buf := make([]byte, 0, maxVarLen32+len(r.Key)+chunkPositionSize)

	buf = binary.AppendUvarint(buf, uint64(r.KeySize))
	buf = append(buf, r.Key...)
	buf = append(buf, r.Position.Encode()...)

	return buf
}

// decode
func (r *IndexRecord) decode(buf []byte) {
	var index int
	// keySize
	keySize, n := binary.Uvarint(buf[index:])
	r.KeySize = uint32(keySize)
	index += n
	// key
	r.Key = buf[index : index+int(keySize)]
	index += int(keySize)
	// position
	r.Position = wal.DecodeChunkPosition(buf[index:])
}
