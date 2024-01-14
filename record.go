package mewdb

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/rosedblabs/wal"
)

var (
	order = binary.LittleEndian

	keydirSize = int(unsafe.Sizeof(&wal.ChunkPosition{}))
)

// LogRecord is the mewdb data record format on disk.
type LogRecord struct {
	Timestamp uint32
	Key       []byte
	Value     []byte
}

// encode
func (r *LogRecord) encode() []byte {
	buf := make([]byte, 0, 4+binary.MaxVarintLen32+len(r.Key)+len(r.Value))
	// timestamp
	buf = order.AppendUint32(buf, r.Timestamp)
	// key
	buf = binary.AppendUvarint(buf, uint64(len(r.Key)))
	buf = append(buf, r.Key...)
	// value
	return append(buf, r.Value...)
}

// decode
func (r *LogRecord) decode(buf []byte) {
	var index int
	// timestamp
	r.Timestamp = order.Uint32(buf)
	index += 4
	// keySize
	keySize, n := binary.Uvarint(buf[index:])
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

// String
func (r *LogRecord) String() string {
	return fmt.Sprintf("LogRecord{Timestamp: %d, key: %s, value: %s}", r.Timestamp, r.Key, r.Value)
}

// HintRecord is the mewdb index record format on disk.
type HintRecord struct {
	Timestamp uint32
	Key       []byte
	Keydir    Keydir
}

// encode
func (r *HintRecord) encode() []byte {
	buf := make([]byte, 0, 4+5+len(r.Key)+keydirSize)
	// timestamp
	buf = order.AppendUint32(buf, r.Timestamp)
	// key
	buf = binary.AppendUvarint(buf, uint64(len(r.Key)))
	buf = append(buf, r.Key...)
	// keydir
	return append(buf, r.Keydir.Encode()...)
}

// decode
func (r *HintRecord) decode(buf []byte) {
	var index int
	// timestamp
	r.Timestamp = order.Uint32(buf)
	index += 4
	// keySize
	keySize, n := binary.Uvarint(buf[index:])
	index += n
	// key
	r.Key = buf[index : index+int(keySize)]
	index += int(keySize)
	// keydir
	r.Keydir = wal.DecodeChunkPosition(buf[index:])
}

// TTL
func (r *HintRecord) TTL() int64 {
	return int64(r.Timestamp) * timeCarry
}
