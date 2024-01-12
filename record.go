package mewdb

import (
	"encoding/binary"
)

var (
	order = binary.LittleEndian
)

// LogRecord is the db record format on disk.
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
	buf = append(buf, r.Value...)

	return buf
}

// decode
func (r *LogRecord) decode(buf []byte) {
	var index int
	// timestamp
	r.Timestamp = order.Uint32(buf)
	index += 4
	// key size
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
