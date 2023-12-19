package mewdb

import (
	"errors"
	"hash/crc32"
)

// Record is the data format store in disk.
type Record struct {
	CRC         uint32
	Timestamp   uint32
	KeySize     uint32
	ValueSize   uint32
	InternalKey uint64
	Key         []byte
	Value       []byte
}

func (r *Record) encode() []byte {
	buf := make([]byte, 16+8+len(r.Key)+len(r.Value))

	// encode record.
	order.PutUint32(buf[4:8], r.Timestamp)
	order.PutUint32(buf[8:12], r.KeySize)
	order.PutUint32(buf[12:16], r.ValueSize)
	order.PutUint64(buf[16:24], r.InternalKey)
	copy(buf[24:24+len(r.Key)], r.Key)
	copy(buf[24+len(r.Key):], r.Value)

	// calculate crc.
	r.CRC = crc32.Checksum(buf[4:], crctable)
	order.PutUint32(buf, r.CRC)

	return buf
}

func (r *Record) decode(buf []byte) error {
	r.CRC = order.Uint32(buf)
	// check crc.
	if crc32.Checksum(buf[4:], crctable) != r.CRC {
		return errors.New("crc check failed")
	}

	r.Timestamp = order.Uint32(buf[4:8])
	r.KeySize = order.Uint32(buf[8:12])
	r.ValueSize = order.Uint32(buf[12:16])
	r.InternalKey = order.Uint64(buf[16:24])
	r.Key = buf[24 : 24+r.KeySize]
	r.Value = buf[24+r.KeySize:]

	return nil
}
