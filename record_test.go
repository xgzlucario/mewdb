package mewdb

import (
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/rosedblabs/wal"
	"github.com/stretchr/testify/assert"
)

func TestLogRecord(t *testing.T) {
	assert := assert.New(t)
	var record *LogRecord

	for i := 0; i < 10000; i++ {
		now := uint32(time.Now().Unix())
		key := gofakeit.UUID()
		value := gofakeit.URL()

		record = &LogRecord{
			Timestamp: now,
			KeySize:   uint32(len(key)),
			Key:       []byte(key),
			Value:     []byte(value),
		}

		// encode
		buf := record.encode()

		// decode
		record.decode(buf)

		assert.Equal(now, record.Timestamp)
		assert.Equal(uint32(len(key)), record.KeySize)
		assert.Equal(key, string(record.Key))
		assert.Equal(value, string(record.Value))
	}
}

func TestIndexRecord(t *testing.T) {
	assert := assert.New(t)
	var record *IndexRecord

	for i := 0; i < 10000; i++ {
		key := gofakeit.UUID()
		chunkPosition := &wal.ChunkPosition{
			SegmentId:   uint32(gofakeit.Number(0, 100)),
			BlockNumber: uint32(gofakeit.Number(0, 100)),
			ChunkOffset: int64(gofakeit.Number(0, 1000)),
			ChunkSize:   uint32(gofakeit.Number(0, 1000)),
		}

		record = &IndexRecord{
			KeySize:  uint32(len(key)),
			Key:      []byte(key),
			Position: chunkPosition,
		}

		// encode
		buf := record.encode()

		// decode
		record.decode(buf)

		assert.Equal(uint32(len(key)), record.KeySize)
		assert.Equal(key, string(record.Key))
		assert.Equal(chunkPosition, record.Position)
	}
}
