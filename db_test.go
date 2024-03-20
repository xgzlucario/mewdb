package mewdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/rosedblabs/wal"
	"github.com/stretchr/testify/assert"
	cache "github.com/xgzlucario/GigaCache"
)

func randKey() []byte {
	return []byte(fmt.Sprintf("%08x", cache.FastRand64()))
}

func randKeydir() Keydir {
	return &wal.ChunkPosition{
		SegmentId:   cache.FastRand(),
		BlockNumber: cache.FastRand(),
		ChunkOffset: int64(cache.FastRand64()),
		ChunkSize:   cache.FastRand(),
	}
}

func TestIndex(t *testing.T) {
	assert := assert.New(t)
	const N = 10000

	t.Run("index-set", func(t *testing.T) {
		index := NewIndex()
		validMap := map[string]Keydir{}

		// set
		for i := 0; i < N; i++ {
			key, keydir := randKey(), randKeydir()
			index.Set(key, keydir, 0)
			validMap[string(key)] = keydir

			// check length.
			assert.Equal(index.Len(), len(validMap))
		}

		// get
		for k, v := range validMap {
			keydir, ok := index.Get([]byte(k))
			assert.True(ok)
			assert.Equal(keydir, v)
		}

		// scan
		count := 0
		index.Scan(func(key []byte, _ int64, keydir Keydir) bool {
			count++
			v, ok := validMap[string(key)]
			assert.True(ok)
			assert.Equal(keydir, v)
			return true
		})
		assert.Equal(count, N)
	})
}

func TestRecord(t *testing.T) {
	assert := assert.New(t)

	t.Run("logRecord", func(t *testing.T) {
		var record *LogRecord

		for i := 0; i < 10000; i++ {
			now := uint32(time.Now().Unix())
			key, val := randKey(), randKey()

			record = &LogRecord{
				Timestamp: now,
				Key:       key,
				Value:     val,
			}
			// encode
			buf := record.encode()

			// decode
			record.decodeAll(buf)

			assert.Equal(int64(now)*timeCarry, record.TTL())
			assert.Equal(key, record.Key)
			assert.Equal(val, record.Value)
		}
	})
}
